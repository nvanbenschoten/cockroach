// Copyright 2018 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package kv

import (
	"context"
	"sync"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
)

var parallelCommitsEnabled = settings.RegisterBoolSetting(
	"kv.transaction.parallel_commits",
	"if enabled, transactional writes are pipelined through Raft consensus",
	false,
)

// txnCommitter is a txnInterceptor that concerns itself with committing and
// rolling back transactions.
//
// TODO
//
// comment about implict vs explicit transactions.
// comment about intent resolution only being allowed for explicit transactions.
// add a TODO about that.
type txnCommitter struct {
	st      *cluster.Settings
	stopper *stop.Stopper

	wrapped lockedSender
	mu      sync.Locker
}

// SendLocked implements the lockedSender interface.
func (tc *txnCommitter) SendLocked(
	ctx context.Context, ba roachpb.BatchRequest,
) (*roachpb.BatchResponse, *roachpb.Error) {
	// If the batch does not include an EndTransaction request, pass through.
	rArgs, hasET := ba.GetArg(roachpb.EndTransaction)
	if !hasET {
		return tc.wrapped.SendLocked(ctx, ba)
	}
	et := rArgs.(*roachpb.EndTransactionRequest)

	// Determine whether we can elide the EndTransaction entirely. We can do
	// so if the transaction is read-only, which we determine based on whether
	// the EndTransaction request contains any intents.
	if len(et.WrittenIntents) == 0 {
		return tc.sendLockedWithElidedEndTransaction(ctx, ba, et)
	}

	// If the EndTransaction request is a rollback, pass it through.
	if !et.Commit {
		return tc.wrapped.SendLocked(ctx, ba)
	}

	// Determine whether the commit can be run in parallel with the rest of the
	// writes in the batch. If so, attach the writes that will be in-flight
	// concurrently with the EndTransaction request as the EndTransaction's
	// promised intents.
	if ok, promisedWrites := tc.canCommitInParallelWithWrites(ba, et); ok {
		et.PromisedIntents = promisedWrites
	}

	// Send the adjusted batch through the wrapped lockedSender. Unlocks while
	// sending then re-locks.
	br, pErr := tc.wrapped.SendLocked(ctx, ba)
	if pErr != nil {
		return nil, pErr
	}

	// Determine next steps based on the status of the transaction.
	switch br.Txn.Status {
	case roachpb.STAGING:
		// Continue with STAGING-specific validation and cleanup.
	case roachpb.COMMITTED:
		// The transaction is explicitly committed. This is possible if all
		// promised writes were sent to the same range as the EndTransaction
		// request, in a single batch. In this case, a range can determine that
		// all promised writes will succeed with the EndTransaction and can
		// decide to skip the STAGING state.
		//
		// This is also possible if we never attached any promised writes to the
		// EndTransaction request, either because canCommitInParallelWithWrites
		// returned false or because there were no unproven outstanding writes
		// (see txnPipeliner) and were no other writes in the batch request.
		return br, nil
	default:
		return nil, roachpb.NewErrorf("unexpected response status without error: %v", br.Txn)
	}

	// Determine whether the transaction needs to either retry or refresh.
	if ok, reason := tc.needTxnRetry(&ba, br); ok {
		err := roachpb.NewTransactionRetryError(reason)
		return nil, roachpb.NewErrorWithTxn(err, br.Txn)
	}

	// If the transaction doesn't need to retry then it is implicitly committed!
	// We're the only ones who know that though - other concurrent transactions
	// will need to go through the full status resolution process to make a
	// determination about the status of our STAGING transaction. To avoid this,
	// we transition to an explicitly committed transaction as soon as possible.
	// This also has the side-effect of kicking off intent resolution.
	tc.makeTxnCommitExplicitAsync(ctx, br.Txn)

	// Switch the status on the batch response's transaction to COMMITTED. No
	// interceptor above this one in the stack should ever need to deal with
	// transaction proto in the STAGING state.
	br.Txn = cloneWithStatus(br.Txn, roachpb.COMMITTED)
	return br, nil
}

// sendLockedWithElidedEndTransaction sends the provided batch without
// its EndTransaction request. It is used for read-only transactions,
// which never need to write a transaction record.
func (tc *txnCommitter) sendLockedWithElidedEndTransaction(
	ctx context.Context, ba roachpb.BatchRequest, et *roachpb.EndTransactionRequest,
) (*roachpb.BatchResponse, *roachpb.Error) {
	// Send the batch without its final request, which we know to be the
	// EndTransaction request that we're eliding.
	ba.Requests = ba.Requests[:len(ba.Requests)-1]
	br, pErr := tc.wrapped.SendLocked(ctx, ba)
	if pErr != nil {
		return nil, pErr
	}

	// Check if the (read-only) txn was pushed above its deadline.
	if et.Deadline != nil && et.Deadline.Less(br.Txn.Timestamp) {
		return nil, roachpb.NewErrorWithTxn(roachpb.NewTransactionStatusError(
			"deadline exceeded before transaction finalization"), br.Txn)
	}

	// Update the response's transaction proto. This normally happens on the
	// server and is sent back in response headers, but in this case the
	// EndTransaction request was optimized away. The caller may still inspect
	// the transaction struct, so we manually update it here to emulate a true
	// transaction.
	status := roachpb.ABORTED
	if et.Commit {
		status = roachpb.COMMITTED
	}
	br.Txn = cloneWithStatus(br.Txn, status)

	// Synthesize and append an EndTransaction response.
	resp := &roachpb.EndTransactionResponse{}
	resp.Txn = br.Txn
	br.Add(resp)
	return br, pErr
}

// canCommitInParallelWithWrites determines whether the batch can issue its
// committing EndTransaction in parallel with other writes. If so, it returns
// the set of writes that may still be in-flight when the EndTransaction is
// evaluated. These writes should be declared are requirements for the
// transaction to move from a STAGING status to a COMMITTED status.
func (tc *txnCommitter) canCommitInParallelWithWrites(
	ba roachpb.BatchRequest, et *roachpb.EndTransactionRequest,
) (bool, []roachpb.SequencedWrite) {
	if !parallelCommitsEnabled.Get(&tc.st.SV) {
		return false, nil
	}

	// If the transaction has a commit trigger, we don't allow it to commit in
	// parallel with writes. There's no fundamental reason for this restriction,
	// but for now it's not worth the complication.
	if et.InternalCommitTrigger != nil {
		return false, nil
	}

	// TODO
	var inFlightWrites []roachpb.SequencedWrite
	inFlightWriteLimit := maxTxnIntentsBytes.Get(&tc.st.SV)
	for _, ru := range ba.Requests {
		req := ru.GetInner()
		h := req.Header()
		var w roachpb.SequencedWrite
		switch {
		case roachpb.IsTransactionWrite(req):
			// Similarly to how we can't pipelining ranged writes, we also can't
			// commit in parallel with them. The reason for this is that the status
			// resolution process for STAGING transactions wouldn't know where to
			// look for the intents
			if roachpb.IsRange(req) {
				return false, nil
			}

			// ...
			w = roachpb.SequencedWrite{Key: h.Key, Sequence: h.Sequence}
		case req.Method() == roachpb.QueryIntent:
			// A QueryIntent isn't a write, but it indicates that there is an
			// in-flight write that needs to complete as part of the transaction.
			qi := req.(*roachpb.QueryIntentRequest)
			w = roachpb.SequencedWrite{Key: h.Key, Sequence: qi.Txn.Sequence}
		default:
			continue
		}

		s := int64(w.Size())
		if s > inFlightWriteLimit {
			//
			return false, nil
		}
		inFlightWriteLimit -= s
		inFlightWrites = append(inFlightWrites, w)
	}
	return true, inFlightWrites
}

// needTxnRetry determines whether the transaction needs to refresh (see
// txnSpanRefresher) or retry based on the batch response of a parallel
// commit attempt.
//
// TODO(nvanbenschoten): explain this better.
func (*txnCommitter) needTxnRetry(
	ba *roachpb.BatchRequest, br *roachpb.BatchResponse,
) (bool, roachpb.TransactionRetryReason) {
	// TODO(nvanbenschoten): Explain cases.
	if br.Txn.WriteTooOld {
		return true, roachpb.RETRY_WRITE_TOO_OLD
	} else if ba.Txn.Timestamp.Less(br.Txn.Timestamp) {
		return true, roachpb.RETRY_SERIALIZABLE
	}
	return false, 0
}

func (tc *txnCommitter) makeTxnCommitExplicitAsync(ctx context.Context, txn *roachpb.Transaction) {
	// log.VEventf(ctx, 2, "async abort for txn: %s", txn)
	if err := tc.stopper.RunAsyncTask(
		ctx, "txnCommitter: making txn commit explicit", func(ctx context.Context) {
			tc.mu.Lock()
			defer tc.mu.Unlock()
			if err := makeTxnCommitExplicit(ctx, tc.wrapped, txn); err != nil {
				// log.VErrEventf(ctx, 1, "async abort failed for %s: %s ", txn, pErr)
			}
		},
	); err != nil {
		log.Warning(ctx, err)
	}
}

func makeTxnCommitExplicit(ctx context.Context, s lockedSender, txn *roachpb.Transaction) error {
	// Construct a new batch with just an EndTransaction request. We don't
	// need to include any intents because the transaction record already
	// contains them.
	ba := roachpb.BatchRequest{}
	ba.Header = roachpb.Header{Txn: txn}
	ba.Add(&roachpb.EndTransactionRequest{Commit: true})

	// Retry until the
	const maxAttempts = 5
	return retry.WithMaxAttempts(ctx, base.DefaultRetryOptions(), maxAttempts, func() error {
		_, pErr := s.SendLocked(ctx, ba)
		if pErr == nil {
			return nil
		}
		// TODO(nvanbenschoten): Detect errors that indicate that someone else
		// has cleaned us up.
		return pErr.GoError()
	})
}

// setWrapped implements the txnInterceptor interface.
func (tc *txnCommitter) setWrapped(wrapped lockedSender) { tc.wrapped = wrapped }

// populateMetaLocked implements the txnReqInterceptor interface.
func (tc *txnCommitter) populateMetaLocked(meta *roachpb.TxnCoordMeta) {}

// augmentMetaLocked implements the txnReqInterceptor interface.
func (tc *txnCommitter) augmentMetaLocked(meta roachpb.TxnCoordMeta) {}

// epochBumpedLocked implements the txnReqInterceptor interface.
func (tc *txnCommitter) epochBumpedLocked() {}

// closeLocked implements the txnReqInterceptor interface.
func (tc *txnCommitter) closeLocked() {}

func cloneWithStatus(txn *roachpb.Transaction, s roachpb.TransactionStatus) *roachpb.Transaction {
	clone := txn.Clone()
	txn = &clone
	txn.Status = s
	return txn
}
