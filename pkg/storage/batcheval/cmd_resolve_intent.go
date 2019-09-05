// Copyright 2014 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package batcheval

import (
	"context"
	"log"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/batcheval/result"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/storage/spanset"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/pkg/errors"
)

func init() {
	RegisterCommand(roachpb.ResolveIntent, declareKeysResolveIntent, ResolveIntent)
}

func declareKeysResolveIntentCombined(
	desc *roachpb.RangeDescriptor, header roachpb.Header, req roachpb.Request, spans *spanset.SpanSet,
) {
	DefaultDeclareKeys(desc, header, req, spans)
	var status roachpb.TransactionStatus
	var txnID uuid.UUID
	switch t := req.(type) {
	case *roachpb.ResolveIntentRequest:
		status = t.Status
		txnID = t.IntentTxn.ID
	case *roachpb.ResolveIntentRangeRequest:
		status = t.Status
		txnID = t.IntentTxn.ID
	}
	if WriteAbortSpanOnResolve(status) {
		spans.Add(spanset.SpanReadWrite, roachpb.Span{Key: keys.AbortSpanKey(header.RangeID, txnID)})
	}
}

func declareKeysResolveIntent(
	desc *roachpb.RangeDescriptor, header roachpb.Header, req roachpb.Request, spans *spanset.SpanSet,
) {
	declareKeysResolveIntentCombined(desc, header, req, spans)
}

func resolveToMetricType(status roachpb.TransactionStatus, poison bool) *result.Metrics {
	var typ result.Metrics
	if WriteAbortSpanOnResolve(status) {
		if poison {
			typ.ResolvePoison = 1
		} else {
			typ.ResolveAbort = 1
		}
	} else {
		typ.ResolveCommit = 1
	}
	return &typ
}

// ResolveIntent resolves a write intent from the specified key
// according to the status of the transaction which created it.
func ResolveIntent(
	ctx context.Context, batch engine.ReadWriter, cArgs CommandArgs, resp roachpb.Response,
) (result.Result, error) {
	args := cArgs.Args.(*roachpb.ResolveIntentRequest)
	h := cArgs.Header
	ms := cArgs.Stats

	if h.Txn != nil {
		return result.Result{}, ErrTransactionUnsupported
	}

	intent := roachpb.Intent{
		Span:   args.Span(),
		Txn:    args.IntentTxn,
		Status: args.Status,
	}
	if err := detectIntentTimestampViolation(cArgs.EvalCtx.Clock(), intent); err != nil {
		if util.RaceEnabled {
			log.Fatal(ctx, err)
		}
		return result.Result{}, err
	}
	if err := engine.MVCCResolveWriteIntent(ctx, batch, ms, intent); err != nil {
		return result.Result{}, err
	}

	var res result.Result
	res.Local.Metrics = resolveToMetricType(args.Status, args.Poison)

	if WriteAbortSpanOnResolve(args.Status) {
		if err := SetAbortSpan(ctx, cArgs.EvalCtx, batch, ms, args.IntentTxn, args.Poison); err != nil {
			return result.Result{}, err
		}
	}
	return res, nil
}

// detectIntentTimestampViolation detects violations in the condition governing
// how committed intent timestamps can related to the clock of their Range's
// leaseholder. In order for the transaction observed timestamp optimization to
// work without allowing stale reads in clusters with loosly synchronized
// clocks, transaction's must never be committed at timestamps above that in the
// clock of any leaseholder that contains any of its intents. See the comment on
// roachpb.Transaction.ObservedTimestamps for a detailed discussion on why that
// invariant must hold.
//
// We can partially detect violations of that condition here by comparing the
// commit timestamp of the intent's transaction with the timestamp of the local
// clock.
func detectIntentTimestampViolation(clock *hlc.Clock, intent roachpb.Intent) error {
	if intent.Status != roachpb.COMMITTED {
		// Nothing to assert.
		return nil
	}
	now := clock.Now()
	if now.Less(intent.Txn.Timestamp) {
		return errors.Errorf("programming error: intent resolved to timestamp in future of local clock")
	}
	return nil
}
