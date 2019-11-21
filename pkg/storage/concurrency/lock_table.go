// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package concurrency

import (
	"bytes"
	"context"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/storage/engine/enginepb"
	"github.com/cockroachdb/cockroach/pkg/storage/spanset"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
)

// lockTableImpl ...
type lockTableImpl struct {
	r engine.Reader
	// opts [spanset.NumSpanScope]engine.IterOptions
}

func (m *lockTableImpl) scan(req Request) (roachpb.Intent, bool, error) {
	it := engineLockIter{it: m.r.NewIterator(engine.IterOptions{Prefix: true})}
	for s := spanset.SpanScope(0); s < spanset.NumSpanScope; s++ {
		for a := spanset.SpanAccess(0); a < spanset.NumSpanAccess; a++ {
			ss := req.spans().GetSpans(a, s)
			for _, span := range ss {
				var maxTS hlc.Timestamp
				switch a {
				case spanset.SpanReadOnly:
					// Ignore locks above read timestamp.
					maxTS = span.Timestamp
				case spanset.SpanReadWrite:
					// Don't ignore any locks.
					maxTS = hlc.MaxTimestamp
				default:
					panic("unknown access")
				}

				lock, ok, err := scanForLocks(req.context(), &it, req.txn(), span.Span, maxTS)
				if err != nil {
					return roachpb.Intent{}, false, err
				} else if ok {
					return lock, true, nil
				}
			}
		}
	}
	return roachpb.Intent{}, false, nil
}

func scanForLocks(
	ctx context.Context,
	it *engineLockIter,
	txnID uuid.UUID,
	span roachpb.Span,
	maxTS hlc.Timestamp,
) (roachpb.Intent, bool, error) {
	if span.EndKey == nil {
		it.it.SetLowerBound(nil)
		it.it.SetUpperBound(nil)
		it.it.(engine.WithBounds).SetPrefix(true)
	} else {
		it.it.SetLowerBound(keys.LockTablePrefixKey(span.Key))
		it.it.SetUpperBound(keys.LockTablePrefixKey(span.EndKey))
		it.it.(engine.WithBounds).SetPrefix(false)
	}

	for it.seekGE(span.Key); ; it.next() {
		if ok, err := it.valid(); err != nil {
			return roachpb.Intent{}, false, err
		} else if !ok {
			break
		}

		unsafeKey := it.unsafeKey()
		// TODO(WIP): is this needed with the bounds set above?
		if span.EndKey == nil && unsafeKey.Compare(span.Key) > 0 {
			break
		} else if unsafeKey.Compare(span.EndKey) >= 0 {
			break
		}

		unsafeDetail := it.unsafeKeyDetail()
		if txnID != uuid.Nil && bytes.Equal(txnID.GetBytes(), unsafeDetail) {
			// Lock owned by this transaction.
			continue
		}

		lock := it.unsafeValue()
		if maxTS.Less(lock.Txn.WriteTimestamp) {
			continue
		}
		return roachpb.Intent{
			Span:   roachpb.Span{Key: append([]byte(nil), unsafeKey...)},
			Txn:    lock.Txn,
			Status: roachpb.PENDING,
		}, true, nil
	}
	return roachpb.Intent{}, false, nil
}

func (m *lockTableImpl) insertOrUpdate(
	ctx context.Context,
	w engine.Writer,
	ms *enginepb.MVCCStats,
	txn *enginepb.TxnMeta,
	key engine.MVCCKey,
	curLockWithVal func(*enginepb.MVCCLock) (bool, []byte, error),
) error {
	var lock enginepb.MVCCLock
	lockKey := keys.LockTableKey(key.Key, txn.ID)
	if ok, val, err := curLockWithVal(&lock); err != nil {
		return err
	} else if ok {
		lock.AddToIntentHistory(lock.Txn.Sequence, val)
		lock.Txn = *txn
		lock.Txn.WriteTimestamp.Forward(key.Timestamp)
		return engine.MVCCBlindPutProto(ctx, w, ms, lockKey, hlc.Timestamp{}, &lock, nil)
	}
	lock = enginepb.MVCCLock{Txn: *txn}
	lock.Txn.WriteTimestamp.Forward(key.Timestamp)
	return engine.MVCCBlindPutProto(ctx, w, ms, lockKey, hlc.Timestamp{}, &lock, nil)
}

func (m *lockTableImpl) removeOne(
	ctx context.Context,
	rw engine.ReadWriter,
	ms *enginepb.MVCCStats,
	intent roachpb.Intent,
	fn LockUpdatedCallback,
) error {
	lockKey := keys.LockTableKey(intent.Key, intent.Txn.ID)

	var lock enginepb.MVCCLock
	if ok, err := engine.MVCCGetProto(
		ctx, rw, lockKey, hlc.Timestamp{}, &lock, engine.MVCCGetOptions{},
	); err != nil {
		return err
	} else if !ok {
		return nil
	}
	return m.removeCurLock(ctx, rw, ms, intent, lock, lockKey, fn)
}

func (m *lockTableImpl) removeMany(
	ctx context.Context,
	rw engine.ReadWriter,
	ms *enginepb.MVCCStats,
	intent roachpb.Intent,
	max int64,
	fn LockUpdatedCallback,
) (int64, *roachpb.Span, error) {
	it := newLockIter(rw, engine.IterOptions{
		LowerBound: intent.Key, UpperBound: intent.EndKey,
	})
	it.setTxnFilter(intent.Txn.ID)
	defer it.close()

	var count int64
	var resume *roachpb.Span
	for it.seekGE(intent.Key); ; it.next() {
		if ok, err := it.valid(); err != nil {
			return 0, nil, err
		} else if !ok {
			break
		} else if count == max {
			resume = &roachpb.Span{Key: append([]byte(nil), it.unsafeKey()...), EndKey: intent.EndKey}
			break
		}

		lock := it.unsafeValue()
		singleIntent := intent
		singleIntent.Span = roachpb.Span{Key: it.unsafeKey()}
		if err := m.removeCurLock(ctx, rw, ms, singleIntent, lock, it.unsafeLockKey(), fn); err != nil {
			return 0, nil, err
		}
		count++
	}
	return count, resume, nil
}

func (m *lockTableImpl) removeCurLock(
	ctx context.Context,
	rw engine.ReadWriter,
	ms *enginepb.MVCCStats,
	intent roachpb.Intent,
	lock enginepb.MVCCLock,
	lockKey roachpb.Key,
	fn LockUpdatedCallback,
) error {
	epochsMatch := intent.Txn.Epoch == lock.Txn.Epoch
	timestampsMatch := intent.Txn.WriteTimestamp == lock.Txn.WriteTimestamp
	timestampsValid := !intent.Txn.WriteTimestamp.Less(lock.Txn.WriteTimestamp)
	commit := intent.Status == roachpb.COMMITTED && epochsMatch && timestampsValid

	inProgress := !intent.Status.IsFinalized() && lock.Txn.Epoch >= intent.Txn.Epoch
	pushed := inProgress && lock.Txn.WriteTimestamp.Less(intent.Txn.WriteTimestamp)

	if inProgress && !pushed {
		return nil
	}

	if commit {
		if err := engine.MVCCDelete(ctx, rw, ms, lockKey, hlc.Timestamp{}, nil); err != nil {
			return err
		}
		if timestampsMatch {
			return nil
		}
		return fn(engine.MVCCKey{Key: intent.Key, Timestamp: lock.Txn.WriteTimestamp}, intent)
	} else if pushed {
		panic("unsupported")
	} else {
		if err := engine.MVCCDelete(ctx, rw, ms, lockKey, hlc.Timestamp{}, nil); err != nil {
			return err
		}
		intent.Status = roachpb.ABORTED
		return fn(engine.MVCCKey{Key: intent.Key, Timestamp: lock.Txn.WriteTimestamp}, intent)
	}
}

func (m *lockTableImpl) newLockAwareReadWriter(
	rw engine.ReadWriter, txn *roachpb.Transaction,
) engine.ReadWriter {
	return newLockAwareReadWriter(rw, m, txn)
}

func (m *lockTableImpl) newLockAwareBatch(
	b engine.Batch, txn *roachpb.Transaction,
) engine.Batch {
	return newLockAwareBatch(b, m, txn)
}

func (m *lockTableImpl) setBounds(start, end roachpb.RKey) {
	// m.opts[spanset.SpanGlobal] = engine.IterOptions{
	// 	LowerBound: keys.LockTableKey(start.AsRawKey(), uuid.Nil),
	// 	UpperBound: keys.LockTableKey(end.AsRawKey(), uuid.Nil),
	// }
	// m.opts[spanset.SpanLocal] = engine.IterOptions{
	// 	LowerBound: keys.LockTableKey(keys.MakeRangeKeyPrefix(start), uuid.Nil),
	// 	UpperBound: keys.LockTableKey(keys.MakeRangeKeyPrefix(end), uuid.Nil),
	// }
}
