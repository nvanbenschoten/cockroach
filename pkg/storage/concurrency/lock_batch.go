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
	"context"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/storage/engine/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
)

var _ engine.ReadWriter = &lockAwareReadWriter{}
var _ engine.Batch = &lockAwareBatch{}

func newLockAwareReadWriter(rw engine.ReadWriter, lt *lockTableImpl, txn *roachpb.Transaction) engine.ReadWriter {
	return &lockAwareReadWriter{
		rw:  rw,
		lt:  lt,
		txn: txn,
	}
}

func newLockAwareBatch(b engine.Batch, lt *lockTableImpl, txn *roachpb.Transaction) engine.Batch {
	return &lockAwareBatch{
		lockAwareReadWriter: lockAwareReadWriter{
			rw:  b,
			lt:  lt,
			txn: txn,
		},
		b:   b,
		lt:  lt,
		txn: txn,
	}
}

// lockAwareReadWriter is an engine.ReadWriter that is bound to a specific
// transaction, inserts locks on the transaction's behalf when it writes, and is
// aware of that the transaction's locks so that it can ensure that it always
// reads-its-writes.
type lockAwareReadWriter struct {
	rw  engine.ReadWriter
	lt  *lockTableImpl
	txn *roachpb.Transaction

	it lockAwareIter
}

func (rw *lockAwareReadWriter) Close() {
	rw.rw.Close()
}

func (rw *lockAwareReadWriter) Closed() bool {
	return rw.rw.Closed()
}

func (rw *lockAwareReadWriter) ExportToSst(
	startKey, endKey roachpb.Key,
	startTS, endTS hlc.Timestamp,
	exportAllRevisions bool,
	io engine.IterOptions,
) ([]byte, roachpb.BulkOpSummary, error) {
	panic("unimplemented")
}

func (rw *lockAwareReadWriter) Get(key engine.MVCCKey) ([]byte, error) {
	// Deprecated and mostly unused API, so don't support.
	panic("unimplemented")
}

func (rw *lockAwareReadWriter) GetProto(
	key engine.MVCCKey, msg protoutil.Message,
) (bool, int64, int64, error) {
	// Deprecated and mostly unused API, so don't support.
	panic("unimplemented")
}

func (rw *lockAwareReadWriter) Iterate(
	start, end roachpb.Key, f func(engine.MVCCKeyValue) (bool, error),
) error {
	panic("unimplemented")
}

func (rw *lockAwareReadWriter) NewIterator(opts engine.IterOptions) engine.Iterator {
	it := &rw.it
	it.inuse = true
	it.wrappedIt = rw.rw.NewIterator(opts)
	it.lockIt = newLockIter(rw.rw, opts)
	it.setTxn(rw.txn)
	return it
}

func (rw *lockAwareReadWriter) ApplyBatchRepr(repr []byte, sync bool) error {
	panic("unimplemented")
}

func (rw *lockAwareReadWriter) Clear(key engine.MVCCKey) error {
	panic("unimplemented")
}

func (rw *lockAwareReadWriter) SingleClear(key engine.MVCCKey) error {
	panic("unimplemented")
}

func (rw *lockAwareReadWriter) ClearRange(start, end engine.MVCCKey) error {
	panic("unimplemented")
}

func (rw *lockAwareReadWriter) ClearIterRange(iter engine.Iterator, start, end roachpb.Key) error {
	panic("unimplemented")
}

func (rw *lockAwareReadWriter) Merge(key engine.MVCCKey, value []byte) error {
	panic("unimplemented")
}

func (rw *lockAwareReadWriter) Put(key engine.MVCCKey, value []byte) error {
	// TODO(WIP): the fact that we don't have access to the MVCCStats here indicates
	// a problem with the writer abstraction for this use case. Is it too low-level
	// to provide polymorphism over operations that write MVCC KVs?
	var ms *enginepb.MVCCStats
	if err := rw.lt.insertOrUpdate(
		context.Background(), rw.rw, ms, &rw.txn.TxnMeta, key,
		func(lock *enginepb.MVCCLock) (bool, []byte, error) {
			// This is pretty gross.
			if !rw.it.inuse {
				// Must have been a blind-put.
				return false, nil, nil
			}
			if !rw.it.posOnLock {
				return false, nil, nil
			}
			*lock = rw.it.lockIt.unsafeValue()
			if lock.Txn.Epoch < rw.txn.Epoch {
				return false, nil, nil
			}
			return true, rw.it.wrappedIt.UnsafeValue(), nil
		},
	); err != nil {
		return err
	}
	return rw.rw.Put(key, value)
}

func (rw *lockAwareReadWriter) LogData(data []byte) error {
	panic("unimplemented")
}

func (rw *lockAwareReadWriter) LogLogicalOp(
	op engine.MVCCLogicalOpType, details engine.MVCCLogicalOpDetails,
) {
	// TODO(WIP): this is now where we know whether we're writing an intent or
	// writing a new value directly. I guess the way this would work is that we'd
	// translate the operation at this level when we're actually writing an intent
	// instead of a value. That should work fine and is somewhat clean.
	rw.rw.LogLogicalOp(op, details)
}

// lockAwareBatch is an engine.Batch that is bound to a specific transaction,
// inserts locks on the transaction's behalf when it writes, and is aware of
// that the transaction's locks so that it can ensure that it always
// reads-its-writes.
type lockAwareBatch struct {
	lockAwareReadWriter
	b   engine.Batch
	lt  *lockTableImpl
	txn *roachpb.Transaction
}

func (b *lockAwareBatch) Commit(sync bool) error {
	panic("unimplemented")
}

func (b *lockAwareBatch) Distinct() engine.ReadWriter {
	return newLockAwareReadWriter(b.b.Distinct(), b.lt, b.txn)
}

func (b *lockAwareBatch) Empty() bool {
	return b.b.Empty()
}

func (b *lockAwareBatch) Len() int {
	return b.b.Len()
}

func (b *lockAwareBatch) Repr() []byte {
	return b.b.Repr()
}
