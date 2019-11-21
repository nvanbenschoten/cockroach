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

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/storage/engine/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
)

// lockIter is capable of iterating over locks.
type lockIter interface {
	// see engine.Iterator.Close.
	close()
	// see engine.Iterator.SeekGE.
	seekGE(roachpb.Key)
	// see engine.Iterator.SeekLT.
	seekLT(roachpb.Key)
	// see engine.Iterator.Next.
	next()
	// see engine.Iterator.Prev.
	prev()
	// see engine.Iterator.Valid.
	valid() (bool, error)
	// see engine.Iterator.UnsafeKey.
	unsafeKey() roachpb.Key
	// like unsafeKey, but for the detail of a decoded range key.
	unsafeKeyDetail() []byte
	// see engine.Iterator.UnsafeKey.
	unsafeLockKey() roachpb.Key
	// see engine.Iterator.UnsafeValue.
	unsafeValue() enginepb.MVCCLock
	// setTxnFilter instructs the iterator to only return locks for
	// the specified transaction.
	setTxnFilter(txnID uuid.UUID)
}

// engineLockIter is an iterator over the lock table keyspace. It holds a
// handle to an engine iterator.
type engineLockIter struct {
	it        engine.Iterator // may contain other local keys
	noBounds  bool
	txnFilter uuid.UUID // optional

	curKey    roachpb.Key
	curDetail []byte
	curMeta   enginepb.MVCCMetadata
	curLock   enginepb.MVCCLock
	err       error
}

func newLockIter(r engine.Reader, opts engine.IterOptions) lockIter {
	var lockOpts engine.IterOptions
	var noBounds bool
	if opts.Prefix {
		// TODO(WIP): this doesn't work because the prefix doesn't
		// work after appending a 0 timestamp :(
		// lockOpts.Prefix = true
		noBounds = true
		lockOpts.UpperBound = keys.LockTablePrefixKey(keys.MaxKey)
	}
	if k := opts.LowerBound; k != nil {
		lockOpts.LowerBound = keys.LockTablePrefixKey(k)
	}
	if k := opts.UpperBound; k != nil {
		lockOpts.UpperBound = keys.LockTablePrefixKey(k)
	}
	return &engineLockIter{it: r.NewIterator(lockOpts), noBounds: noBounds}
}

func (it *engineLockIter) close() {
	it.it.Close()
}

func (it *engineLockIter) seekGE(key roachpb.Key) {
	lockKey := keys.LockTablePrefixKey(key)
	if it.noBounds {
		it.it.SetUpperBound(keys.LockTablePrefixKey(key.Next()))
	}
	it.it.SeekGE(engine.MVCCKey{Key: lockKey})
	it.findNext(engine.Iterator.Next)
}

func (it *engineLockIter) seekLT(key roachpb.Key) {
	lockKey := keys.LockTablePrefixKey(key)
	it.it.SeekLT(engine.MVCCKey{Key: lockKey})
	it.findNext(engine.Iterator.Prev)
}

func (it *engineLockIter) next() {
	it.it.Next()
	it.findNext(engine.Iterator.Next)
}

func (it *engineLockIter) prev() {
	it.it.Prev()
	it.findNext(engine.Iterator.Prev)
}

func (it *engineLockIter) findNext(advance func(engine.Iterator)) {
	it.curKey = nil
	it.curDetail = nil
	it.curMeta.Reset()
	it.curLock.Reset()
	for ; ; advance(it.it) {
		if ok, err := it.valid(); !ok || err != nil {
			return
		}

		unsafeKey := it.it.UnsafeKey()
		key, suffix, detail, err := keys.DecodeRangeKey(unsafeKey.Key)
		if err != nil {
			it.err = err
			return
		} else if !bytes.Equal(suffix, keys.LocalLockTableSuffix) {
			// The lock table is interleaved with other range-local keys.
			continue
		} else if it.txnFilter != uuid.Nil && !bytes.Equal(it.txnFilter.GetBytes(), detail) {
			// Lock owned by a different transaction.
			continue
		}

		it.curKey = key
		it.curDetail = detail
		if err := it.it.ValueProto(&it.curMeta); err != nil {
			it.err = err
			return
		}
		value := roachpb.Value{RawBytes: it.curMeta.RawBytes}
		it.err = value.GetProto(&it.curLock)
		return
	}
}

func (it *engineLockIter) valid() (bool, error) {
	if it.err != nil {
		return false, it.err
	}
	return it.it.Valid()
}

func (it *engineLockIter) unsafeKey() roachpb.Key {
	return it.curKey
}

func (it *engineLockIter) unsafeKeyDetail() []byte {
	return it.curDetail
}

func (it *engineLockIter) unsafeLockKey() roachpb.Key {
	return it.it.UnsafeKey().Key
}

func (it *engineLockIter) unsafeValue() enginepb.MVCCLock {
	return it.curLock
}

func (it *engineLockIter) setTxnFilter(txnID uuid.UUID) {
	it.txnFilter = txnID
}

// memLockIter is an iterator over the lock table keyspace. It holds all
// locks in-memory, so it's primarily suited for caching locks retrived
// using a different lock iterator.
//
// TODO(WIP): this isn't actually used now, but could be used in cases
// where a transaction is reading over a small number of its own intents.
// In that case, we really only ever need to use a full-blown engineLockIter
// when a transaction is reading over a large number of its own intents
// (determined during the lockTable sequencing scan), which is very rare.
type memLockIter struct {
	locks []memLock
	pos   int
}

type memLock struct {
	key  roachpb.Key
	lock enginepb.MVCCLock
}

// lockAwareIter is an iterator that is aware of a transaction's own locks and
// performs the necessary translation steps to:
// 1. logically push existing intents down below the transaction's read
//    timestamp so that MVCC logic observes them even when it doesn't realize
//    they're part of the same transaction. This is required to get
//    read-your-writes working when a transactions read and write timestamp
//    diverge.
// 2. ignore intents from earlier transaction epochs.
// 3. read through the transaction's sequence history, which is stored on the
//    corresponding lock, and replace the current value with the one from the
//    sequence history when necessary.
//
// The abstraction could be extended in the future to be aware of transactions
// that are known to be committed/aborted and in the process of resolving
// intents. This would allow intent resolution to proceed without acquiring
// latches and synchronizing with other requests even if intent resolution
// needs to modify the provisional MVCC value.
//
// TODO(WIP): write more here.
// It expects that wrappedIt.Key() <= lockIt.Key(). An inversion of this
// ordering is a bug.
// It expects that there exists at least one version of a key in wrappedIt for
// every key in lockIt.
type lockAwareIter struct {
	wrappedIt engine.Iterator
	lockIt    lockIter
	txn       *roachpb.Transaction

	posOnLock bool
	inuse     bool
}

var _ engine.Iterator = (*lockAwareIter)(nil)

func (it *lockAwareIter) setTxn(txn *roachpb.Transaction) {
	it.lockIt.setTxnFilter(txn.ID)
	it.txn = txn
}

func (it *lockAwareIter) setPositionedOnLock() {
	okWrapped, _ := it.wrappedIt.Valid()
	okLock, _ := it.lockIt.valid()
	if okWrapped && okLock {
		keyWrapped := it.wrappedIt.UnsafeKey().Key
		keyLock := it.lockIt.unsafeKey()
		if /*util.RaceEnabled &&*/ keyWrapped.Compare(keyLock) > 0 {
			panic("inverted lockAwareIter")
		}
		it.posOnLock = keyWrapped.Equal(keyLock)
	} else {
		it.posOnLock = false
	}
}

func (it *lockAwareIter) curLockVisible() (bool, []byte) {
	lock := it.lockIt.unsafeValue()
	if lock.Txn.Epoch < it.txn.Epoch {
		return false, nil
	}
	if lock.Txn.Sequence > it.txn.Sequence {
		if val, ok := lock.GetIntentValue(it.txn.Sequence); ok {
			return true, val
		}
		return false, nil
	}
	return true, nil
}

// Close implements the SimpleIterator interface.
func (it *lockAwareIter) Close() {
	if !it.inuse {
		panic("closing idle iterator")
	}
	it.inuse = false
	it.wrappedIt.Close()
	it.lockIt.close()
}

// SeekGE implements the SimpleIterator interface.
func (it *lockAwareIter) SeekGE(key engine.MVCCKey) {
	if key.Equal(it.UnsafeKey()) {
		return
	}
	it.wrappedIt.SeekGE(key)
	it.lockIt.seekGE(key.Key)
	it.setPositionedOnLock()
}

// Valid implements the SimpleIterator interface.
func (it *lockAwareIter) Valid() (bool, error) {
	return it.wrappedIt.Valid()
}

// Next implements the SimpleIterator interface.
func (it *lockAwareIter) Next() {
	it.wrappedIt.Next()
	if it.posOnLock {
		it.lockIt.next()
	}
	it.setPositionedOnLock()
}

// NextKey implements the SimpleIterator interface.
func (it *lockAwareIter) NextKey() {
	it.wrappedIt.NextKey()
	if it.posOnLock {
		it.lockIt.next()
	}
	it.setPositionedOnLock()
}

// UnsafeKey implements the SimpleIterator interface.
func (it *lockAwareIter) UnsafeKey() engine.MVCCKey {
	key := it.wrappedIt.UnsafeKey()
	if it.posOnLock {
		if ok, _ := it.curLockVisible(); ok {
			key = engine.MVCCKey{
				Key:       key.Key,
				Timestamp: it.txn.ReadTimestamp.Prev(),
			}
			return key
		}
	}
	return key
}

// UnsafeValue implements the SimpleIterator interface.
func (it *lockAwareIter) UnsafeValue() []byte {
	if it.posOnLock {
		if ok, val := it.curLockVisible(); ok && val != nil {
			return val
		}
	}
	return it.wrappedIt.UnsafeValue()
}

// SeekLT implements the Iterator interface.
func (it *lockAwareIter) SeekLT(key engine.MVCCKey) {
	it.wrappedIt.SeekLT(key)
	it.lockIt.seekLT(key.Key)
	it.setPositionedOnLock()
	if !it.posOnLock {
		// Is this actually what we want? Seems bad.
		it.lockIt.next()
		it.setPositionedOnLock()
	}
}

// Prev implements the Iterator interface.
func (it *lockAwareIter) Prev() {
	it.wrappedIt.Prev()
	it.lockIt.prev()
	it.setPositionedOnLock()
	if !it.posOnLock {
		// Is this actually what we want? Seems bad.
		it.lockIt.next()
		it.setPositionedOnLock()
	}
}

// Key implements the Iterator interface.
func (it *lockAwareIter) Key() engine.MVCCKey {
	key := it.wrappedIt.Key()
	if it.posOnLock {
		if ok, _ := it.curLockVisible(); ok {
			return engine.MVCCKey{
				Key:       key.Key,
				Timestamp: it.txn.ReadTimestamp.Prev(),
			}
		}
	}
	return key
}

// Value implements the Iterator interface.
func (it *lockAwareIter) Value() []byte {
	if it.posOnLock {
		if ok, val := it.curLockVisible(); ok && val != nil {
			return val
		}
	}
	return it.wrappedIt.Value()
}

// ValueProto implements the Iterator interface.
func (it *lockAwareIter) ValueProto(msg protoutil.Message) error {
	if it.posOnLock {
		if ok, val := it.curLockVisible(); ok && val != nil {
			return protoutil.Unmarshal(val, msg)
		}
	}
	return it.wrappedIt.ValueProto(msg)
}

// ComputeStats implements the Iterator interface.
func (it *lockAwareIter) ComputeStats(
	start, end roachpb.Key, nowNanos int64,
) (enginepb.MVCCStats, error) {
	return it.wrappedIt.ComputeStats(start, end, nowNanos)
}

// FindSplitKey implements the Iterator interface.
func (it *lockAwareIter) FindSplitKey(
	start, end, minSplitKey roachpb.Key, targetSize int64,
) (engine.MVCCKey, error) {
	panic("unimplemented")
}

// CheckForKeyCollisions implements the Iterator interface.
func (it *lockAwareIter) CheckForKeyCollisions(
	sstData []byte, start, end roachpb.Key,
) (enginepb.MVCCStats, error) {
	panic("unimplemented")
}

// SetLowerBound implements the Iterator interface.
func (it *lockAwareIter) SetLowerBound(key roachpb.Key) {
	it.wrappedIt.SetLowerBound(key)
}

// SetUpperBound implements the Iterator interface.
func (it *lockAwareIter) SetUpperBound(key roachpb.Key) {
	it.wrappedIt.SetUpperBound(key)
}

// Stats implements the Iterator interface.
func (it *lockAwareIter) Stats() engine.IteratorStats {
	return it.wrappedIt.Stats()
}
