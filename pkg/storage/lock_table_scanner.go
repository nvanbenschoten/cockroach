// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package storage

import (
	"bytes"
	"context"
	"sync"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/concurrency/lock"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble"
)

// MVCCCheckLock ...
func MVCCCheckLock(
	ctx context.Context,
	reader Reader,
	txn *roachpb.Transaction,
	str lock.Strength,
	key roachpb.Key,
	maxConflicts int64,
) error {
	ltScanner, err := newLockTableScanner(reader, txn, str, maxConflicts)
	if err != nil {
		return err
	}
	defer ltScanner.close()
	return ltScanner.scan(key)
}

func MVCCAcquireLock(
	ctx context.Context,
	rw ReadWriter,
	txn *roachpb.Transaction,
	str lock.Strength,
	key roachpb.Key,
	maxConflicts int64,
) error {
	ltScanner, err := newLockTableScanner(rw, txn, str, maxConflicts)
	if err != nil {
		return err
	}
	defer ltScanner.close()
	err = ltScanner.scan(key)
	if err != nil {
		return err
	}

	// Iterate over the replicated lock strengths, from strongest to weakest,
	// stopping at the lock strength that we'd like to acquire. If the loop
	// terminates, rolledBack will reference the desired lock strength.
	var rolledBack bool
	for _, iterStr := range equalOrStrongerStrengths(str) {
		foundLock := ltScanner.foundOwn(iterStr)
		rolledBack = false
		if foundLock != nil {
			if foundLock.Txn.Epoch > txn.Epoch {
				return errors.AssertionFailedf("TODO")
			} else if foundLock.Txn.Epoch < txn.Epoch {
				rolledBack = true
			} else if enginepb.TxnSeqIsIgnored(foundLock.Txn.Sequence, txn.IgnoredSeqNums) {
				rolledBack = true
			}
		}

		if foundLock != nil && !rolledBack {
			// Lock held at desired or stronger strength. No need to reacquire.
			return nil
		}
		// Proceed to check weaker strengths...
	}

	// Write the lock.
	buf := newPutBuffer()
	defer buf.release()

	newMeta := &buf.newMeta
	newMeta.Txn = &txn.TxnMeta
	_, _, err = buf.putLockMeta(rw, MakeMVCCMetadataKey(key), str, newMeta, rolledBack)
	return err
}

func mvccReleaseLock(
	ctx context.Context,
	writer Writer,
	str lock.Strength,
	meta *enginepb.MVCCMetadata,
	intent roachpb.LockUpdate,
	buf *putBuffer,
) error {
	canSingleDelHelper := singleDelOptimizationHelper{
		_didNotUpdateMeta: meta.TxnDidNotUpdateMeta,
		_hasIgnoredSeqs:   len(intent.IgnoredSeqNums) > 0,
		// NB: the value is only used if epochs match, so it doesn't
		// matter if we use the one from meta or incoming request here.
		_epoch: intent.Txn.Epoch,
	}

	_, _, err := buf.clearLockMeta(
		writer, MakeMVCCMetadataKey(intent.Key), str, canSingleDelHelper.onCommitIntent(), meta.Txn.ID, ClearOptions{
			ValueSizeKnown: true,
			ValueSize:      uint32(meta.Size()),
		})
	return err
}

// Fixed length slice for all supported lock strengths for replicated locks. May
// be used to iterate supported lock strengths in strength order (strongest to
// weakest).
var replicatedLockStrengths = [...]lock.Strength{lock.Intent, lock.Exclusive, lock.Shared}

// replicatedLockStrengthToIndexMap returns a mapping between (strength, index)
// pairs that can be used to index into the lockTableScanner.ownLocks array.
//
// Trying to use a lock strength that isn't supported with replicated locks to
// index into the lockTableScanner.ownLocks array will cause a runtime error.
var replicatedLockStrengthToIndexMap = func() [lock.MaxStrength + 1]int {
	var m [lock.MaxStrength + 1]int
	// Initialize all to -1.
	for str := range m {
		m[str] = -1
	}
	// Set the indices of the valid strengths.
	for i, str := range replicatedLockStrengths {
		m[str] = i
	}
	return m
}()

// equalOrStrongerStrengths returns all supported lock strengths for replicated
// locks that are as strong or stronger than the provided strength. The returned
// slice is ordered from strongest to weakest.
func equalOrStrongerStrengths(str lock.Strength) []lock.Strength {
	return replicatedLockStrengths[:replicatedLockStrengthToIndexMap[str]+1]
}

// needs:
// - MVCCCheckLock, single key, check for conflicting locks with str
// - MVCCAcquireLock, single key, check for conflicting locks with str, record all existing locks
// - mvccPutUsingIter, single key, check for conflicting locks with intent, record existing intent
// - MVCCResolveWriteIntent, single key, ignore conflicting locks, record all existing locks
// - MVCCResolveWriteIntentRange, multi key, ignore conflicting locks, record all existing locks
// - computeMinIntentTimestamp
//   + SeparatedIntentScanner
//   + ScanIntents
//   + ScanConflictingIntentsForDroppingLatchesEarly, multi key, record all intents, ignore locks

// lockTableScanner is used to scan the replicated lock table. It looks for
// locks that conflict with a transaction and for locks that the transaction has
// already acquired.
type lockTableScanner struct {
	iter EngineIterator
	// The transaction attempting to acquire a lock.
	txnID uuid.UUID
	// Stop adding conflicting locks and abort scan once the maxConflicts limit
	// is reached. Ignored if zero.
	maxConflicts int64

	// Stores any error returned. If non-nil, iteration short circuits.
	err error
	// Stores any locks that conflict with the transaction and locking strength.
	conflicts []roachpb.Lock
	// Stores any locks that the transaction has already acquired.
	ownLocks [len(replicatedLockStrengths)]*enginepb.MVCCMetadata

	// Avoids heap allocations.
	lockTableScannerAlloc
}

// lockTableScannerAlloc holds buffers that the lockTableScanner can use to
// avoid heap allocations. It is extracted into a separate struct to avoid being
// cleared when lockTableScanner is recycled.
type lockTableScannerAlloc struct {
	ltKeyBuf []byte
	ltValue  enginepb.MVCCMetadata
}

var lockTableScannerPool = sync.Pool{
	New: func() interface{} { return &lockTableScanner{} },
}

// newLockTableScanner creates a new lockTableScanner.
//
// TODO: give this a good comment with a code example.
func newLockTableScanner(
	reader Reader, txn *roachpb.Transaction, str lock.Strength, maxConflicts int64,
) (*lockTableScanner, error) {
	var txnID uuid.UUID
	if txn != nil {
		txnID = txn.ID
	}
	var minStr lock.Strength
	switch str {
	case lock.Shared:
		minStr = lock.Exclusive
	case lock.Exclusive, lock.Intent:
		minStr = lock.Shared
	}
	iter, err := newLockTableIter(reader, lockTableIterOptions{
		Prefix:      true,
		MatchTxnID:  txnID,
		MatchMinStr: minStr,
	})
	if err != nil {
		return nil, err
	}
	s := lockTableScannerPool.Get().(*lockTableScanner)
	s.iter = iter
	s.txnID = txnID
	s.maxConflicts = maxConflicts
	return s, nil
}

func (s *lockTableScanner) close() {
	s.iter.Close()
	s.ltValue.Reset()
	*s = lockTableScanner{lockTableScannerAlloc: s.lockTableScannerAlloc}
	lockTableScannerPool.Put(s)
}

// scan scans the lock table at the provided key for locks held by other
// transactions that conflict with the desired locking strength (if not None)
// and for locks of any strength that the transaction has already acquired.
func (s *lockTableScanner) scan(key roachpb.Key) error {
	s.beforeScan()
	for ok := s.seek(key); ok; ok = s.getOneAndAdvance() {
	}
	return s.afterScan()
}

// beforeScan resets the scanner's state before a scan.
func (s *lockTableScanner) beforeScan() {
	s.err = nil
	s.conflicts = nil
	for i := range s.ownLocks {
		s.ownLocks[i] = nil
	}
}

// afterScan returns any error encountered during the scan.
func (s *lockTableScanner) afterScan() error {
	if s.err != nil {
		return s.err
	}
	if len(s.conflicts) != 0 {
		return &kvpb.LockConflictError{Locks: s.conflicts}
	}
	return nil
}

// seek seeks the iterator to the first lock table key associated with the
// provided key.
func (s *lockTableScanner) seek(key roachpb.Key) bool {
	var ltKey roachpb.Key
	ltKey, s.ltKeyBuf = keys.LockTableSingleKey(key, s.ltKeyBuf)
	valid, err := s.iter.SeekEngineKeyGE(EngineKey{Key: ltKey})
	if err != nil {
		s.err = err
	}
	return valid
}

// advance advances the iterator to the next lock table key.
func (s *lockTableScanner) advance() bool {
	valid, err := s.iter.NextEngineKey()
	if err != nil {
		s.err = err
	}
	return valid
}

// getOneAndAdvance consumes the current lock table key and value and advances
// the iterator.
func (s *lockTableScanner) getOneAndAdvance() bool {
	ltKey, ok := s.getLockTableKey()
	if !ok {
		return false
	}
	ltValue, ok := s.getLockTableValue()
	if !ok {
		return false
	}
	if !s.consumeLockTableKeyValue(ltKey, ltValue) {
		return false
	}
	return s.advance()
}

// getLockTableKey decodes the current lock table key.
func (s *lockTableScanner) getLockTableKey() (LockTableKey, bool) {
	ltEngKey, err := s.iter.UnsafeEngineKey()
	if err != nil {
		s.err = err
		return LockTableKey{}, false
	}
	ltKey, err := ltEngKey.ToLockTableKey()
	if err != nil {
		s.err = err
		return LockTableKey{}, false
	}
	return ltKey, true
}

// getLockTableValue decodes the current lock table values.
func (s *lockTableScanner) getLockTableValue() (*enginepb.MVCCMetadata, bool) {
	ltValueBytes, err := s.iter.UnsafeValue()
	if err != nil {
		s.err = err
		return nil, false
	}
	if err := protoutil.Unmarshal(ltValueBytes, &s.ltValue); err != nil {
		s.err = err
		return nil, false
	}
	return &s.ltValue, true
}

// consumeLockTableKeyValue consumes the current lock table key and value, which
// is either a conflicting lock or a lock held by the scanning transaction.
func (s *lockTableScanner) consumeLockTableKeyValue(
	ltKey LockTableKey, ltValue *enginepb.MVCCMetadata,
) bool {
	if ltValue.Txn == nil {
		s.err = errors.AssertionFailedf("unexpectedly found non-transactional lock: %v", ltValue)
		return false
	}
	if ltKey.TxnUUID != ltValue.Txn.ID {
		s.err = errors.AssertionFailedf("lock table key (%+v) and value (%+v) txn ID mismatch", ltKey, ltValue)
		return false
	}
	if ltKey.TxnUUID == s.txnID {
		return s.consumeOwnLock(ltKey, ltValue)
	}
	return s.consumeConflictingLock(ltKey, ltValue)
}

// consumeOwnLock consumes a lock held by the scanning transaction.
func (s *lockTableScanner) consumeOwnLock(ltKey LockTableKey, ltValue *enginepb.MVCCMetadata) bool {
	// TODO(nvanbenschoten): avoid this copy.
	ltValueCopy := *ltValue
	s.ownLocks[replicatedLockStrengthToIndexMap[ltKey.Strength]] = &ltValueCopy
	return true
}

// consumeConflictingLock consumes a conflicting lock.
func (s *lockTableScanner) consumeConflictingLock(
	ltKey LockTableKey, ltValue *enginepb.MVCCMetadata,
) bool {
	conflict := roachpb.MakeLock(ltValue.Txn, ltKey.Key.Clone(), ltKey.Strength)
	s.conflicts = append(s.conflicts, conflict)
	if s.maxConflicts != 0 && s.maxConflicts == int64(len(s.conflicts)) {
		return false
	}
	return true
}

// foundOwn returns the lock table value for the provided strength if the
// transaction has already acquired a lock of that strength. Returns nil if not.
func (s *lockTableScanner) foundOwn(str lock.Strength) *enginepb.MVCCMetadata {
	return s.ownLocks[replicatedLockStrengthToIndexMap[str]]
}

// lockTableIter is an EngineIterator that iterates over locks in the lock table
// keyspace. It performs no translation of input or output keys or values, so it
// is used like a normal EngineIterator, with the limitation that it can only be
// used to iterate over the lock table keyspace.
//
// The benefit of using a lockTableIter is that it performs filtering of the
// locks in the lock table, only returning locks that match the configured
// filtering criteria and transparently skipping past locks that do not. The
// filtering criteria is expressed as a combination of two configuration
// parameters, at least one of which must be set:
//
//   - MatchTxnID: if set, the iterator return locks held by this transaction.
//
//   - MatchMinStr: if set, the iterator returns locks held by any transaction
//     with this strength or stronger.
//
// Expressed abstractly as a SQL query, the filtering criteria is:
//
//	SELECT * FROM lock_table WHERE (MatchTxnID  != 0 AND txn_id = MatchTxnID)
//	                            OR (MatchMinStr != 0 AND strength >= MatchMinStr)
//
// Pushing this filtering logic into the iterator is a convenience for its
// users. It also allows the iterator to use its knowledge of the lock table
// keyspace structure to efficiently skip past locks that do not match the
// filtering criteria. It does this by seeking past many ignored locks when
// appropriate to avoid cases of O(ignored_locks) work, instead performing at
// most O(matching_locks + locked_keys) work.
//
// A common case where this matters is with shared locks. If the iterator is
// configured to ignore shared locks, a single key with a large number of shared
// locks can be skipped over with a single seek. Avoiding this unnecessary work
// is essential to avoiding quadratic behavior during shared lock acquisition
// and release.
type lockTableIter struct {
	iter EngineIterator
	// If set, return locks with any strength held by this transaction.
	matchTxnID uuid.UUID
	// If set, return locks held by any transaction with this strength or
	// stronger.
	matchMinStr lock.Strength
}

var _ EngineIterator = &lockTableIter{}

// lockTableIterOptions contains options used to create an lockTableIter.
type lockTableIterOptions struct {
	// See IterOptions.Prefix.
	Prefix bool
	// See IterOptions.LowerBound.
	LowerBound roachpb.Key
	// See IterOptions.UpperBound.
	UpperBound roachpb.Key

	// If set, return locks with any strength held by this transaction.
	MatchTxnID uuid.UUID
	// If set, return locks held by any transaction with this strength or
	// stronger.
	MatchMinStr lock.Strength
}

// validate validates the lockTableIterOptions.
func (opts lockTableIterOptions) validate() error {
	if !opts.Prefix && len(opts.UpperBound) == 0 && len(opts.LowerBound) == 0 {
		return errors.AssertionFailedf("lockTableIter must set prefix or upper bound or lower bound")
	}
	if len(opts.LowerBound) != 0 && !isLockTableKey(opts.LowerBound) {
		return errors.AssertionFailedf("lockTableIter upper bound must be a lock table key")
	}
	if len(opts.UpperBound) != 0 && !isLockTableKey(opts.UpperBound) {
		return errors.AssertionFailedf("lockTableIter upper bound must be a lock table key")
	}
	if opts.MatchTxnID == uuid.Nil && opts.MatchMinStr == 0 {
		return errors.AssertionFailedf("lockTableIter must specify MatchTxnID, MatchMinStr, or both")
	}
	return nil
}

// toIterOptions converts the lockTableIterOptions to IterOptions.
func (opts lockTableIterOptions) toIterOptions() IterOptions {
	return IterOptions{
		Prefix:     opts.Prefix,
		LowerBound: opts.LowerBound,
		UpperBound: opts.UpperBound,
	}
}

var lockTableIterPool = sync.Pool{
	New: func() interface{} { return &lockTableIter{} },
}

// newLockTableIter creates a new lockTableIter.
func newLockTableIter(reader Reader, opts lockTableIterOptions) (*lockTableIter, error) {
	if err := opts.validate(); err != nil {
		return nil, err
	}
	iter, err := reader.NewEngineIterator(opts.toIterOptions())
	if err != nil {
		return nil, err
	}
	ltIter := lockTableIterPool.Get().(*lockTableIter)
	*ltIter = lockTableIter{
		iter:        iter,
		matchTxnID:  opts.MatchTxnID,
		matchMinStr: opts.MatchMinStr,
	}
	return ltIter, nil
}

// SeekEngineKeyGE implements the EngineIterator interface.
func (i *lockTableIter) SeekEngineKeyGE(key EngineKey) (valid bool, err error) {
	if err := checkLockTableKey(key.Key); err != nil {
		return false, err
	}
	valid, err = i.iter.SeekEngineKeyGE(key)
	if !valid || err != nil {
		return valid, err
	}
	state, err := i.advanceToMatchingLock(+1, nil)
	return state == pebble.IterValid, err
}

// SeekEngineKeyLT implements the EngineIterator interface.
func (i *lockTableIter) SeekEngineKeyLT(key EngineKey) (valid bool, err error) {
	if err := checkLockTableKey(key.Key); err != nil {
		return false, err
	}
	valid, err = i.iter.SeekEngineKeyLT(key)
	if !valid || err != nil {
		return valid, err
	}
	state, err := i.advanceToMatchingLock(-1, nil)
	return state == pebble.IterValid, err
}

// NextEngineKey implements the EngineIterator interface.
func (i *lockTableIter) NextEngineKey() (valid bool, err error) {
	valid, err = i.iter.NextEngineKey()
	if !valid || err != nil {
		return valid, err
	}
	state, err := i.advanceToMatchingLock(+1, nil)
	return state == pebble.IterValid, err
}

// PrevEngineKey implements the EngineIterator interface.
func (i *lockTableIter) PrevEngineKey() (valid bool, err error) {
	valid, err = i.iter.PrevEngineKey()
	if !valid || err != nil {
		return valid, err
	}
	state, err := i.advanceToMatchingLock(-1, nil)
	return state == pebble.IterValid, err
}

// SeekEngineKeyGEWithLimit implements the EngineIterator interface.
func (i *lockTableIter) SeekEngineKeyGEWithLimit(
	key EngineKey, limit roachpb.Key,
) (state pebble.IterValidityState, err error) {
	if err := checkLockTableKey(key.Key); err != nil {
		return 0, err
	}
	state, err = i.iter.SeekEngineKeyGEWithLimit(key, limit)
	if state != pebble.IterValid || err != nil {
		return state, err
	}
	return i.advanceToMatchingLock(+1, limit)
}

// SeekEngineKeyLTWithLimit implements the EngineIterator interface.
func (i *lockTableIter) SeekEngineKeyLTWithLimit(
	key EngineKey, limit roachpb.Key,
) (state pebble.IterValidityState, err error) {
	if err := checkLockTableKey(key.Key); err != nil {
		return 0, err
	}
	state, err = i.iter.SeekEngineKeyLTWithLimit(key, limit)
	if state != pebble.IterValid || err != nil {
		return state, err
	}
	return i.advanceToMatchingLock(-1, limit)
}

// NextEngineKeyWithLimit implements the EngineIterator interface.
func (i *lockTableIter) NextEngineKeyWithLimit(
	limit roachpb.Key,
) (state pebble.IterValidityState, err error) {
	state, err = i.iter.NextEngineKeyWithLimit(limit)
	if state != pebble.IterValid || err != nil {
		return state, err
	}
	return i.advanceToMatchingLock(+1, limit)
}

// PrevEngineKeyWithLimit implements the EngineIterator interface.
func (i *lockTableIter) PrevEngineKeyWithLimit(
	limit roachpb.Key,
) (state pebble.IterValidityState, err error) {
	state, err = i.iter.PrevEngineKeyWithLimit(limit)
	if state != pebble.IterValid || err != nil {
		return state, err
	}
	return i.advanceToMatchingLock(-1, limit)
}

// advanceToMatchingLock advances the iterator to the next lock table key that
// matches the configured filtering criteria. If limit is non-nil, the iterator
// will stop advancing once it reaches the limit.
func (i *lockTableIter) advanceToMatchingLock(
	dir int, limit roachpb.Key,
) (state pebble.IterValidityState, err error) {
	for {
		engineKey, err := i.iter.UnsafeEngineKey()
		if err != nil {
			return 0, err
		}
		str, txnID, err := engineKey.DecodeLockTableKeyVersion()
		if err != nil {
			return 0, err
		}
		if i.matchingLock(str, txnID) {
			return pebble.IterValid, nil
		}

		// TODO(nvanbenschoten): implement maxIntentItersBeforeSeek.
		if limit != nil {
			if dir < 0 {
				state, err = i.iter.PrevEngineKeyWithLimit(limit)
			} else {
				state, err = i.iter.NextEngineKeyWithLimit(limit)
			}
		} else {
			var valid bool
			if dir < 0 {
				valid, err = i.iter.PrevEngineKey()
			} else {
				valid, err = i.iter.NextEngineKey()
			}
			if valid {
				state = pebble.IterValid
			} else {
				state = pebble.IterExhausted
			}
		}
		if state != pebble.IterValid || err != nil {
			return state, err
		}
	}
}

// matchingLock returns whether the lock table key with the provided strength
// and transaction ID matches the configured filtering criteria.
func (i *lockTableIter) matchingLock(str lock.Strength, txnID uuid.UUID) bool {
	// Is this a lock held by the desired transaction?
	return (i.matchTxnID != uuid.Nil && i.matchTxnID == txnID) ||
		// Or, is this a lock with the desired strength or stronger?
		(i.matchMinStr != 0 && i.matchMinStr <= str)
}

// Close implements the EngineIterator interface.
func (i *lockTableIter) Close() {
	i.iter.Close()
	*i = lockTableIter{}
	lockTableIterPool.Put(i)
}

// HasPointAndRange implements the EngineIterator interface.
func (i *lockTableIter) HasPointAndRange() (bool, bool) {
	return i.iter.HasPointAndRange()
}

// EngineRangeBounds implements the EngineIterator interface.
func (i *lockTableIter) EngineRangeBounds() (roachpb.Span, error) {
	return i.iter.EngineRangeBounds()
}

// EngineRangeKeys implements the EngineIterator interface.
func (i *lockTableIter) EngineRangeKeys() []EngineRangeKeyValue {
	return i.iter.EngineRangeKeys()
}

// RangeKeyChanged implements the EngineIterator interface.
func (i *lockTableIter) RangeKeyChanged() bool {
	return i.iter.RangeKeyChanged()
}

// UnsafeEngineKey implements the EngineIterator interface.
func (i *lockTableIter) UnsafeEngineKey() (EngineKey, error) {
	return i.iter.UnsafeEngineKey()
}

// EngineKey implements the EngineIterator interface.
func (i *lockTableIter) EngineKey() (EngineKey, error) {
	return i.iter.EngineKey()
}

// UnsafeRawEngineKey implements the EngineIterator interface.
func (i *lockTableIter) UnsafeRawEngineKey() []byte {
	return i.iter.UnsafeRawEngineKey()
}

// LockTableKeyVersion returns the strength and txn ID from the version of the
// current key.
func (i *lockTableIter) LockTableKeyVersion() (lock.Strength, uuid.UUID, error) {
	k, err := i.iter.UnsafeEngineKey()
	if err != nil {
		return 0, uuid.UUID{}, errors.Wrap(err, "retrieving lock table key")
	}
	return k.DecodeLockTableKeyVersion()
}

// UnsafeValue implements the EngineIterator interface.
func (i *lockTableIter) UnsafeValue() ([]byte, error) {
	return i.iter.UnsafeValue()
}

// UnsafeLazyValue implements the EngineIterator interface.
// TODO(nvanbenschoten): add this to the EngineIterator interface.
func (i *lockTableIter) UnsafeLazyValue() pebble.LazyValue {
	return i.iter.(*pebbleIterator).UnsafeLazyValue()
}

// Value implements the EngineIterator interface.
func (i *lockTableIter) Value() ([]byte, error) {
	return i.iter.Value()
}

// ValueLen implements the EngineIterator interface.
func (i *lockTableIter) ValueLen() int {
	return i.iter.ValueLen()
}

// ValueProto unmarshals the current value into the provided proto.
func (i *lockTableIter) ValueProto(meta *enginepb.MVCCMetadata) error {
	v, err := i.iter.UnsafeValue()
	if err != nil {
		return errors.Wrap(err, "retrieving lock table value")
	}
	return protoutil.Unmarshal(v, meta)
}

// CloneContext implements the EngineIterator interface.
func (i *lockTableIter) CloneContext() CloneContext {
	return i.iter.CloneContext()
}

// Stats implements the EngineIterator interface.
func (i *lockTableIter) Stats() IteratorStats {
	return i.iter.Stats()
}

func isLockTableKey(key roachpb.Key) bool {
	return bytes.HasPrefix(key, keys.LocalRangeLockTablePrefix)
}

func checkLockTableKey(key roachpb.Key) error {
	if isLockTableKey(key) {
		return nil
	}
	return errors.AssertionFailedf("key %s is not a lock table key", key)
}
