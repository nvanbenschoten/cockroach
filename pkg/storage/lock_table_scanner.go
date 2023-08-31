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
	"context"
	"sync"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/concurrency/lock"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/errors"
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

	if ltScanner.foundOwn(str) != nil {
		return nil
	}

	// Write the lock.
	ltKey, _ := LockTableKey{
		Key:      key,
		Strength: str,
		TxnUUID:  txn.ID,
	}.ToEngineKey(nil)
	var meta enginepb.MVCCMetadata
	meta.Txn = &txn.TxnMeta
	//if !ltScanner.alreadyLockedButRolledBack {
	//	meta.TxnDidNotUpdateMeta = &trueValue
	//}
	bytes, err := protoutil.Marshal(&meta)
	if err != nil {
		return err
	}
	return rw.PutEngineKey(ltKey, bytes)
}

// lockTableScanner is used to scan the replicated lock table. It looks for
// locks that conflict with a transaction and for locks that the transaction has
// already acquired.
type lockTableScanner struct {
	iter EngineIterator
	// The transaction attempting to acquire a lock.
	txn *roachpb.Transaction
	// The strength of the lock that the transaction is attempting to acquire.
	// TODO: allow None to collect no conflicts?
	str lock.Strength
	// Stop adding conflicting locks and abort scan once the maxConflicts limit
	// is reached. Ignored if zero.
	maxConflicts int64

	// Stores any error returned. If non-nil, iteration short circuits.
	err error
	// Stores any locks that conflict with the transaction and locking strength.
	conflicts []roachpb.Lock
	// Stores any locks that the transaction has already acquired.
	ownLocks [lock.MaxStrength]*enginepb.MVCCMetadata

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
	iter, err := reader.NewEngineIterator(IterOptions{Prefix: true})
	if err != nil {
		return nil, err
	}
	s := lockTableScannerPool.Get().(*lockTableScanner)
	s.iter = iter
	s.txn = txn
	s.str = str
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
	if s.shouldIgnoreLockTableKey(ltKey) {
		return s.advance()
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

// shouldIgnoreLockTableKey returns true if the lock table key can be ignored
// because it is not relevant to the scanning transaction.
func (s *lockTableScanner) shouldIgnoreLockTableKey(ltKey LockTableKey) bool {
	return s.txn.ID != ltKey.TxnUUID && !lockStrengthsConflict(s.str, ltKey.Strength)
}

func lockStrengthsConflict(str1, str2 lock.Strength) bool {
	m1, m2 := lock.Mode{Strength: str1}, lock.Mode{Strength: str2}
	return lock.Conflicts(m1, m2, nil /* sv */)
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
	if ltKey.TxnUUID != s.txn.ID {
		return s.consumeConflictingLock(ltKey, ltValue)
	}
	return s.consumeOwnLock(ltKey, ltValue)
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

// consumeOwnLock consumes a lock held by the scanning transaction.
func (s *lockTableScanner) consumeOwnLock(ltKey LockTableKey, ltValue *enginepb.MVCCMetadata) bool {
	// TODO(nvanbenschoten): avoid this copy.
	ltValueCopy := *ltValue
	s.ownLocks[ltKey.Strength] = &ltValueCopy
	return true
}

// foundOwn returns the lock table value for the provided strength if the
// transaction has already acquired a lock of that strength. Returns nil if not.
func (s *lockTableScanner) foundOwn(str lock.Strength) *enginepb.MVCCMetadata {
	return s.ownLocks[str]
}
