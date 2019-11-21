// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package engine

import (
	"bytes"
	"encoding/binary"
	"sync"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/engine/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/errors"
)

const (
	maxItersBeforeSeek = 10
)

// Struct to store MVCCScan / MVCCGet in the same binary format as that
// expected by MVCCScanDecodeKeyValue.
type pebbleResults struct {
	count int64
	repr  []byte
	bufs  [][]byte
}

func (p *pebbleResults) clear() {
	*p = pebbleResults{}
}

// The repr that MVCCScan / MVCCGet expects to provide as output goes:
// <valueLen:Uint32><keyLen:Uint32><Key><Value>
// This function adds to repr in that format.
func (p *pebbleResults) put(key MVCCKey, value []byte) {
	// Key value lengths take up 8 bytes (2 x Uint32).
	const kvLenSize = 8
	const minSize = 16
	const maxSize = 128 << 20 // 128 MB

	// We maintain a list of buffers, always encoding into the last one (a.k.a.
	// pebbleResults.repr). The size of the buffers is exponentially increasing,
	// capped at maxSize.
	lenKey := key.Len()
	lenToAdd := kvLenSize + lenKey + len(value)
	if len(p.repr)+lenToAdd > cap(p.repr) {
		newSize := 2 * cap(p.repr)
		if newSize == 0 {
			newSize = minSize
		}
		for newSize < lenToAdd && newSize < maxSize {
			newSize *= 2
		}
		if len(p.repr) > 0 {
			p.bufs = append(p.bufs, p.repr)
		}
		p.repr = nonZeroingMakeByteSlice(newSize)[:0]
	}

	startIdx := len(p.repr)
	p.repr = p.repr[:startIdx+lenToAdd]
	binary.LittleEndian.PutUint32(p.repr[startIdx:], uint32(len(value)))
	binary.LittleEndian.PutUint32(p.repr[startIdx+4:], uint32(lenKey))
	encodeKeyToBuf(p.repr[startIdx+kvLenSize:startIdx+kvLenSize+lenKey], key, lenKey)
	copy(p.repr[startIdx+kvLenSize+lenKey:], value)
	p.count++
}

func (p *pebbleResults) finish() [][]byte {
	if len(p.repr) > 0 {
		p.bufs = append(p.bufs, p.repr)
		p.repr = nil
	}
	return p.bufs
}

// Go port of mvccScanner in libroach/mvcc.h. Stores all variables relating to
// one MVCCGet / MVCCScan call.
type pebbleMVCCScanner struct {
	parent  Iterator
	reverse bool
	peeked  bool
	// Iteration bounds. Does not contain MVCC timestamp.
	start, end roachpb.Key
	// Timestamp with which MVCCScan/MVCCGet was called.
	ts hlc.Timestamp
	// Max number of keys to return.
	maxKeys int64
	// Transaction epoch and sequence number.
	txn         *roachpb.Transaction
	txnEpoch    enginepb.TxnEpoch
	txnSequence enginepb.TxnSeq
	// Metadata object for unmarshalling intents.
	meta enginepb.MVCCMetadata
	// Bools copied over from MVCC{Scan,Get}Options. See the comment on the
	// package level MVCCScan for what these mean.
	inconsistent, tombstones bool
	checkUncertainty         bool
	keyBuf                   []byte
	savedBuf                 []byte
	// cur* variables store the "current" record we're pointing to. Updated in
	// updateCurrent.
	curKey, curValue []byte
	curTS            hlc.Timestamp
	results          pebbleResults
	// Stores any error returned. If non-nil, iteration short circuits.
	err error
	// Number of iterations to try before we do a Seek/SeekReverse. Stays within
	// [1, maxItersBeforeSeek] and defaults to maxItersBeforeSeek/2 .
	itersBeforeSeek int
}

// Pool for allocating pebble MVCC Scanners.
var pebbleMVCCScannerPool = sync.Pool{
	New: func() interface{} {
		return &pebbleMVCCScanner{}
	},
}

// init sets bounds on the underlying pebble iterator, and initializes other
// fields not set by the calling method.
func (p *pebbleMVCCScanner) init() {
	p.itersBeforeSeek = maxItersBeforeSeek / 2
}

// get iterates exactly once and adds one KV to the result set.
func (p *pebbleMVCCScanner) get() {
	p.parent.SeekGE(MVCCKey{Key: p.start})
	if !p.updateCurrent() {
		return
	}
	p.getAndAdvance()
}

// scan iterates until maxKeys records are in results, or the underlying
// iterator is exhausted, or an error is encountered.
func (p *pebbleMVCCScanner) scan() (*roachpb.Span, error) {
	if p.reverse {
		if !p.iterSeekReverse(MVCCKey{Key: p.end}) {
			return nil, p.err
		}
	} else {
		if !p.iterSeek(MVCCKey{Key: p.start}) {
			return nil, p.err
		}
	}

	for p.getAndAdvance() {
	}

	var resume *roachpb.Span
	if p.results.count == p.maxKeys && p.advanceKey() {
		if p.reverse {
			// curKey was not added to results, so it needs to be included in the
			// resume span.
			resume = &roachpb.Span{
				Key:    p.start,
				EndKey: roachpb.Key(p.curKey).Next(),
			}
		} else {
			resume = &roachpb.Span{
				Key:    p.curKey,
				EndKey: p.end,
			}
		}
	}
	return resume, p.err
}

// Increments itersBeforeSeek while ensuring it stays <= maxItersBeforeSeek
func (p *pebbleMVCCScanner) incrementItersBeforeSeek() {
	p.itersBeforeSeek++
	if p.itersBeforeSeek > maxItersBeforeSeek {
		p.itersBeforeSeek = maxItersBeforeSeek
	}
}

// Decrements itersBeforeSeek while ensuring it stays positive.
func (p *pebbleMVCCScanner) decrementItersBeforeSeek() {
	p.itersBeforeSeek--
	if p.itersBeforeSeek < 1 {
		p.itersBeforeSeek = 1
	}
}

// Returns an uncertainty error with the specified timestamp and p.txn.
func (p *pebbleMVCCScanner) uncertaintyError(ts hlc.Timestamp) bool {
	p.err = roachpb.NewReadWithinUncertaintyIntervalError(p.ts, ts, p.txn)
	p.results.clear()
	return false
}

// Emit a tuple and return true if we have reason to believe iteration can
// continue.
func (p *pebbleMVCCScanner) getAndAdvance() bool {
	mvccKey := MVCCKey{p.curKey, p.curTS}
	if mvccKey.IsValue() {
		if !p.ts.Less(p.curTS) {
			// 1. Fast path: there is no intent and our read timestamp is newer than
			// the most recent version's timestamp.
			return p.addAndAdvance(p.curValue)
		}

		if p.checkUncertainty {
			// 2. Our txn's read timestamp is less than the max timestamp
			// seen by the txn. We need to check for clock uncertainty
			// errors.
			if !p.txn.MaxTimestamp.Less(p.curTS) {
				return p.uncertaintyError(p.curTS)
			}

			return p.seekVersion(p.txn.MaxTimestamp, true)
		}

		// 3. Our txn's read timestamp is greater than or equal to the
		// max timestamp seen by the txn so clock uncertainty checks are
		// unnecessary. We need to seek to the desired version of the
		// value (i.e. one with a timestamp earlier than our read
		// timestamp).
		return p.seekVersion(p.ts, false)
	}

	if len(p.curValue) == 0 {
		p.err = errors.Errorf("zero-length mvcc metadata")
		return false
	}
	err := protoutil.Unmarshal(p.curValue, &p.meta)
	if err != nil {
		p.err = errors.Errorf("unable to decode MVCCMetadata: %s", err)
		return false
	}
	if len(p.meta.RawBytes) == 0 {
		p.err = errors.Errorf("inline key without value")
		return false
	}
	// 4. Emit immediately if the value is inline.
	return p.addAndAdvance(p.meta.RawBytes)
}

// nextKey advances to the next user key.
func (p *pebbleMVCCScanner) nextKey() bool {
	p.keyBuf = append(p.keyBuf[:0], p.curKey...)

	for i := 0; i < p.itersBeforeSeek; i++ {
		if !p.iterNext() {
			return false
		}
		if !bytes.Equal(p.curKey, p.keyBuf) {
			p.incrementItersBeforeSeek()
			return true
		}
	}

	p.decrementItersBeforeSeek()
	// We're pointed at a different version of the same key. Fall back to
	// seeking to the next key. We append a NUL to account for the "next-key".
	p.keyBuf = append(p.keyBuf, 0)
	return p.iterSeek(MVCCKey{Key: p.keyBuf})
}

// backwardLatestVersion backs up the iterator to the latest version for the
// specified key. The parameter i is used to maintain iteration count between
// the loop here and the caller (usually prevKey). Returns false if the
// iterator was exhausted. Assumes that the iterator is currently positioned at
// the oldest version of key.
func (p *pebbleMVCCScanner) backwardLatestVersion(key []byte, i int) bool {
	p.keyBuf = append(p.keyBuf[:0], key...)

	for ; i < p.itersBeforeSeek; i++ {
		peekedKey, ok := p.iterPeekPrev()
		if !ok {
			// No previous entry exists, so we're at the latest version of key.
			return true
		}
		if !bytes.Equal(peekedKey, p.keyBuf) {
			p.incrementItersBeforeSeek()
			return true
		}
		if !p.iterPrev() {
			return false
		}
	}

	p.decrementItersBeforeSeek()
	return p.iterSeek(MVCCKey{Key: p.keyBuf})
}

// prevKey advances to the newest version of the user key preceding the
// specified key. Assumes that the iterator is currently positioned at
// key or 1 record after key.
func (p *pebbleMVCCScanner) prevKey(key []byte) bool {
	p.keyBuf = append(p.keyBuf[:0], key...)

	for i := 0; i < p.itersBeforeSeek; i++ {
		peekedKey, ok := p.iterPeekPrev()
		if !ok {
			return false
		}
		if !bytes.Equal(peekedKey, p.keyBuf) {
			return p.backwardLatestVersion(peekedKey, i+1)
		}
		if !p.iterPrev() {
			return false
		}
	}

	p.decrementItersBeforeSeek()
	return p.iterSeekReverse(MVCCKey{Key: p.keyBuf})
}

// advanceKey advances to the next key in the iterator's direction.
func (p *pebbleMVCCScanner) advanceKey() bool {
	if p.reverse {
		return p.prevKey(p.curKey)
	}
	return p.nextKey()
}

// advanceKeyAtEnd advances to the next key when the iterator's end has been
// reached.
func (p *pebbleMVCCScanner) advanceKeyAtEnd() bool {
	if p.reverse {
		// Iterating to the next key might have caused the iterator to reach the
		// end of the key space. If that happens, back up to the very last key.
		p.peeked = false
		p.parent.SeekLT(MVCCKey{Key: keys.MaxKey})
		if !p.updateCurrent() {
			return false
		}
		return p.advanceKey()
	}
	// We've reached the end of the iterator and there is nothing left to do.
	return false
}

// advanceKeyAtNewKey advances to the key after the specified key, assuming we
// have just reached the specified key.
func (p *pebbleMVCCScanner) advanceKeyAtNewKey(key []byte) bool {
	if p.reverse {
		// We've advanced to the next key but need to move back to the previous
		// key.
		return p.prevKey(key)
	}
	// We're already at the new key so there is nothing to do.
	return true
}

// Adds the specified value to the result set, excluding tombstones unless
// p.tombstones is true. Advances to the next key unless we've reached the max
// results limit.
func (p *pebbleMVCCScanner) addAndAdvance(val []byte) bool {
	// Don't include deleted versions len(val) == 0, unless we've been instructed
	// to include tombstones in the results.
	if len(val) > 0 || p.tombstones {
		p.results.put(p.curMVCCKey(), val)
		if p.results.count == p.maxKeys {
			return false
		}
	}
	return p.advanceKey()
}

// Seeks to the latest revision of the current key that's still less than or
// equal to the specified timestamp, adds it to the result set, then moves onto
// the next user key.
func (p *pebbleMVCCScanner) seekVersion(ts hlc.Timestamp, uncertaintyCheck bool) bool {
	key := MVCCKey{Key: p.curKey, Timestamp: ts}
	p.keyBuf = EncodeKeyToBuf(p.keyBuf[:0], key)
	origKey := p.keyBuf[:len(p.curKey)]

	for i := 0; i < p.itersBeforeSeek; i++ {
		if !p.iterNext() {
			return p.advanceKeyAtEnd()
		}
		if !bytes.Equal(p.curKey, origKey) {
			p.incrementItersBeforeSeek()
			return p.advanceKeyAtNewKey(origKey)
		}
		if !ts.Less(p.curTS) {
			p.incrementItersBeforeSeek()
			if uncertaintyCheck && p.ts.Less(p.curTS) {
				return p.uncertaintyError(p.curTS)
			}
			return p.addAndAdvance(p.curValue)
		}
	}

	p.decrementItersBeforeSeek()
	if !p.iterSeek(key) {
		return p.advanceKeyAtEnd()
	}
	if !bytes.Equal(p.curKey, origKey) {
		return p.advanceKeyAtNewKey(origKey)
	}
	if !ts.Less(p.curTS) {
		if uncertaintyCheck && p.ts.Less(p.curTS) {
			return p.uncertaintyError(p.curTS)
		}
		return p.addAndAdvance(p.curValue)
	}
	return p.advanceKey()
}

// Updates cur{RawKey, Key, TS} to match record the iterator is pointing to.
func (p *pebbleMVCCScanner) updateCurrent() bool {
	if !p.iterValid() {
		return false
	}

	p.curValue = p.parent.UnsafeValue()
	key := p.parent.UnsafeKey()
	p.curKey, p.curTS = key.Key, key.Timestamp
	return true
}

func (p *pebbleMVCCScanner) iterValid() bool {
	if valid, err := p.parent.Valid(); !valid {
		p.err = err
		return false
	}
	return true
}

// iterSeek seeks to the latest revision of the specified key (or a greater key).
func (p *pebbleMVCCScanner) iterSeek(key MVCCKey) bool {
	p.clearPeeked()
	p.parent.SeekGE(key)
	return p.updateCurrent()
}

// iterSeekReverse seeks to the latest revision of the key before the specified key.
func (p *pebbleMVCCScanner) iterSeekReverse(key MVCCKey) bool {
	p.clearPeeked()
	p.parent.SeekLT(key)
	if !p.updateCurrent() {
		// We have seeked to before the start key. Return.
		return false
	}

	if p.curTS == (hlc.Timestamp{}) {
		// We landed on an intent or inline value.
		return true
	}
	// We landed on a versioned value, we need to back up to find the
	// latest version.
	return p.backwardLatestVersion(p.curKey, 0)
}

// iterNext advances to the next MVCC key.
func (p *pebbleMVCCScanner) iterNext() bool {
	if p.reverse && p.peeked {
		// If we have peeked at the previous entry, we need to advance the iterator
		// twice.
		p.peeked = false
		if !p.iterValid() {
			// We were peeked off the beginning of iteration. Seek to the first
			// entry, and then advance one step.
			p.parent.SeekGE(MVCCKey{})
			if !p.iterValid() {
				return false
			}
			p.parent.Next()
			return p.updateCurrent()
		}
		p.parent.Next()
		if !p.iterValid() {
			return false
		}
	}
	p.parent.Next()
	return p.updateCurrent()
}

// iterPrev advances to the previous MVCC Key.
func (p *pebbleMVCCScanner) iterPrev() bool {
	if p.peeked {
		p.peeked = false
		return p.updateCurrent()
	}
	p.parent.Prev()
	return p.updateCurrent()
}

// Peek the previous key and store the result in peekedKey. Note that this
// moves the iterator backward, while leaving p.cur{key,value,rawKey} untouched
// and therefore out of sync. iterPrev and iterNext take this into account.
func (p *pebbleMVCCScanner) iterPeekPrev() ([]byte, bool) {
	if !p.peeked {
		p.peeked = true
		// We need to save a copy of the current iterator key and value and adjust
		// curRawKey, curKey and curValue to point to this saved data. We use a
		// single buffer for this purpose: savedBuf.
		p.savedBuf = append(p.savedBuf[:0], p.curKey...)
		p.savedBuf = append(p.savedBuf, p.curValue...)
		p.curKey = p.savedBuf[:len(p.curKey)]
		p.curValue = p.savedBuf[len(p.curKey):]

		// With the current iterator state saved we can move the iterator to the
		// previous entry.
		p.parent.Prev()
		if !p.iterValid() {
			// The iterator is now invalid, but note that this case is handled in
			// both iterNext and iterPrev. In the former case, we'll position the
			// iterator at the first entry, and in the latter iteration will be done.
			return nil, false
		}
	} else if !p.iterValid() {
		return nil, false
	}

	peekedKey := p.parent.UnsafeKey()
	return peekedKey.Key, true
}

// Clear the peeked flag. Call this before any iterator operations.
func (p *pebbleMVCCScanner) clearPeeked() {
	if p.reverse {
		p.peeked = false
	}
}

func (p *pebbleMVCCScanner) curMVCCKey() MVCCKey {
	return MVCCKey{Key: p.curKey, Timestamp: p.curTS}
}
