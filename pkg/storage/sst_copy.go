// Copyright 2021 The Cockroach Authors.
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
	"github.com/cockroachdb/cockroach/pkg/storage/fs"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/sstable"
)

// CopySST copies the values from the provided source sst file into the
// destination writer. Range deletion tombstones are copied over first,
// followed by the point Set and Delete values. Callers should provide a
// batch that they can later commit if they desire atomicity across all
// operations in the sst.
func CopySST(dest Writer, src fs.File) error {
	rf, ok := src.(sstable.ReadableFile)
	if !ok {
		return errors.Errorf("%T is not a ReadableFile", src)
	}
	sst, err := sstable.NewReader(rf, sstable.ReaderOptions{
		Comparer: EngineComparer,
	})
	if err != nil {
		return err
	}
	// Copy range deletion tombstones first, as they should not shadow the
	// values in the SST.
	err = copyRangeDels(dest, sst)
	if err != nil {
		return err
	}
	// Then copy the values.
	return copyVals(dest, sst)
}

func copyRangeDels(dest Writer, sst *sstable.Reader) error {
	iter, err := sst.NewRawRangeDelIter()
	if iter == nil || err != nil {
		return err
	}
	defer iter.Close()
	for iKey, value := iter.First(); iKey != nil; iKey, value = iter.Next() {
		start, ok := DecodeEngineKey(iKey.UserKey)
		if !ok {
			return errors.Errorf("invalid encoded engine key: %x", iKey.UserKey)
		}
		end, ok := DecodeEngineKey(value)
		if !ok {
			return errors.Errorf("invalid encoded engine key: %x", value)
		}
		if k := iKey.Kind(); k != pebble.InternalKeyKindRangeDelete {
			return errors.Errorf("expected RangeDelete, found %v", k)
		}
		// TODO DURING REVIEW: Are we losing the Version here? Do we need a
		// ClearEngineKeyRange? The Version isn't actually set in the cases that
		// we care about. Should we just assert that it's empty?
		// TODO DURING REVIEW: We are using EngineKey throughout this file, but
		// EngineKey was added in 21.1. How are we going to backport this?
		if err := dest.ClearRawRange(start.Key, end.Key); err != nil {
			return err
		}
	}
	return iter.Error()
}

func copyVals(dest Writer, sst *sstable.Reader) error {
	iter, err := sst.NewIter(nil /* lower */, nil /* upper */)
	if err != nil {
		return err
	}
	defer iter.Close()
	for iKey, value := iter.First(); iKey != nil; iKey, value = iter.Next() {
		key, ok := DecodeEngineKey(iKey.UserKey)
		if !ok {
			return errors.Errorf("invalid encoded engine key: %x", iKey.UserKey)
		}
		switch iKey.Kind() {
		case pebble.InternalKeyKindSet:
			err = dest.PutEngineKey(key, value)
		case pebble.InternalKeyKindDelete:
			err = dest.ClearEngineKey(key)
		default:
			return errors.Errorf("unexpected key kind found: %v", iKey.Kind())
		}
		if err != nil {
			return err
		}
	}
	return iter.Error()
}
