// Copyright 2019 The Cockroach Authors.
//
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with the Business
// Source License, use of this software will be governed by the Apache License,
// Version 2.0, included in the file licenses/APL.txt.

#include "incremental_iterator.h"
#include "comparator.h"
#include "encoding.h"
#include "protos/roachpb/errors.pb.h"

using namespace cockroach;

DBIncrementalIterator::DBIncrementalIterator(DBEngine* engine, DBIterOptions opts, DBKey start,
                                             DBKey end)
    : engine(engine),
      opts(opts),
      valid(true),
      status(kSuccess),
      start(start),
      end(end) {

  start_time.set_wall_time(start.wall_time);
  start_time.set_logical(start.logical);
  end_time.set_wall_time(end.wall_time);
  end_time.set_logical(end.logical);

  iter.reset(DBNewIter(engine, opts));
}

DBIncrementalIterator::~DBIncrementalIterator() {}

// legacyTimestampIsLess compares the timestamps t1 and t2, and returns a
// boolean indicating whether t1 is less than t2.
bool DBIncrementalIterator::legacyTimestampIsLess(const cockroach::util::hlc::LegacyTimestamp& t1,
                                                  const cockroach::util::hlc::LegacyTimestamp& t2) {
  return t1.wall_time() < t2.wall_time() ||
         (t1.wall_time() == t2.wall_time() && t1.logical() < t2.logical());
}

// advanceKey finds the key and its appropriate version which lies in
// (start_time, end_time].
void DBIncrementalIterator::advanceKey() {
  for (;;) {
    if (!valid) {
      return;
    }

    if (!iter.get()->rep->Valid()) {
      status = ToDBStatus(iter.get()->rep->status());
      valid = false;
      return;
    }

    rocksdb::Slice key;
    int64_t wall_time = 0;
    int32_t logical = 0;
    if (!DecodeKey(iter.get()->rep->key(), &key, &wall_time, &logical)) {
      status = ToDBString("unable to decode key");
      valid = false;
      return;
    }

    cockroach::storage::engine::enginepb::MVCCMetadata meta;
    if (wall_time != 0 || logical != 0) {
      meta.mutable_timestamp()->set_wall_time(wall_time);
      meta.mutable_timestamp()->set_logical(logical);
    } else {
      valid = false;
      status = ToDBString("Inline values are unsupported by the IncrementalIterator");
      return;
    }

    if (legacyTimestampIsLess(end_time, meta.timestamp())) {
      DBIterNext(iter.get(), false);
      continue;
    } else if (!legacyTimestampIsLess(start_time, meta.timestamp())) {
      DBIterNext(iter.get(), true);
      continue;
    }

    break;
  }
}

DBIterState DBIncrementalIterator::getState() {
  DBIterState state = {};
  state.valid = valid;
  state.status = status;

  if (state.valid) {
    rocksdb::Slice key;
    state.valid = DecodeKey(iter.get()->rep->key(), &key, &state.key.wall_time, &state.key.logical);
    if (state.valid) {
      state.key.key = ToDBSlice(key);
      state.value = ToDBSlice(iter.get()->rep->value());
    }
  }

  return state;
}

DBIterState DBIncrementalIterator::seek(DBKey key) {
  DBIterSeek(iter.get(), key);
  advanceKey();
  return getState();
}

DBIterState DBIncrementalIterator::next(bool skip_current_key_versions) {
  DBIterNext(iter.get(), skip_current_key_versions);
  advanceKey();
  return getState();
}

const rocksdb::Slice DBIncrementalIterator::key() { return iter.get()->rep->key(); }

const rocksdb::Slice DBIncrementalIterator::value() { return iter.get()->rep->value(); }
