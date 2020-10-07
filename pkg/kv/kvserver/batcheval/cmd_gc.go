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

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/batcheval/result"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/spanset"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/errors"
)

func init() {
	RegisterReadWriteCommand(roachpb.GC, declareKeysGC, GC)
}

func declareKeysGC(
	desc *roachpb.RangeDescriptor,
	header roachpb.Header,
	req roachpb.Request,
	latchSpans, _ *spanset.SpanSet,
) {
	// Grab exclusive access to the RangeLastGCKey. We may be writing to the key
	// if args.Threshold is set. Even if not, we use this latch to serialize all
	// GC requests on a Range to ensure that their stats updates are accurate.
	latchSpans.AddNonMVCC(spanset.SpanReadWrite, roachpb.Span{Key: keys.RangeLastGCKey(header.RangeID)})
	// Needed for Range bounds checks in calls to EvalContext.ContainsKey.
	latchSpans.AddNonMVCC(spanset.SpanReadOnly, roachpb.Span{Key: keys.RangeDescriptorKey(desc.StartKey)})
}

// GC iterates through the list of keys to garbage collect specified in the
// arguments. MVCCGarbageCollect is invoked on each listed key along with the
// expiration timestamp. GC request is also used to update the RangeLastGCKey,
// which denotes the largest time at which MVCC versions may have been garbage
// collected.
//
// Latchless Garbage-Collection:
//
// Garbage collection needs to synchronize with concurrent read and write
// requests to ensure that they do not return corrupt results after observing
// post-collected data. For instance, consider a key "A" with two versions:
//
//  "a":20 -> 'newer version'
//  "a":10 -> 'older version'
//
// Garbage collection will be able to remove the version at time 10 after the GC
// threshold reaches 20. If a read were to concurrently read key "A" at time 15,
// we'd want it to either return 'older version' or throw an error. Either is
// acceptable. What is not acceptable is for the read to return successfully
// with no value, having not noticed that garbage collection removed the version
// and erased part of history.
//
// So to ensure that MVCC garbage collection does not remove a version of a key
// while a request is acessing it, the two operations must synchronize. Garbage
// collection does not acquire latches or other forms of in-memory locks to
// synchronize with concurrent read and write requests. Instead, it uses a
// standard lock-free approach to coordinate with requests.
//
//  The steps GC requests take are:
//  1. bump the (in-memory) GC threshold
//  2. remove old versions of keys
//
//  The steps read and write requests take are:
//  1. check the (in-memory) GC threshold before evaluating
//  2. evaluate
//  3. check the (in-memory) GC threshold again after evaluating
//
// By bumping the GC threshold before beginning to remove older versions of
// keys, GC requests ensure that any actor who observes the effects of the GC on
// MVCC will also observe the effects of the GC on the GC threshold [1]. By
// checking the GC threshold twice, read and write requests ensure that even if
// they ran concurrently with a GC request, they will detect this before
// returning missing data.
//
// [1] NOTE: if removing old versions of keys and bumping the in-memory GC
//     threshold could be performed atomically, these two steps could be
//     performed together. Since they are not (the in-memory update is a
//     post-application side-effect), they must be separate steps.
//
func GC(
	ctx context.Context, readWriter storage.ReadWriter, cArgs CommandArgs, resp roachpb.Response,
) (result.Result, error) {
	args := cArgs.Args.(*roachpb.GCRequest)
	h := cArgs.Header

	// The in-memory GC threshold must already be above the timestamp of the
	// versions we want to remove here. See the NOTE above for an explanation
	// of why the GC threshold must be bumped first.
	oldThreshold := cArgs.EvalCtx.GetGCThreshold()

	// All keys must be inside the current replica range. Keys outside
	// of this range in the GC request are dropped silently, which is
	// safe because they can simply be re-collected later on the correct
	// replica. Discrepancies here can arise from race conditions during
	// range splitting.
	keys := make([]roachpb.GCRequest_GCKey, 0, len(args.Keys))
	for _, k := range args.Keys {
		if cArgs.EvalCtx.ContainsKey(k.Key) {
			if oldThreshold.Less(k.Timestamp) {
				return result.Result{}, errors.Errorf(
					"GCRequest_GCKey timestamp %s above current GC threshold %s",
					k.Timestamp, oldThreshold,
				)
			}
			keys = append(keys, k)
		}
	}

	// Garbage collect the specified keys by expiration timestamps.
	if err := storage.MVCCGarbageCollect(
		ctx, readWriter, cArgs.Stats, keys, h.Timestamp,
	); err != nil {
		return result.Result{}, err
	}

	// Optionally bump the GC threshold timestamp.
	var res result.Result
	if !args.Threshold.IsEmpty() {
		// Protect against multiple GC requests arriving out of order; we track
		// the maximum timestamp by forwarding the existing timestamp.
		newThreshold := oldThreshold
		updated := newThreshold.Forward(args.Threshold)

		// Don't write the GC threshold key unless we have to. We also don't
		// declare the key unless we have to (to allow the GC queue to batch
		// requests more efficiently), and we must honor what we declare.
		if updated {
			if err := MakeStateLoader(cArgs.EvalCtx).SetGCThreshold(
				ctx, readWriter, cArgs.Stats, &newThreshold,
			); err != nil {
				return result.Result{}, err
			}

			res.Replicated.State = &kvserverpb.ReplicaState{
				GCThreshold: &newThreshold,
			}
		}
	}

	return res, nil
}
