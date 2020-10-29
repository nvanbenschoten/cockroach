// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package minprop

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

// SingleRangeTracker is a tracker scoped to a single range.
type SingleRangeTracker struct {
	mu struct {
		syncutil.Mutex
		// closed is the most recently closed timestamp.
		closed hlc.Timestamp

		// The variables below track required information for the next closed
		// timestamp and beyond. First, `next` is the timestamp that will be
		// closed out next (i.e. will replace `closed`).
		//
		// "left" and "right" refers to how the timestamps at which the
		// associated command evaluations take place relate to `next`.
		// `left`-tracked proposals are taken into account for the next closed
		// timestamp, i.e. they could mutate at timestamps <= `next`. `right`
		// proposals affect only MVCC timestamps > `next` and thus will become
		// relevant only after `next` has been closed out, at which point the
		// "right" set will replace the "left".
		//
		//    closed           next
		//      |          left | right
		//      |               |
		//      |               |
		//      v               v
		//---------------------------------------------------------> time
		//
		// A replica wishing to serve a follower read will first have to catch
		// up to a lease applied index that is guaranteed to include all writes
		// affecting the closed timestamp or below. When `next` is closed out,
		// the set of relevant Lease Applied Indexes will be stored in `leftMLAI`.
		//
		// This is augmented by reference counts for the proposals currently in
		// the process of evaluating. `next` can only be closed out once
		// `leftRef` has been drained (i.e. has dropped to zero); new proposals
		// are always forced above `next` and consequently count towards
		// `rightRef`.
		//
		// Epochs track the highest liveness epoch observed for any released
		// proposals. Tracking a max epoch allows the MPT to provide some MLAI
		// information about the current epoch when calls to Close straddle multiple
		// different epochs. Before epoch tracking was added the client of the MPT
		// was forced to assume that the MLAI information from the current call to
		// Close corresponded to the highest known epoch as of the previous call to
		// Close. This is problematic in cases where an epoch change leads to a
		// lease change for an otherwise quiescent range. If this mechanism were
		// not in place then the client would never learn about an MLAI for the
		// current epoch. Clients provide their view of the current epoch to calls
		// to Close which use this information to determine whether the current
		// state should be moved and whether the caller can make use of the
		// currently tracked data. Each side tracks data which corresponds exactly
		// to the side's epoch value. Releasing a proposal into the tracker at a
		// later epoch than is currently tracked will result in the current data
		// corresponding to the prior epoch to be evicted.

		next              hlc.Timestamp
		leftRef, rightRef int
	}
}

// Track is called before evaluating a proposal. It returns the minimum
// timestamp at which the proposal can be evaluated (i.e. the request timestamp
// needs to be forwarded if necessary), and acquires a reference with the
// Tracker. This reference is released by calling the returned closure either
// a) before proposing the command, supplying the Lease Applied Index at which
//    the proposal will be carried out, or
// b) with zero arguments if the command won't end up being proposed (i.e. hit
//    an error during evaluation).
//
// The ReleaseFunc is not thread safe. For convenience, it may be called with
// zero arguments once after a regular call.
func (t *SingleRangeTracker) Track(ctx context.Context) (hlc.Timestamp, func(ctx context.Context)) {
	shouldLog := log.V(3)

	t.mu.Lock()
	minProp := t.mu.next.Next()
	t.mu.rightRef++
	t.mu.Unlock()

	if shouldLog {
		log.Infof(ctx, "track: proposal on the right at minProp %s", minProp)
	}

	var calls int
	release := func(ctx context.Context) {
		calls++
		t.release(ctx, minProp, shouldLog)
	}

	return minProp, release
}

func (t *SingleRangeTracker) release(
	ctx context.Context,
	minProp hlc.Timestamp,
	shouldLog bool,
) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if minProp == t.mu.closed.Next() {
		t.mu.leftRef--
		if t.mu.leftRef == 0 {
			// TODO(nvanbenschoten): do this.
		}
	} else if minProp == t.mu.next.Next() {
		t.mu.rightRef--
	} else {
		log.Fatalf(ctx, "min proposal %s not tracked under closed (%s) or next (%s) timestamp", minProp, t.mu.closed, t.mu.next)
	}
}

func (t *SingleRangeTracker) Closed() hlc.Timestamp {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.mu.closed
}
