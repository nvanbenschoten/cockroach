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
	"github.com/cockroachdb/cockroach/pkg/storage/intentresolver"
	"github.com/cockroachdb/cockroach/pkg/storage/spanset"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/google/btree"
)

// lockTableImpl implements the lockTable interface.
type lockTableImpl struct {
	qs btree.BTree
}

type perKeyWaitQueue struct {
	key  roachpb.Key
	in   roachpb.Intent
	req  Request
	done intentresolver.CleanupFunc
	// TODO(WIP): linked-list structure. Most of this would be pulled straight
	// from the existing contentionQueue, which could then go away, along with
	// its terrible hooks into Replica.executeBatchWithConcurrencyRetries.
}

// Less implements the btree.Item interface.
func (a *perKeyWaitQueue) Less(b btree.Item) bool {
	return a.key.Compare(b.(*perKeyWaitQueue).key) < 0
}

func (lt *lockTableImpl) acquireLock() {
	// TODO
}

func (lt *lockTableImpl) releaseLock() {
	// TODO
}

func (lt *lockTableImpl) addDiscoveredLock(req Request, in roachpb.Intent) lockWaitQueueGuard {
	return perKeyWaitQueue{in.Key, in, req, nil}
}

func (lt *lockTableImpl) scanAndEnqueue(req Request) []lockWaitQueueGuard {
	// TODO(nvanbenschoten/sumeer): actually implement this.
	return nil
}

func (lt *lockTableImpl) dequeue(wqg lockWaitQueueGuard) {
	pkwq := wqg.(perKeyWaitQueue)
	if pkwq.done != nil {
		pkwq.done(nil, nil)
	}
}

func (lt *lockTableImpl) setBounds(start, end roachpb.RKey) {
	// TODO
}

// lockWaitQueueWaiterImpl implements the lockWaitQueueWaiter interface.
type lockWaitQueueWaiterImpl struct {
	c  *hlc.Clock
	ir *intentresolver.IntentResolver
}

func (wq *lockWaitQueueWaiterImpl) waitOn(
	ctx context.Context, req Request, wqg lockWaitQueueGuard,
) *Error {
	pkwq := wqg.(perKeyWaitQueue)

	// WIP: Just to get it working.
	wiErr := roachpb.NewError(&roachpb.WriteIntentError{Intents: []roachpb.Intent{pkwq.in}})

	h := roachpb.Header{
		Timestamp:    req.Timestamp,
		UserPriority: req.Priority,
	}
	if req.Txn != nil {
		// We must push at least to req.Timestamp, but in fact we want to
		// go all the way up to a timestamp which was taken off the HLC
		// after our operation started. This allows us to not have to
		// restart for uncertainty as we come back and read.
		h.Timestamp = wq.c.Now()
		// We are going to hand the header (and thus the transaction proto)
		// to the RPC framework, after which it must not be changed (since
		// that could race). Since the subsequent execution of the original
		// request might mutate the transaction, make a copy here.
		//
		// See #9130.
		h.Txn = req.Txn.Clone()
	}

	var pushType roachpb.PushTxnType
	if req.Spans.Contains(spanset.SpanReadWrite) {
		pushType = roachpb.PUSH_ABORT
	} else {
		pushType = roachpb.PUSH_TIMESTAMP
	}

	var err *Error
	if pkwq.done, err = wq.ir.ProcessWriteIntentError(ctx, wiErr, h, pushType); err != nil {
		// Do not propagate ambiguous results; assume success and retry original op.
		if _, ok := err.GetDetail().(*roachpb.AmbiguousResultError); !ok {
			return err
		}
	}
	return nil
}
