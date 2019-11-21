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
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/intentresolver"
	"github.com/google/btree"
)

// lockWaitQueueImpl implements the lockWaitQueue interface.
type lockWaitQueueImpl struct {
	ir *intentresolver.IntentResolver
	qs btree.BTree
}

type perKeyWaitQueue struct {
	key roachpb.Key
	// TODO(WIP): linked-list structure. Most of this would be pulled straight
	// from the existing contentionQueue, which could then go away, along with
	// its terrible hooks into Store.Send.
}

// Less implements the btree.Item interface.
func (a *perKeyWaitQueue) Less(b btree.Item) bool {
	return a.key.Compare(b.(*perKeyWaitQueue).key) < 0
}

func (wq *lockWaitQueueImpl) getFirstQueue(req Request) lockWaitQueueKey {
	return nil
}

func (wq *lockWaitQueueImpl) waitInQueue(req Request, key lockWaitQueueKey) (lockWaitQueueGuard, error) {
	panic("unimplemented")
}

func (wq *lockWaitQueueImpl) createAndWaitInQueue(req Request, intent roachpb.Intent) (lockWaitQueueGuard, error) {
	// TODO(WIP): this isn't actually the real implementation, but this works
	// for now by just never queuing and treating all requests as if they're
	// the the front of the queue. The queue is just an optimization to avoid
	// thundering herds after all.
	wiPErr := roachpb.NewError(&roachpb.WriteIntentError{
		Intents: []roachpb.Intent{intent},
	})
	pt := roachpb.PUSH_ABORT
	_, pErr := wq.ir.ProcessWriteIntentError(req.context(), wiPErr, nil, req.header(), pt)
	return nil, pErr.GoError()
}

func (wq *lockWaitQueueImpl) exitQueue(key lockWaitQueueGuard) {
	if key == nil {
		return
	}
	panic("unimplemented")
}
