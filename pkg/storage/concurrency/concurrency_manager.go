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
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/storage/engine/enginepb"
	"github.com/cockroachdb/cockroach/pkg/storage/intentresolver"
	"github.com/cockroachdb/cockroach/pkg/storage/spanlatch"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
)

// managerImpl implements the Manager interface.
type managerImpl struct {
	// Synchronizes conflicting in-flight requests.
	lm latchManager
	// Synchronizes conflicting in-progress transactions.
	lt lockTable
	// Ensures fair FIFO access to keys accessed by conflicting transactions.
	lwq lockWaitQueue
	// Waits for transaction completion and detects deadlocks.
	twq txnWaitQueue
}

// NewManager creates a new concurrency Manager structure.
func NewManager(
	stopper *stop.Stopper,
	r engine.Reader,
	ir *intentresolver.IntentResolver,
) Manager {
	return &managerImpl{
		lm: &latchManagerImpl{
			m: spanlatch.Make(stopper, nil),
		},
		lt: &lockTableImpl{
			r: r,
		},
		lwq: &lockWaitQueueImpl{
			ir: ir,
		},
	}
}

// SequenceRequest implements the Manager interface.
func (m *managerImpl) SequenceRequest(req Request) (rg RequestGuard, err error) {
	rg = RequestGuard{req: req}
	defer func() {
		if err != nil {
			m.FinishRequest(&rg)
		}
	}()

	for {
		rg.lg, err = m.lm.acquire(req)
		if err != nil {
			return rg, err
		}

		if queue := m.lwq.getFirstQueue(req); queue != nil {
			// TODO(WIP): if we actually wanted to do this right, we wouldn't
			// drop the latches until we are already in the lockWaitQueue.
			m.lm.release(rg.moveLatchGuard())

			wqg, err := m.lwq.waitInQueue(req, queue)
			if err != nil {
				return rg, err
			}
			rg.wqgs = append(rg.wqgs, wqg)
			continue
		}

		if lock, ok, err := m.lt.scan(req); err != nil {
			return rg, err
		} else if ok {
			m.lm.release(rg.moveLatchGuard())

			wqg, err := m.lwq.createAndWaitInQueue(req, lock)
			if err != nil {
				return rg, err
			}
			rg.wqgs = append(rg.wqgs, wqg)
			continue
		}

		return rg, nil
	}
}

// FinishRequest implements the Manager interface.
func (m *managerImpl) FinishRequest(rg *RequestGuard) {
	for _, wqg := range rg.moveWaitQueueGuards() {
		m.lwq.exitQueue(wqg)
	}
	if lg := rg.moveLatchGuard(); lg != nil {
		m.lm.release(lg)
	}
}

// NewLockAwareReadWriter implements the Manager interface.
func (m *managerImpl) NewLockAwareReadWriter(
	rw engine.ReadWriter, txn *roachpb.Transaction,
) engine.ReadWriter {
	return m.lt.newLockAwareReadWriter(rw, txn)
}

// NewLockAwareBatch implements the Manager interface.
func (m *managerImpl) NewLockAwareBatch(
	rw engine.Batch, txn *roachpb.Transaction,
) engine.Batch {
	return m.lt.newLockAwareBatch(rw, txn)
}

// RemoveLock implements the Manager interface.
func (m *managerImpl) RemoveLock(
	ctx context.Context,
	eng engine.ReadWriter,
	ms *enginepb.MVCCStats,
	intent roachpb.Intent,
	fn LockUpdatedCallback,
) error {
	return m.lt.removeOne(ctx, eng, ms, intent, fn)
}

// RemoveLocks implements the Manager interface.
func (m *managerImpl) RemoveLocks(
	ctx context.Context,
	eng engine.ReadWriter,
	ms *enginepb.MVCCStats,
	intent roachpb.Intent,
	max int64,
	fn LockUpdatedCallback,
) (int64, *roachpb.Span, error) {
	return m.lt.removeMany(ctx, eng, ms, intent, max, fn)
}

// SetDescriptor implements the Manager interface.
func (m *managerImpl) SetDescriptor(desc *roachpb.RangeDescriptor) {
	m.lt.setBounds(desc.StartKey, desc.EndKey)
}
