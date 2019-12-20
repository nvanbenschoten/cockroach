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

	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/intentresolver"
	"github.com/cockroachdb/cockroach/pkg/storage/spanlatch"
	"github.com/cockroachdb/cockroach/pkg/storage/storagebase"
	"github.com/cockroachdb/cockroach/pkg/storage/txnwait"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
)

// managerImpl implements the Manager interface.
type managerImpl struct {
	// Synchronizes conflicting in-flight requests.
	lm latchManager
	// Synchronizes conflicting in-progress transactions.
	lt lockTable
	// Ensures fair FIFO access to keys accessed by conflicting transactions.
	lwq lockWaitQueueWaiter
	// Waits for transaction completion and detects deadlocks.
	twq txnWaitQueue

	store     StoreInterface
	rangeDesc *roachpb.RangeDescriptor
}

// StoreInterface provides some parts of a Store without incurring a dependency.
type StoreInterface interface {
	// Components.
	Clock() *hlc.Clock
	Stopper() *stop.Stopper
	DB() *client.DB
	IntentResolver() *intentresolver.IntentResolver
	// Knobs.
	GetTxnWaitKnobs() txnwait.TestingKnobs
	// Metrics.
	GetTxnWaitMetrics() *txnwait.Metrics
	GetSlowLatchGauge() *metric.Gauge
}

// NewManager creates a new concurrency Manager structure.
func NewManager(store StoreInterface) Manager {
	m := new(managerImpl)
	// TODO(nvanbenschoten): move pkg/storage/spanlatch to a new
	// pkg/storage/concurrency/latch package. Make it implement
	// the latchManager interface directly, if possible.
	m.lm = &latchManagerImpl{
		m: spanlatch.Make(store.Stopper(), store.GetSlowLatchGauge()),
	}
	m.lt = &lockTableImpl{}
	m.lwq = &lockWaitQueueWaiterImpl{
		c:  store.Clock(),
		ir: store.IntentResolver(),
	}
	// TODO(nvanbenschoten): move pkg/storage/txnwait to a new
	// pkg/storage/concurrency/txnwait package.
	m.twq = txnwait.NewQueue(store, m)
	m.store = store
	return m
}

// SequenceReq implements the Manager interface.
func (m *managerImpl) SequenceReq(
	ctx context.Context, prev *Guard, req Request,
) (g *Guard, err *Error) {
	if !shouldAcquireLatches(req) {
		return nil, nil
	}

	// Ensure that we release the guard if we return an error.
	defer func() {
		if g != nil && err != nil {
			m.FinishReq(g)
			g = nil
		}
	}()

	// Provide the manager with an opportunity to intercept the request.
	if err := m.maybeInterceptReq(ctx, req); err != nil {
		return nil, err
	}

	g = createOrReuseGuard(prev, req)
	for {
		// Acquire latches for the request. This synchronizes the request
		// with all conflicting in-flight requests.
		log.Event(ctx, "acquire latches")
		g.lg, err = m.lm.Acquire(ctx, req)
		if err != nil {
			return g, err
		}

		// Some requests don't want the wait on locks.
		if !shouldWaitOnConflicts(req) {
			return g, nil
		}

		// Scan for conflicting locks.
		log.Event(ctx, "scan for conflicting locks")
		if queues := m.lt.scanAndEnqueue(g.req); len(queues) > 0 {
			m.lm.Release(g.moveLatchGuard())

			// Wait on each of the conflicting locks.
			g.wqgs = append(g.wqgs, queues...)
			log.Event(ctx, "waiting in lock wait-queues")
			for _, wqg := range queues {
				if err := m.lwq.waitOn(ctx, g.req, wqg); err != nil {
					return g, err
				}
			}
			continue
		}
		return g, nil
	}
}

// maybeInterceptReq allows the concurrency manager to intercept requests before
// sequencing and evaluation so that it can immediately act on them. This allows
// the concurrency manager to route certain concurrency control-related requests
// into queues and update its optionally internal state based on the requests.
func (m *managerImpl) maybeInterceptReq(ctx context.Context, req Request) *Error {
	switch {
	case req.isSingle(roachpb.PushTxn):
		// If necessary, wait in the txnWaitQueue for the pushee transaction
		// to expire or to move to a finalized state.
		t := req.Requests[0].GetPushTxn()
		return m.twq.MaybeWaitForPush(ctx, t)
	case req.isSingle(roachpb.QueryTxn):
		// If necessary, wait in the txnWaitQueue either for transaction
		// update or for dependent transactions to change.
		t := req.Requests[0].GetQueryTxn()
		return m.twq.MaybeWaitForQuery(ctx, t)
	default:
		// TODO(nvanbenschoten): in the future, use this hook to update the
		// lock table to allow contending transactions to proceed.
		// for _, arg := range req.Requests {
		// 	switch t := arg.GetInner().(type) {
		// 	case *roachpb.ResolveIntentRequest:
		// 		_ = t
		// 	case *roachpb.ResolveIntentRangeRequest:
		// 		_ = t
		// 	}
		// }
	}
	return nil
}

// shouldAcquireLatches determines whether the request should acquire latches
// before proceeding to evaluate. Latches are used to synchronize with other
// conflicting requests, based on the Spans collected for the request. Most
// request types will want to acquire latches.
func shouldAcquireLatches(req Request) bool {
	switch {
	case req.ReadConsistency != roachpb.CONSISTENT:
		// Only acquire latches for consistent operations.
		return false
	case req.isSingle(roachpb.RequestLease):
		// Don't acquire latches for lease requests. These are run on replicas
		// that do not hold the lease, so acquiring latches wouldn't help
		// synchronize with other requests.
		return false
	}
	return true
}

// shouldWaitOnConflicts determines whether the request should wait on locks and
// wait-queues owned by other transactions before proceeding to evaluate. Most
// requests will want to wait on conflicting transactions to ensure that they
// are sufficiently isolated during their evaluation, but some "isolation aware"
// requests want to proceed to evaluation even in the presence of conflicts
// because they know how to handle them.
func shouldWaitOnConflicts(req Request) bool {
	// TODO what's the best way to define this. There are request types that do
	// want to wait of locks like (PutRequest and ScanRequest) and then there
	// are those that don't want to wait on locks like (QueryIntentRequest,
	// RefreshRequest, and ResolveIntentRequest). Should we define a flag for
	// this?
	return true
}

// FinishReq implements the Manager interface.
func (m *managerImpl) FinishReq(g *Guard) {
	for _, wqg := range g.moveWaitQueueGuards() {
		m.lt.dequeue(wqg)
	}
	if lg := g.moveLatchGuard(); lg != nil {
		m.lm.Release(lg)
	}
}

// HandleWriterIntentError implements the Manager interface.
func (m *managerImpl) HandleWriterIntentError(
	ctx context.Context, g *Guard, t *roachpb.WriteIntentError,
) (*Guard, *Error) {
	// Enter or create a txnWaitQueue entry per intent.
	orig := len(g.wqgs)
	for _, intent := range t.Intents {
		g.wqgs = append(g.wqgs, m.lt.addDiscoveredLock(g.req, intent))
	}
	m.lm.Release(g.moveLatchGuard())

	// Wait on each new txnWaitQueue entry.
	// TODO: should we do this here?
	for _, wqg := range g.wqgs[orig:] {
		if err := m.lwq.waitOn(ctx, g.req, wqg); err != nil {
			m.FinishReq(g)
			return nil, err
		}
	}

	// Allow the request to retry while holding on to its lockWaitQueueGuard.
	return g, nil
}

// HandleTransactionPushError implements the Manager interface.
func (m *managerImpl) HandleTransactionPushError(
	ctx context.Context, g *Guard, t *roachpb.TransactionPushError,
) *Guard {
	m.twq.EnqueueTxn(&t.PusheeTxn)
	m.lm.Release(g.moveLatchGuard())

	// Allow the request to retry while holding on to its lockWaitQueueGuard.
	return g
}

// OnTransactionUpdated implements the transactionManager interface.
func (m *managerImpl) OnTransactionUpdated(ctx context.Context, txn *roachpb.Transaction) {
	m.twq.UpdateTxn(ctx, txn)
}

// GetDependents implements the transactionManager interface.
func (m *managerImpl) GetDependents(txnID uuid.UUID) []uuid.UUID {
	return m.twq.GetDependents(txnID)
}

// OnDescriptorUpdated implements the replicaStateContainer interface.
func (m *managerImpl) OnDescriptorUpdated(desc *roachpb.RangeDescriptor) {
	m.rangeDesc = desc
	m.lt.setBounds(desc.StartKey, desc.EndKey)
}

// OnLeaseUpdated implements the replicaStateContainer interface.
func (m *managerImpl) OnLeaseUpdated(iAmTheLeaseHolder bool) {
	if iAmTheLeaseHolder {
		m.twq.Enable()
	} else {
		m.twq.Clear(true /* disable */)
	}
}

// OnSplit implements the replicaStateContainer interface.
func (m *managerImpl) OnSplit() {
	m.twq.Clear(false /* disable */)
}

// OnMerge implements the replicaStateContainer interface.
func (m *managerImpl) OnMerge() {
	m.twq.Clear(true /* disable */)
}

// ContainsKey implements the txnwait.ReplicaInterface interface.
func (m *managerImpl) ContainsKey(key roachpb.Key) bool {
	return storagebase.ContainsKey(m.rangeDesc, key)
}

func (r *Request) isSingle(m roachpb.Method) bool {
	if len(r.Requests) != 1 {
		return false
	}
	return r.Requests[0].GetInner().Method() == m
}

func createOrReuseGuard(g *Guard, req Request) *Guard {
	if g == nil {
		g = &Guard{req: req}
	} else {
		g.assertNoLatches()
	}
	return g
}

func (g *Guard) assertNoLatches() {
	if g.lg != nil {
		panic("unexpected latches held")
	}
}

func (g *Guard) moveLatchGuard() latchGuard {
	lg := g.lg
	g.lg = nil
	return lg
}

func (g *Guard) moveWaitQueueGuards() []lockWaitQueueGuard {
	wqgs := g.wqgs
	g.wqgs = nil
	return wqgs
}
