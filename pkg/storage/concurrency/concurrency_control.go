// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Package concurrency provides a concurrency manager structure that
// encapsulates the details of concurrency control and contention handling for
// serializable key-value transactions.
package concurrency

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/storage/engine/enginepb"
	"github.com/cockroachdb/cockroach/pkg/storage/spanset"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
)

// Manager is a structure that sequences incoming requests and provides
// isolation between requests that intend to perform conflicting operations.
// During sequencing, conflicts are discovered and any found are resolved
// through a combination of passive queuing and active pushing. Once a request
// has been sequenced, it is free to evaluate without concerns of conflicting
// with other in-flight requests due to the isolation provided by the manager.
// This isolation is guaranteed for the lifetime of the request but terminates
// once the request completes.
//
// Transactions require isolation both within requests and across requests. The
// manager accommodates this by allowing transactional requests to acquire
// locks, which outlive the requests themselves. Locks extend the duration of
// the isolation provided over specific keys to the lifetime of the lock-holder
// transaction itself. They are (typically) only released when the transaction
// commits or aborts. Other requests that find these locks while being sequenced
// wait on them to be released in a queue before proceeding. Because locks are
// checked during sequencing, requests are guaranteed access to all declared
// keys after they have been sequenced. In other words, locks don't need to be
// checked again during evaluation.
//
// Fairness is ensured between requests. If any two requests conflict then the
// request that arrived first will be sequenced first. As such, sequencing
// guarantees FIFO semantics. The one exception to this is that a request that
// is part of a transaction which has already acquired a lock does not need to
// wait on that lock during sequencing, and can therefore ignore any queue that
// has formed on the lock.
//
// Internal Components
//
// The concurrency manager is composed of a number of internal synchronization,
// bookkeeping, and queueing structures. Each of these is discussed in more
// detail on their interface definition. The following diagram details how the
// components are tied together:
//
//  +---------------------+-------------------------------------------------+
//  | concurrency.Manager |                                                 |
//  +---------------------+                                                 |
//  |                                                                       |
//  |------------+  acquire   +--------------+        acquire               |
//    Sequence() |--->--->--->| latchManager |<---<---<---<---<---<---<--+  |
//  |------------+            +--------------+                           |  |
//  |                           / check locks + wait queues              |  |
//  |                          v  if conflict, drop latches & wait       ^  |
//  |             +---------------------------------------------------+  |  |
//  |             | [ lockTable ]                                     |  |  |
//  |             | [    key1   ]    -------------+-----------------+ |  ^  |
//  |             | [    key2   ]  /  MVCCLock:   | lockWaitQueue:  | |  |  |
//  |             | [    key3   ]-{   - lock type | +-[a]<-[b]<-[c] | |  |  |
//  |             | [    key4   ]  \  - txn  meta | |  (no latches) |----^  |
//  |             | [    key5   ]    -------------+-|---------------+ |     |
//  |             | [    ...    ]                   v                 |     |
//  |             +---------------------------------|-----------------+     |
//  |                     |                         |                       |
//  |                     |       +- may be remote -+--+                    |
//  |                     |       |                    |                    |
//  |                     |       v                    ^                    |
//  |                     |    +--------------------------+                 |
//  |                     |    | txnWaitQueue:            |                 |
//  |                     |    | (located on txn record's |                 |
//  |                     |    |  leaseholder replica)    |                 |
//  |                     |    |--------------------------|                 |
//  |                     |    | [txn1] [txn2] [txn3] ... |                 |
//  |                     |    +--------------------------+                 |
//  |                     |                                                 |
//  |                     +--> hold latches ---> remain at head of queues -----> evaluate ...
//  |                                                                       |
//  |----------+                                                            |
//    Finish() | ---> exit wait queues ---> drop latches ----------------------> respond  ...
//  |----------+                                                            |
//  +-----------------------------------------------------------------------+
//
// At a high-level, requests enter the concurrency manager and immediately
// acquire latches from the latchManager to serialize access to the keys that
// they intend to touch. This latching takes into account the keys being
// accessed, the MVCC timestamp of accesses, and the access method being used
// (read vs. write) to allow for concurrency where possible. This has the effect
// of queuing on conflicting in-flight operations until their completion.
//
// Once latched, the request consults the lockTable to check for any conflicting
// locks owned by other transactions. If any are found, the request enters the
// corresponding lockWaitQueue and its latches are dropped. The head of the
// lockWaitQueue pushes the owner of the lock through a remote RPC that ends up
// in the pushee's txnWaitQueue. This queue exists on the leaseholder replica of
// the range that contains the pushee's transaction record. Other entries in the
// queue wait for the head of the queue, eventually pushing it to detect
// deadlocks. Once the lock is cleared, the head of the queue reacquires latches
// and attempts to proceed while remains at the head of that lockWaitQueue to
// ensure fairness.
//
// Once a request is latched and observes no conflicting locks in the lockTable
// and no conflicting lockWaitQueues that it is not already the head of, the
// request can proceed to evaluate. During evaluation, the request may insert or
// remove locks from the lockTable for its own transaction. This is performed
// transparently by a lockAwareBatch/lockAwareReadWriter. The request may also
// need to consult its own locks to properly interpret the corresponding intents
// in the MVCC keyspace. This is performed transparently by a lockAwareIter.
//
// When the request completes, it exits any lockWaitQueues that it was at the
// head of and releases its latches. However, any locks that it inserted into
// the lockTable remain.
type Manager interface {
	// SequenceRequest acquires latches, checks for locks, and queues and/or
	// pushes to resolve any conflicts. Once sequenced, the request is
	// guaranteed sufficient isolation for the duration of evaluation.
	SequenceRequest(Request) (RequestGuard, error)
	// FinishRequest marks the request as complete, releasing any protection
	// the request had against conflicting requests.
	FinishRequest(*RequestGuard)

	// TODO(nvanbenschoten): The following four methods should be moved to
	// RequestGuard. This would allow us to use information from the initial
	// lockTable scan to optimize a number of common cases. For instance, a
	// transaction without any local locks doesn't need a lockAwareIter.

	// NewLockAwareReadWriter creates a ReadWriter that is bound to the provided
	// transaction, inserts locks on the transaction's behalf when it writes,
	// and is aware of that the transaction's locks so that it can ensure that
	// it always reads-its-writes.
	NewLockAwareReadWriter(engine.ReadWriter, *roachpb.Transaction) engine.ReadWriter
	// NewLockAwareBatch creates a Batch with the same properties as
	// the ReadWriter created by NewLockAwareReadWriter.
	NewLockAwareBatch(engine.Batch, *roachpb.Transaction) engine.Batch
	// RemoveLock removes the specified lock from the manager's lock table. If
	// the lock's final state differs from its current state such that the
	// provision intent it is protecting needs to be modified before unlocking,
	// the callback function is called.
	RemoveLock(
		context.Context, engine.ReadWriter, *enginepb.MVCCStats, roachpb.Intent, LockUpdatedCallback,
	) error
	// RemoveLocks removes any locks in the specified key span from the
	// manager's lock table. If any locks' final states differs from their
	// current states such that their provision intents they are protecting need
	// to be modified before unlocking, the callback function is called.
	RemoveLocks(
		context.Context, engine.ReadWriter, *enginepb.MVCCStats, roachpb.Intent, int64, LockUpdatedCallback,
	) (int64, *roachpb.Span, error)

	// SetDescriptor informs the concurrency manager of its Range's descriptor.
	SetDescriptor(*roachpb.RangeDescriptor)
}

// Request is the input to Manager.SequenceRequest.
// NB: this probably isn't actually how this would look (e.g. there's no need
// for an interface), but it was useful for prototyping.
type Request interface {
	context() context.Context
	txn() uuid.UUID
	txnMeta() enginepb.TxnMeta
	spans() *spanset.SpanSet
	ts() hlc.Timestamp
	header() roachpb.Header
}

// LockUpdatedCallback is called when a provisional intent value needs to be
// updated before its lock can be removed. It is called with the key that the
// provisional value is currently written at and the state that the value must
// be moved into.
type LockUpdatedCallback func(engine.MVCCKey, roachpb.Intent) error

// latchManager serializes access to keys and key ranges.
type latchManager interface {
	acquire(Request) (latchGuard, error)
	release(latchGuard)
}

type latchGuard interface{}

// lockTable holds a collection of locks acquired by in-progress transactions.
type lockTable interface {
	scan(Request) (roachpb.Intent, bool, error)
	insertOrUpdate(
		context.Context,
		engine.Writer,
		*enginepb.MVCCStats,
		*enginepb.TxnMeta,
		engine.MVCCKey,
		func(*enginepb.MVCCLock) (bool, []byte, error),
	) error
	removeOne(
		context.Context,
		engine.ReadWriter,
		*enginepb.MVCCStats,
		roachpb.Intent,
		LockUpdatedCallback,
	) error
	removeMany(
		context.Context,
		engine.ReadWriter,
		*enginepb.MVCCStats,
		roachpb.Intent,
		int64,
		LockUpdatedCallback,
	) (int64, *roachpb.Span, error)

	newLockAwareReadWriter(engine.ReadWriter, *roachpb.Transaction) engine.ReadWriter
	newLockAwareBatch(engine.Batch, *roachpb.Transaction) engine.Batch

	setBounds(start, end roachpb.RKey)
}

// lockWaitQueue holds a collection of wait-queues for individual locks.
type lockWaitQueue interface {
	getFirstQueue(Request) lockWaitQueueKey
	waitInQueue(Request, lockWaitQueueKey) (lockWaitQueueGuard, error)
	createAndWaitInQueue(Request, roachpb.Intent) (lockWaitQueueGuard, error)
	exitQueue(lockWaitQueueGuard)
}

type lockWaitQueueKey interface{}
type lockWaitQueueGuard interface{}

// txnWaitQueue holds a collection of wait-queues for transaction records.
type txnWaitQueue interface {
	// TODO(WIP): flesh out unexported interface.
	// TODO(WIP): lift exported txnwait.Queue interface to Manager.
	// TODO(WIP): pull in txnwait.Queue implementation.
}
