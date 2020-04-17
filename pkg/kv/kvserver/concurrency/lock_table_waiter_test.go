// Copyright 2020 The Cockroach Authors.
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
	"testing"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/intentresolver"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/spanset"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/stretchr/testify/require"
)

type mockIntentResolver struct {
	pushTxn       func(context.Context, *enginepb.TxnMeta, roachpb.Header, roachpb.PushTxnType) (roachpb.Transaction, *Error)
	resolveIntent func(context.Context, roachpb.LockUpdate) *Error
}

func (m *mockIntentResolver) PushTransaction(
	ctx context.Context, txn *enginepb.TxnMeta, h roachpb.Header, pushType roachpb.PushTxnType,
) (roachpb.Transaction, *Error) {
	return m.pushTxn(ctx, txn, h, pushType)
}

func (m *mockIntentResolver) ResolveIntent(
	ctx context.Context, intent roachpb.LockUpdate, _ intentresolver.ResolveOptions,
) *Error {
	return m.resolveIntent(ctx, intent)
}

type mockLockTableGuard struct {
	mu            syncutil.Mutex
	state         waitingState // protected by mu, but only in multi-threaded tests
	signal        chan struct{}
	stateObserved chan struct{}
}

func newMockLockTableGuard() *mockLockTableGuard {
	return &mockLockTableGuard{
		signal: make(chan struct{}, 1),
	}
}

func (g *mockLockTableGuard) ShouldWait() bool            { return true }
func (g *mockLockTableGuard) NewStateChan() chan struct{} { return g.signal }
func (g *mockLockTableGuard) CurState() waitingState {
	g.mu.Lock()
	defer g.mu.Unlock()
	s := g.state
	if g.stateObserved != nil {
		g.stateObserved <- struct{}{}
	}
	return s
}
func (g *mockLockTableGuard) notify() { g.signal <- struct{}{} }
func (g *mockLockTableGuard) notifyKind(kind waitKind) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.state = waitingState{kind: kind}
	g.notify()
}
func (g *mockLockTableGuard) notifyDoneWaiting() { g.notifyKind(doneWaiting) }
func (g *mockLockTableGuard) notifyAborted()     { g.notifyKind(txnAborted) }

func setupLockTableWaiterTest() (*lockTableWaiterImpl, *mockIntentResolver, *mockLockTableGuard) {
	ir := &mockIntentResolver{}
	st := cluster.MakeTestingClusterSettings()
	LockTableLivenessPushDelay.Override(&st.SV, 0)
	LockTableDeadlockDetectionPushDelay.Override(&st.SV, 0)
	w := &lockTableWaiterImpl{
		st:      st,
		stopper: stop.NewStopper(),
		ir:      ir,
	}
	guard := newMockLockTableGuard()
	return w, ir, guard
}

func makeTxnProto(name string) roachpb.Transaction {
	return roachpb.MakeTransaction(name, []byte("key"), 0, hlc.Timestamp{WallTime: 10}, 0)
}

// TestLockTableWaiterWithTxn tests the lockTableWaiter's behavior under
// different waiting states while a transactional request is waiting.
func TestLockTableWaiterWithTxn(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()

	maxTS := hlc.Timestamp{WallTime: 15}
	makeReq := func() Request {
		txn := makeTxnProto("request")
		txn.MaxTimestamp = maxTS
		return Request{
			Txn:       &txn,
			Timestamp: txn.ReadTimestamp,
		}
	}

	t.Run("state", func(t *testing.T) {
		t.Run("waitFor", func(t *testing.T) {
			testWaitPush(t, waitFor, makeReq, maxTS)
		})

		t.Run("waitForDistinguished", func(t *testing.T) {
			testWaitPush(t, waitForDistinguished, makeReq, maxTS)
		})

		t.Run("waitElsewhere", func(t *testing.T) {
			testWaitPush(t, waitElsewhere, makeReq, maxTS)
		})

		t.Run("waitSelf", func(t *testing.T) {
			testWaitNoopUntilDone(t, waitSelf, makeReq)
		})

		t.Run("doneWaiting", func(t *testing.T) {
			w, _, g := setupLockTableWaiterTest()
			defer w.stopper.Stop(ctx)

			g.state = waitingState{kind: doneWaiting}
			g.notify()

			err := w.WaitOn(ctx, makeReq(), g)
			require.Nil(t, err)
		})

		t.Run("txnAborted", func(t *testing.T) {
			w, _, g := setupLockTableWaiterTest()
			defer w.stopper.Stop(ctx)

			g.state = waitingState{kind: txnAborted}
			g.notify()

			err := w.WaitOn(ctx, makeReq(), g)
			require.NotNil(t, err)
			require.Regexp(t, `TransactionAbortedError\(ABORT_REASON_PUSHER_ABORTED\)`, err)
		})
	})

	t.Run("ctx done", func(t *testing.T) {
		w, _, g := setupLockTableWaiterTest()
		defer w.stopper.Stop(ctx)

		ctxWithCancel, cancel := context.WithCancel(ctx)
		go cancel()

		err := w.WaitOn(ctxWithCancel, makeReq(), g)
		require.NotNil(t, err)
		require.Equal(t, context.Canceled.Error(), err.GoError().Error())
	})

	t.Run("stopper quiesce", func(t *testing.T) {
		w, _, g := setupLockTableWaiterTest()
		defer w.stopper.Stop(ctx)

		go func() {
			w.stopper.Quiesce(ctx)
		}()

		err := w.WaitOn(ctx, makeReq(), g)
		require.NotNil(t, err)
		require.IsType(t, &roachpb.NodeUnavailableError{}, err.GetDetail())
	})
}

// TestLockTableWaiterWithNonTxn tests the lockTableWaiter's behavior under
// different waiting states while a non-transactional request is waiting.
func TestLockTableWaiterWithNonTxn(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()

	reqHeaderTS := hlc.Timestamp{WallTime: 10}
	makeReq := func() Request {
		return Request{
			Timestamp: reqHeaderTS,
			Priority:  roachpb.NormalUserPriority,
		}
	}

	t.Run("state", func(t *testing.T) {
		t.Run("waitFor", func(t *testing.T) {
			t.Log("waitFor does not cause non-transactional requests to push")
			testWaitNoopUntilDone(t, waitFor, makeReq)
		})

		t.Run("waitForDistinguished", func(t *testing.T) {
			testWaitPush(t, waitForDistinguished, makeReq, reqHeaderTS)
		})

		t.Run("waitElsewhere", func(t *testing.T) {
			testWaitPush(t, waitElsewhere, makeReq, reqHeaderTS)
		})

		t.Run("waitSelf", func(t *testing.T) {
			t.Log("waitSelf is not possible for non-transactional request")
		})

		t.Run("doneWaiting", func(t *testing.T) {
			w, _, g := setupLockTableWaiterTest()
			defer w.stopper.Stop(ctx)

			g.state = waitingState{kind: doneWaiting}
			g.notify()

			err := w.WaitOn(ctx, makeReq(), g)
			require.Nil(t, err)
		})

		t.Run("txnAborted", func(t *testing.T) {
			t.Log("txnAborted is not possible for non-transactional request")
		})
	})

	t.Run("ctx done", func(t *testing.T) {
		w, _, g := setupLockTableWaiterTest()
		defer w.stopper.Stop(ctx)

		ctxWithCancel, cancel := context.WithCancel(ctx)
		go cancel()

		err := w.WaitOn(ctxWithCancel, makeReq(), g)
		require.NotNil(t, err)
		require.Equal(t, context.Canceled.Error(), err.GoError().Error())
	})

	t.Run("stopper quiesce", func(t *testing.T) {
		w, _, g := setupLockTableWaiterTest()
		defer w.stopper.Stop(ctx)

		go func() {
			w.stopper.Quiesce(ctx)
		}()

		err := w.WaitOn(ctx, makeReq(), g)
		require.NotNil(t, err)
		require.IsType(t, &roachpb.NodeUnavailableError{}, err.GetDetail())
	})
}

func testWaitPush(t *testing.T, k waitKind, makeReq func() Request, expPushTS hlc.Timestamp) {
	ctx := context.Background()
	keyA := roachpb.Key("keyA")
	testutils.RunTrueAndFalse(t, "lockHeld", func(t *testing.T, lockHeld bool) {
		testutils.RunTrueAndFalse(t, "waitAsWrite", func(t *testing.T, waitAsWrite bool) {
			w, ir, g := setupLockTableWaiterTest()
			defer w.stopper.Stop(ctx)
			pusheeTxn := makeTxnProto("pushee")

			req := makeReq()
			g.state = waitingState{
				kind:        k,
				txn:         &pusheeTxn.TxnMeta,
				key:         keyA,
				guardAccess: spanset.SpanReadOnly,
			}
			if !lockHeld {
				g.state.req = newMockLockTableGuard()
			}
			if waitAsWrite {
				g.state.guardAccess = spanset.SpanReadWrite
			}
			g.notify()

			// waitElsewhere does not cause a push if the lock is not held.
			// It returns immediately.
			if k == waitElsewhere && !lockHeld {
				err := w.WaitOn(ctx, req, g)
				require.Nil(t, err)
				return
			}

			// Non-transactional requests do not push reservations, only locks.
			// They wait for doneWaiting.
			if req.Txn == nil && !lockHeld {
				defer notifyUntilDone(t, g)()
				err := w.WaitOn(ctx, req, g)
				require.Nil(t, err)
				return
			}

			ir.pushTxn = func(
				_ context.Context,
				pusheeArg *enginepb.TxnMeta,
				h roachpb.Header,
				pushType roachpb.PushTxnType,
			) (roachpb.Transaction, *Error) {
				require.Equal(t, &pusheeTxn.TxnMeta, pusheeArg)
				require.Equal(t, req.Txn, h.Txn)
				require.Equal(t, expPushTS, h.Timestamp)
				if waitAsWrite || !lockHeld {
					require.Equal(t, roachpb.PUSH_ABORT, pushType)
				} else {
					require.Equal(t, roachpb.PUSH_TIMESTAMP, pushType)
				}

				resp := roachpb.Transaction{TxnMeta: *pusheeArg, Status: roachpb.ABORTED}

				// If the lock is held, we'll try to resolve it now that
				// we know the holder is ABORTED. Otherwide, immediately
				// tell the request to stop waiting.
				if lockHeld {
					ir.resolveIntent = func(_ context.Context, intent roachpb.LockUpdate) *Error {
						require.Equal(t, keyA, intent.Key)
						require.Equal(t, pusheeTxn.ID, intent.Txn.ID)
						require.Equal(t, roachpb.ABORTED, intent.Status)
						g.state = waitingState{kind: doneWaiting}
						g.notify()
						return nil
					}
				} else {
					g.state = waitingState{kind: doneWaiting}
					g.notify()
				}
				return resp, nil
			}

			err := w.WaitOn(ctx, req, g)
			require.Nil(t, err)
		})
	})
}

func testWaitNoopUntilDone(t *testing.T, k waitKind, makeReq func() Request) {
	ctx := context.Background()
	w, _, g := setupLockTableWaiterTest()
	defer w.stopper.Stop(ctx)

	g.state = waitingState{kind: k}
	g.notify()
	defer notifyUntilDone(t, g)()

	err := w.WaitOn(ctx, makeReq(), g)
	require.Nil(t, err)
}

func notifyUntilDone(t *testing.T, g *mockLockTableGuard) func() {
	// Set up an observer channel to detect when the current
	// waiting state is observed.
	g.stateObserved = make(chan struct{})
	done := make(chan struct{})
	go func() {
		<-g.stateObserved
		g.notify()
		<-g.stateObserved
		g.state = waitingState{kind: doneWaiting}
		g.notify()
		<-g.stateObserved
		close(done)
	}()
	return func() { <-done }
}

// TestLockTableWaiterIntentResolverError tests that the lockTableWaiter
// propagates errors from its intent resolver when it pushes transactions
// or resolves their intents.
func TestLockTableWaiterIntentResolverError(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	w, ir, g := setupLockTableWaiterTest()
	defer w.stopper.Stop(ctx)

	err1 := roachpb.NewErrorf("error1")
	err2 := roachpb.NewErrorf("error2")

	txn := makeTxnProto("request")
	req := Request{
		Txn:       &txn,
		Timestamp: txn.ReadTimestamp,
	}

	// Test with both synchronous and asynchronous pushes.
	// See the comments on pushLockTxn and pushRequestTxn.
	testutils.RunTrueAndFalse(t, "sync", func(t *testing.T, sync bool) {
		keyA := roachpb.Key("keyA")
		pusheeTxn := makeTxnProto("pushee")
		g.state = waitingState{
			kind:        waitForDistinguished,
			txn:         &pusheeTxn.TxnMeta,
			key:         keyA,
			guardAccess: spanset.SpanReadWrite,
		}
		if !sync {
			g.state.req = newMockLockTableGuard()
		}

		// Errors are propagated when observed while pushing transactions.
		g.notify()
		ir.pushTxn = func(
			_ context.Context, _ *enginepb.TxnMeta, _ roachpb.Header, _ roachpb.PushTxnType,
		) (roachpb.Transaction, *Error) {
			return roachpb.Transaction{}, err1
		}
		err := w.WaitOn(ctx, req, g)
		require.Equal(t, err1, err)

		if sync {
			// Errors are propagated when observed while resolving intents.
			g.notify()
			ir.pushTxn = func(
				_ context.Context, _ *enginepb.TxnMeta, _ roachpb.Header, _ roachpb.PushTxnType,
			) (roachpb.Transaction, *Error) {
				return roachpb.Transaction{}, nil
			}
			ir.resolveIntent = func(_ context.Context, intent roachpb.LockUpdate) *Error {
				return err2
			}
			err = w.WaitOn(ctx, req, g)
			require.Equal(t, err2, err)
		}
	})
}

// TestLockTableWaiterSimultaneousAbort tests that if two conflicting requests
// simultaneously abort each other's transaction records while pushing, at least
// one is informed that it is aborted in order to break the local deadlock. The
// test exercises the reqConflict.notifyAborted mechanism through which aborted
// statuses are communicated directly between conflicting requests waiting in
// the lockTable. Without that mechanism, the test would fail.
func TestLockTableWaiterSimultaneousAbort(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	w, ir, _ := setupLockTableWaiterTest()
	defer w.stopper.Stop(ctx)

	keyA := roachpb.Key("keyA")
	makeReq := func() Request {
		txn := makeTxnProto("request1")
		return Request{
			Txn:       &txn,
			Timestamp: txn.ReadTimestamp,
		}
	}

	type lockTableWaiterReq struct {
		req   Request
		g     *mockLockTableGuard
		pushC chan chan struct{}
		resC  chan *Error
	}
	var reqs [2]lockTableWaiterReq
	for i := range reqs {
		reqs[i] = lockTableWaiterReq{
			req:   makeReq(),
			g:     newMockLockTableGuard(),
			pushC: make(chan chan struct{}),
			resC:  make(chan *Error, 1),
		}
	}

	// NOTE: there can only be one pushTxn func attached to mockIntentResolver,
	// so we can't capture per-request state across multiple closures.
	ir.pushTxn = func(
		_ context.Context,
		pusheeArg *enginepb.TxnMeta,
		h roachpb.Header,
		pushType roachpb.PushTxnType,
	) (roachpb.Transaction, *Error) {
		// Determine which request this is.
		var req *lockTableWaiterReq
		for i := range reqs {
			if reqs[i].req.Txn.ID == h.Txn.ID {
				req = &reqs[i]
			}
		}
		require.NotNil(t, req)
		require.Equal(t, roachpb.PUSH_ABORT, pushType)

		// Barrier.
		barrierC := make(chan struct{})
		req.pushC <- barrierC
		<-barrierC

		// Push successful, abort pushee transaction.
		resp := roachpb.Transaction{TxnMeta: *pusheeArg, Status: roachpb.ABORTED}
		return resp, nil
	}
	for i := range reqs {
		req := &reqs[i]
		otherReq := &reqs[(i+1)%len(reqs)]
		go func() {
			req.g.state = waitingState{
				kind:        waitForDistinguished,
				txn:         &otherReq.req.Txn.TxnMeta,
				key:         keyA,
				req:         otherReq.g,
				guardAccess: spanset.SpanReadWrite,
			}
			req.g.notify()
			err := w.WaitOn(ctx, req.req, req.g)

			// Once the request exists the lockTable, mark the other
			// request as doneWaiting. The deadlock has been resolved.
			otherReq.g.notifyDoneWaiting()
			req.resC <- err
		}()
	}

	// Both requests should push.
	var barriers [len(reqs)]chan struct{}
	for i, req := range reqs {
		barriers[i] = <-req.pushC
	}

	// Both requests pushing. Let both succeed.
	for _, b := range barriers {
		close(b)
	}

	// Both requests should complete. At least one should return a
	// TransactionAbortedError, although it's valid if both return one.
	var txnAbortedErrs int
	for _, req := range reqs {
		err := <-req.resC
		if err != nil {
			require.Regexp(t, `TransactionAbortedError\(ABORT_REASON_PUSHER_ABORTED\)`, err)
			txnAbortedErrs++
		}
	}
	require.GreaterOrEqual(t, txnAbortedErrs, 1)
	t.Logf("%d requests completed, %d returned txn aborted error", len(reqs), txnAbortedErrs)
}
