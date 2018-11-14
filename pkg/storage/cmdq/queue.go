// Copyright 2018 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package cmdq

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/spanset"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

// A Queue maintains an interval tree of keys or key ranges for executing
// commands. New commands affecting keys or key ranges must wait on
// already-executing commands which overlap their key range.
//
// Before executing, commands invoke Queue.Lock and provide details about the
// spans that they plan to touch. Lock inserts the command into the queue and
// waits on prerequisite command that were already in the command queue. After
// executing, commands invoke Queue.Unlock with the LockGuard acquired when the
// queue was originally locked. Doing so removes the command from the queue and
// signals to waiting dependent commands that they no longer need to wait on the
// completed command.
//
// Queue is safe for concurrent use by multiple goroutines. Concurrent access is
// made efficient using a copy-on-write technique to capture immutable snapshots
// of the queue's inner btrees. Using this strategy, tasks requiring mutual
// exclusion are limited to updating the queue's trees and grabbing snapshots.
// Notably, scanning for and waiting on prerequisite commands is performed
// outside of the mutual exclusion zone. This means that the work performed
// under lock is linear with respect to the number of spans that a command
// declares but NOT linear with respect to the number of other commands that it
// will wait on.
type Queue struct {
	mu      syncutil.Mutex
	idAlloc int64
	scopes  [spanset.NumSpanScope]scopedQueue
}

type scopedQueue struct {
	q     *Queue
	scope spanset.SpanScope

	rSet  map[*cmd]struct{}
	trees [spanset.NumSpanAccess]btree
}

// snapshot is an immutable view into the command queue's state.
type snapshot struct {
	trees [spanset.NumSpanScope][spanset.NumSpanAccess]btree
}

// close closes the snapshot and releases any associated resources.
func (sn *snapshot) close() {
	for s := spanset.SpanScope(0); s < spanset.NumSpanScope; s++ {
		for a := spanset.SpanAccess(0); a < spanset.NumSpanAccess; a++ {
			sn.trees[s][a].Reset()
		}
	}
}

// cmds are stored in the Queue's btrees.
type cmd struct {
	id   int64
	span roachpb.Span
	ts   hlc.Timestamp
	done *signal

	inReadSet bool // is this cmd buffered in readsBuffer
	// expanded  int32 // have the children been added; accessed atomically
	// children  []cmd
}

// func (c *cmd) childrenOverlap(r interval.Range) bool {
// 	// TODO(nvanbenschoten): use fact that childred are sorted... or stop
// 	// sorting them.
// 	for i := range c.children {
// 		child := &c.children[i]
// 		if interval.ExclusiveOverlapper.Overlap(child.span, r) {
// 			return true
// 		}
// 	}
// 	return false
// }

// LockGuard is a handle to a Queue lock. It is returned by Queue.Lock and
// accepted by Queue.Unlock.
type LockGuard struct {
	done signal
	cmds [spanset.NumSpanScope][spanset.NumSpanAccess][]cmd
}

func newLockGuard(spans *spanset.SpanSet) *LockGuard {
	nCmds := 0
	for s := spanset.SpanScope(0); s < spanset.NumSpanScope; s++ {
		for a := spanset.SpanAccess(0); a < spanset.NumSpanAccess; a++ {
			nCmds += cmdsForSpans(spans.GetSpans(a, s))
		}
	}

	// LockGuard would be an ideal candidate for object pooling, but without
	// reference counting its cmds, we can't know whether they're still
	// referenced by other tree snapshots and therefore safe for re-use.
	var guard *LockGuard
	var cmds []cmd
	if nCmds <= 2 {
		alloc := new(struct {
			g    LockGuard
			cmds [2]cmd
		})
		guard = &alloc.g
		cmds = alloc.cmds[:nCmds]
	} else if nCmds <= 4 {
		alloc := new(struct {
			g    LockGuard
			cmds [4]cmd
		})
		guard = &alloc.g
		cmds = alloc.cmds[:nCmds]
	} else if nCmds <= 8 {
		alloc := new(struct {
			g    LockGuard
			cmds [8]cmd
		})
		guard = &alloc.g
		cmds = alloc.cmds[:nCmds]
	} else {
		guard = new(LockGuard)
		cmds = make([]cmd, nCmds)
	}

	for s := spanset.SpanScope(0); s < spanset.NumSpanScope; s++ {
		for a := spanset.SpanAccess(0); a < spanset.NumSpanAccess; a++ {
			n := cmdsForSpans(spans.GetSpans(a, s))
			guard.cmds[s][a] = cmds[:n]
			cmds = cmds[n:]
		}
	}
	if len(cmds) != 0 {
		panic("alloc too large")
	}
	return guard
}

// New creates a new Queue.
func New() *Queue {
	q := new(Queue)
	for s := spanset.SpanScope(0); s < spanset.NumSpanScope; s++ {
		q.scopes[s] = scopedQueue{
			q:     q,
			scope: s,
			rSet:  make(map[*cmd]struct{}),
		}
	}
	return q
}

// Lock locks the Queue over the provided spans at the specified timestamp. In
// doing so, it waits for all overlapping spans to unlock before returning. It
// returns a LockGuard which must be provided to Unlock.
func (q *Queue) Lock(
	ctx context.Context, spans *spanset.SpanSet, ts hlc.Timestamp,
) (*LockGuard, error) {
	lg := newLockGuard(spans)

	q.mu.Lock()
	snap := q.snapshot(spans)
	q.insert(lg, spans, ts)
	q.mu.Unlock()
	defer snap.close()

	err := q.wait(ctx, lg, ts, snap)
	if err != nil {
		q.Unlock(lg)
		return nil, err
	}
	return lg, nil
}

// snapshot captures an immutable snapshot of the queue. It takes a spanset to
// limit the amount of state captured. Must be called with mu held.
func (q *Queue) snapshot(spans *spanset.SpanSet) snapshot {
	var snap snapshot
	for s := spanset.SpanScope(0); s < spanset.NumSpanScope; s++ {
		qq := &q.scopes[s]
		needRSnap := len(spans.GetSpans(spanset.SpanReadWrite, s)) > 0
		needWSnap := needRSnap || len(spans.GetSpans(spanset.SpanReadOnly, s)) > 0

		if needRSnap {
			if len(qq.rSet) > 0 {
				qq.flushReadSet()
			}
			snap.trees[s][spanset.SpanReadOnly] = qq.trees[spanset.SpanReadOnly].Clone()
		}
		if needWSnap {
			snap.trees[s][spanset.SpanReadWrite] = qq.trees[spanset.SpanReadWrite].Clone()
		}
	}
	return snap
}

// flushReadSet flushes the read set into the read interval tree.
func (qq *scopedQueue) flushReadSet() {
	rTree := qq.trees[spanset.SpanReadOnly]
	// TODO(nvanbenschoten): never re-alloc in go1.11.
	realloc := len(qq.rSet) > 16
	for cmd := range qq.rSet {
		cmd.inReadSet = false
		rTree.Set(cmd)
		if !realloc {
			delete(qq.rSet, cmd)
		}
	}
	if realloc {
		qq.rSet = make(map[*cmd]struct{})
	}
}

// insert inserts the provided spans into the Queue at the specified time. Must
// be called with mu held.
func (q *Queue) insert(lg *LockGuard, spans *spanset.SpanSet, ts hlc.Timestamp) {
	for s := spanset.SpanScope(0); s < spanset.NumSpanScope; s++ {
		q.scopes[s].insert(lg, spans, ts)
	}
}

func (qq *scopedQueue) insert(lg *LockGuard, spans *spanset.SpanSet, ts hlc.Timestamp) {
	for a := spanset.SpanAccess(0); a < spanset.NumSpanAccess; a++ {
		ss := spans.GetSpans(a, qq.scope)
		if len(ss) == 0 {
			continue
		}

		cmds := lg.cmds[qq.scope][a]
		if len(cmds) != cmdsForSpans(ss) {
			panic("lock guard improperly sized")
		}

		for i := range cmds {
			cmd := &cmds[i]
			cmd.id = qq.q.nextID()
			cmd.span = ss[i]
			cmd.ts = ts
			cmd.done = &lg.done

			switch a {
			case spanset.SpanReadOnly:
				cmd.inReadSet = true
				qq.rSet[cmd] = struct{}{}
			case spanset.SpanReadWrite:
				qq.trees[spanset.SpanReadWrite].Set(cmd)
			default:
				panic("unknown access")
			}
		}

		// cmd := &cmds[0]
		// cmd.children = cmds[1:]
		// covering := useCoveringSpan(qq.scope)
		// if len(ss) == 1 || covering {
		// 	cmd.id = qq.q.nextID()
		// 	cmd.span = spans.BoundarySpan(a, qq.scope).AsRange()
		// 	cmd.ts = ts
		// 	cmd.pending = lg.pending

		// 	switch a {
		// 	case spanset.SpanReadOnly:
		// 		cmd.inReadSet = true
		// 		qq.rSet[cmd] = struct{}{}
		// 	case spanset.SpanReadWrite:
		// 		wTree := qq.trees[spanset.SpanReadWrite]
		// 		wTree.Insert(cmd, false /* fast */)
		// 	default:
		// 		panic("unknown access")
		// 	}
		// }

		// if len(ss) > 1 {
		// 	for i := range cmd.children {
		// 		child := &cmd.children[i]
		// 		child.id = qq.q.nextID()
		// 		child.span = ss[i].AsRange()
		// 		child.ts = ts
		// 		child.pending = lg.pending

		// 		if !covering {
		// 			cmd.expanded = 1
		// 			switch a {
		// 			case spanset.SpanReadOnly:
		// 				child.inReadSet = true
		// 				qq.rSet[child] = struct{}{}
		// 			case spanset.SpanReadWrite:
		// 				wTree := qq.trees[spanset.SpanReadWrite]
		// 				fast := fast(len(cmd.children), wTree)
		// 				last := i == len(cmd.children)-1
		// 				wTree.Insert(child, fast)
		// 				if fast && last {
		// 					wTree.AdjustRanges()
		// 				}
		// 			default:
		// 				panic("unknown access")
		// 			}
		// 		}
		// 	}
		// }
	}
}

func (q *Queue) nextID() int64 {
	q.idAlloc++
	return q.idAlloc
}

// ignoreFn is used for non-interference of earlier reads with later writes.
// However, this is only desired for the global scope. Reads and writes to local
// keys are specified to always interfere, regardless of their timestamp. This
// is done to avoid confusion with local keys declared as part of proposer
// evaluated KV.
type ignoreFn func(ts, other hlc.Timestamp) bool

func ignoreLater(ts, other hlc.Timestamp) bool   { return ts.Less(other) }
func ignoreEarlier(ts, other hlc.Timestamp) bool { return other.Less(ts) }
func ignoreNothing(ts, other hlc.Timestamp) bool { return false }

func ifGlobal(fn ignoreFn, s spanset.SpanScope) ignoreFn {
	switch s {
	case spanset.SpanGlobal:
		return fn
	case spanset.SpanLocal:
		// All local commands interfere.
		return ignoreNothing
	default:
		panic("unknown scope")
	}
}

func (q *Queue) wait(
	ctx context.Context, lg *LockGuard, ts hlc.Timestamp, snap snapshot,
) error {
	for s := spanset.SpanScope(0); s < spanset.NumSpanScope; s++ {
		for a := spanset.SpanAccess(0); a < spanset.NumSpanAccess; a++ {
			cmds := lg.cmds[s][a]
			for i := range cmds {
				cmd := &cmds[i]
				switch a {
				case spanset.SpanReadOnly:
					// Wait for writes at equal or lower timestamps.
					it := snap.trees[s][spanset.SpanReadWrite].MakeIter()
					ignore := ifGlobal(ignoreLater, s)
					if err := iterAndWait(ctx, cmd, ts, &it, ignore); err != nil {
						return err
					}
				case spanset.SpanReadWrite:
					// Wait for reads at equal or higher timestamps.
					it := snap.trees[s][spanset.SpanReadOnly].MakeIter()
					ignore := ifGlobal(ignoreEarlier, s)
					if err := iterAndWait(ctx, cmd, ts, &it, ignore); err != nil {
						return err
					}
					// Wait for all other writes.
					it = snap.trees[s][spanset.SpanReadWrite].MakeIter()
					ignore = ignoreNothing
					if err := iterAndWait(ctx, cmd, ts, &it, ignore); err != nil {
						return err
					}
				default:
					panic("unknown access")
				}
			}
		}
	}
	return nil
}

func iterAndWait(
	ctx context.Context, search *cmd, ts hlc.Timestamp, it *iterator, ignore ignoreFn,
) error {
	done := ctx.Done()
	for it.FirstOverlap(search); it.Valid(); it.NextOverlap() {
		cmd := it.Cmd()
		if cmd.done.signaled() {
			continue
		}
		if ignore(cmd.ts, ts) {
			continue
		}
		select {
		case <-cmd.done.signalChan():
		case <-done:
			return ctx.Err()
		}
	}
	return nil
}

// func (q *Queue) expand(s spanset.SpanScope, a spanset.SpanAccess, cmd *cmd) {
// 	q.mu.Lock()
// 	defer q.mu.Unlock()
// 	if atomic.CompareAndSwapInt32(&cmd.expanded, 0, 1) {
// 		tree := q.scopes[s].trees[a]
// 		fast := fast(len(cmd.children)+1, tree)
// 		tree.Delete(cmd, fast)
// 		for i := range cmd.children {
// 			tree.Insert(&cmd.children[i], fast)
// 		}
// 		if fast {
// 			tree.AdjustRanges()
// 		}
// 		q.scopes[s].invalidateImmutableTree(a)
// 	}
// }

// Unlock unlocks the subset of the Queue held by the provided LockGuard. After
// being called, dependent commands can begin executing if not blocked on any
// other in-flight commands.
func (q *Queue) Unlock(lg *LockGuard) {
	lg.done.signal()
	q.mu.Lock()
	defer q.mu.Unlock()
	for s := spanset.SpanScope(0); s < spanset.NumSpanScope; s++ {
		q.scopes[s].unlock(lg)
	}
}

func (qq *scopedQueue) unlock(lg *LockGuard) {
	for a := spanset.SpanAccess(0); a < spanset.NumSpanAccess; a++ {
		cmds := lg.cmds[qq.scope][a]
		for i := range cmds {
			cmd := &cmds[i]
			if cmd.inReadSet {
				delete(qq.rSet, cmd)
			} else {
				qq.trees[a].Delete(cmd)
			}
		}

		// if len(cmd.children) == 0 || atomic.LoadInt32(&cmd.expanded) == 0 {
		// 	tree := qq.trees[a]
		// 	tree.Delete(cmd, false /* fast */)
		// } else {
		// 	for i := range cmd.children {
		// 		child := &cmd.children[i]
		// 		if child.inReadSet {
		// 			delete(qq.rSet, child)
		// 		} else {
		// 			tree := qq.trees[a]
		// 			fast := fast(len(cmd.children), tree)
		// 			last := i == len(cmd.children)-1
		// 			tree.Delete(child, fast)
		// 			if fast && last {
		// 				tree.AdjustRanges()
		// 			}
		// 		}
		// 	}
		// }
	}
}

// // should commands in the specified scope use the covering optimization?
// // TODO provide overview of optimization. Lazily generated.
// func useCoveringSpan(s spanset.SpanScope) bool {
// 	// TODO(nvanbenschoten): should we avoid this for small numbers of spans? < 4?
// 	return s == spanset.SpanGlobal
// }

// cmdsForSpans returns the number of cmd structs needed for the specified
// set of spans.
func cmdsForSpans(spans []roachpb.Span) int {
	// switch len(spans) {
	// case 0:
	// 	return 0
	// case 1:
	// 	return 1
	// default:
	// 	return len(spans) + 1
	// }
	return len(spans)
}
