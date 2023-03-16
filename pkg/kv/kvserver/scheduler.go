// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package kvserver

import (
	"context"
	"fmt"
	"runtime/debug"
	"sync"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

const rangeIDChunkSize = 1000

type rangeIDChunk struct {
	// Valid contents are buf[rd:wr], read at buf[rd], write at buf[wr].
	buf        [rangeIDChunkSize]roachpb.RangeID
	rd, wr     int
	next, prev *rangeIDChunk // owned by rangeIDChunkList
}

func (c *rangeIDChunk) PushBack(id roachpb.RangeID) bool {
	if c.WriteCap() == 0 {
		return false
	}
	c.buf[c.wr] = id
	c.wr++
	return true
}

func (c *rangeIDChunk) PopFront() (roachpb.RangeID, bool) {
	if c.Len() == 0 {
		return 0, false
	}
	id := c.buf[c.rd]
	c.rd++
	return id, true
}

func (c *rangeIDChunk) WriteCap() int {
	return len(c.buf) - c.wr
}

func (c *rangeIDChunk) Len() int {
	return c.wr - c.rd
}

func (c *rangeIDChunk) Reset() {
	// c.buf does not need to be cleared.
	c.rd, c.wr = 0, 0
	// c.next and c.prev reset by rangeIDChunkList.Remove.
}

type rangeIDChunkList struct {
	root rangeIDChunk
	len  int
}

func (cl *rangeIDChunkList) Front() *rangeIDChunk {
	if cl.len == 0 {
		return nil
	}
	return cl.root.next
}

func (cl *rangeIDChunkList) Back() *rangeIDChunk {
	if cl.len == 0 {
		return nil
	}
	return cl.root.prev
}

func (cl *rangeIDChunkList) lazyInit() {
	if cl.root.next == nil {
		cl.root.next = &cl.root
		cl.root.prev = &cl.root
	}
}

func (cl *rangeIDChunkList) PushBack(c *rangeIDChunk) {
	cl.lazyInit()
	at := cl.root.prev
	n := at.next
	at.next = c
	c.prev = at
	c.next = n
	n.prev = c
	cl.len++
}

func (cl *rangeIDChunkList) Remove(c *rangeIDChunk) {
	c.prev.next = c.next
	c.next.prev = c.prev
	c.next = nil // avoid memory leaks
	c.prev = nil // avoid memory leaks
	cl.len--
}

// rangeIDQueue is a chunked queue of range IDs. Instead of a separate list
// element for every range ID, it uses a rangeIDChunk to hold many range IDs,
// amortizing the allocation/GC cost. Using a chunk queue avoids any copying
// that would occur if a slice were used (the copying would occur on slice
// reallocation).
//
// The queue has a naive understanding of priority and fairness. For the most
// part, it implements a FIFO queueing policy with no prioritization of some
// ranges over others. However, the queue can be configured with up to one
// high-priority range, which will always be placed at the front when added.
type rangeIDQueue struct {
	len int

	// Default priority.
	chunks   rangeIDChunkList
	recycled *rangeIDChunk

	// High priority.
	priorityID     roachpb.RangeID
	priorityStack  []byte // for debugging in case of assertion failure; see #75939
	priorityQueued bool
}

func (q *rangeIDQueue) Push(id roachpb.RangeID) {
	q.len++
	if q.priorityID == id {
		q.priorityQueued = true
		return
	}
	back := q.chunks.Back()
	if back == nil || back.WriteCap() == 0 {
		back = q.recycled
		if back != nil {
			q.recycled = nil
		} else {
			back = new(rangeIDChunk)
		}
		q.chunks.PushBack(back)
	}
	if !back.PushBack(id) {
		panic(fmt.Sprintf(
			"unable to push rangeID to chunk: len=%d, cap=%d",
			back.Len(), back.WriteCap()))
	}
}

func (q *rangeIDQueue) PopFront() (roachpb.RangeID, bool) {
	if q.len == 0 {
		return 0, false
	}
	q.len--
	if q.priorityQueued {
		q.priorityQueued = false
		return q.priorityID, true
	}
	front := q.chunks.Front()
	id, ok := front.PopFront()
	if !ok {
		panic("encountered empty chunk")
	}
	if front.Len() == 0 && front.WriteCap() == 0 {
		q.chunks.Remove(front)
		front.Reset()
		q.recycled = front
	}
	return id, true
}

func (q *rangeIDQueue) Len() int {
	return q.len
}

func (q *rangeIDQueue) SetPriorityID(id roachpb.RangeID) {
	if q.priorityID != 0 && q.priorityID != id {
		panic(fmt.Sprintf(
			"priority range ID already set: old=%d, new=%d, first set at:\n\n%s",
			q.priorityID, id, q.priorityStack))
	}
	q.priorityStack = debug.Stack()
	q.priorityID = id
}

type raftProcessor interface {
	// Process a raft.Ready struct containing entries and messages that are
	// ready to read, be saved to stable storage, committed, or sent to other
	// peers.
	//
	// This method does not take a ctx; the implementation is expected to use a
	// ctx annotated with the range information, according to RangeID.
	processReady(roachpb.RangeID)
	// Process all queued messages for the specified range.
	// Return true if the range should be queued for ready processing.
	processRequestQueue(context.Context, roachpb.RangeID) bool
	// Process a raft tick for the specified range.
	// Return true if the range should be queued for ready processing.
	processTick(context.Context, roachpb.RangeID) bool
}

type raftScheduleFlags int

const (
	stateQueued raftScheduleFlags = 1 << iota
	stateRaftReady
	stateRaftRequest
	stateRaftTick
)

type raftScheduleState struct {
	flags raftScheduleFlags
	begin int64 // nanoseconds

	// The number of ticks queued. Usually it's 0 or 1, but may go above if the
	// scheduling or processing is slow. It is limited by raftScheduler.maxTicks,
	// so that the cost of processing all the ticks doesn't grow uncontrollably.
	// If ticks consistently reaches maxTicks, the node/range is too slow, and it
	// is safer to not deliver all the ticks as it may cause a cascading effect
	// (the range events take longer and longer to process).
	// TODO(pavelkalinnikov): add a node health metric for the ticks.
	//
	// INVARIANT: flags&stateRaftTick == 0 iff ticks == 0.
	ticks int
}

type raftScheduler struct {
	ambientContext log.AmbientContext
	processor      raftProcessor
	latency        metric.IHistogram
	numWorkers     int
	maxTicks       int

	shards []raftSchedulerShard

	done sync.WaitGroup
}

type raftSchedulerShard struct {
	syncutil.Mutex
	cond       *sync.Cond
	queue      rangeIDQueue
	state      map[roachpb.RangeID]raftScheduleState
	stopped    bool
	numWorkers int
	maxTicks   int
}

func newRaftScheduler(
	ambient log.AmbientContext,
	metrics *StoreMetrics,
	processor raftProcessor,
	numWorkers int,
	maxTicks int,
) *raftScheduler {
	numShards := numWorkers / 16
	if numShards < 1 {
		numShards = 1
	}
	s := &raftScheduler{
		ambientContext: ambient,
		processor:      processor,
		latency:        metrics.RaftSchedulerLatency,
		numWorkers:     numWorkers,
		maxTicks:       maxTicks,
		shards:         make([]raftSchedulerShard, numShards),
	}
	for i := range s.shards {
		s.shards[i].cond = sync.NewCond(&s.shards[i].Mutex)
		s.shards[i].state = make(map[roachpb.RangeID]raftScheduleState)
		s.shards[i].numWorkers = numWorkers / len(s.shards)
		s.shards[i].maxTicks = maxTicks
	}
	return s
}

func (s *raftScheduler) Start(stopper *stop.Stopper) {
	ctx := s.ambientContext.AnnotateCtx(context.Background())
	waitQuiesce := func(context.Context) {
		<-stopper.ShouldQuiesce()
		for i := range s.shards {
			s.shards[i].Lock()
			s.shards[i].stopped = true
			s.shards[i].Unlock()
			s.shards[i].cond.Broadcast()
		}
	}
	if err := stopper.RunAsyncTaskEx(ctx,
		stop.TaskOpts{
			TaskName: "raftsched-wait-quiesce",
			// This task doesn't reference a parent because it runs for the server's
			// lifetime.
			SpanOpt: stop.SterileRootSpan,
		},
		waitQuiesce); err != nil {
		waitQuiesce(ctx)
	}

	s.done.Add(s.numWorkers)
	for i := 0; i < s.numWorkers; i++ {
		shard := i % len(s.shards)
		if err := stopper.RunAsyncTaskEx(ctx,
			stop.TaskOpts{
				TaskName: "raft-worker",
				// This task doesn't reference a parent because it runs for the server's
				// lifetime.
				SpanOpt: stop.SterileRootSpan,
			},
			func(ctx context.Context) {
				s.worker(ctx, shard)
			}); err != nil {
			s.done.Done()
		}
	}
}

func (s *raftScheduler) Wait(context.Context) {
	s.done.Wait()
}

// SetPriorityID configures the single range that the scheduler will prioritize
// above others. Once set, callers are not permitted to change this value.
func (s *raftScheduler) SetPriorityID(id roachpb.RangeID) {
	for i := range s.shards {
		s.shards[i].Lock()
		s.shards[i].queue.SetPriorityID(id)
		s.shards[i].Unlock()
	}
}

func (s *raftScheduler) PriorityID() roachpb.RangeID {
	s.shards[0].Lock()
	defer s.shards[0].Unlock()
	return s.shards[0].queue.priorityID
}

func (s *raftScheduler) worker(ctx context.Context, shard int) {
	defer s.done.Done()

	// We use a sync.Cond for worker notification instead of a buffered
	// channel. Buffered channels have internal overhead for maintaining the
	// buffer even when the elements are empty. And the buffer isn't necessary as
	// the raftScheduler work is already buffered on the internal queue. Lastly,
	// signaling a sync.Cond is significantly faster than selecting and sending
	// on a buffered channel.

	s.shards[shard].Lock()
	for {
		var id roachpb.RangeID
		for {
			if s.shards[shard].stopped {
				s.shards[shard].Unlock()
				return
			}
			var ok bool
			if id, ok = s.shards[shard].queue.PopFront(); ok {
				break
			}
			s.shards[shard].cond.Wait()
		}

		// Grab and clear the existing state for the range ID. Note that we leave
		// the range ID marked as "queued" so that a concurrent Enqueue* will not
		// queue the range ID again.
		state := s.shards[shard].state[id]
		s.shards[shard].state[id] = raftScheduleState{flags: stateQueued}
		s.shards[shard].Unlock()

		// Record the scheduling latency for the range.
		lat := nowNanos() - state.begin
		s.latency.RecordValue(lat)

		// Process requests first. This avoids a scenario where a tick and a
		// "quiesce" message are processed in the same iteration and intervening
		// raft ready processing unquiesces the replica because the tick triggers
		// an election.
		if state.flags&stateRaftRequest != 0 {
			// processRequestQueue returns true if the range should perform ready
			// processing. Do not reorder this below the call to processReady.
			if s.processor.processRequestQueue(ctx, id) {
				state.flags |= stateRaftReady
			}
		}
		if util.RaceEnabled { // assert the ticks invariant
			if tick := state.flags&stateRaftTick != 0; tick != (state.ticks != 0) {
				log.Fatalf(ctx, "stateRaftTick is %v with ticks %v", tick, state.ticks)
			}
		}
		if state.flags&stateRaftTick != 0 {
			for t := state.ticks; t > 0; t-- {
				// processRaftTick returns true if the range should perform ready
				// processing. Do not reorder this below the call to processReady.
				if s.processor.processTick(ctx, id) {
					state.flags |= stateRaftReady
				}
			}
		}
		if state.flags&stateRaftReady != 0 {
			s.processor.processReady(id)
		}

		s.shards[shard].Lock()
		state = s.shards[shard].state[id]
		if state.flags == stateQueued {
			// No further processing required by the range ID, clear it from the
			// state map.
			delete(s.shards[shard].state, id)
		} else {
			// There was a concurrent call to one of the Enqueue* methods. Queue
			// the range ID for further processing.
			//
			// Even though the Enqueue* method did not signal after detecting
			// that the range was being processed, there also is no need for us
			// to signal the condition variable. This is because this worker
			// goroutine will loop back around and continue working without ever
			// going back to sleep.
			//
			// We can prove this out through a short derivation.
			// - For optimal concurrency, we want:
			//     awake_workers = min(max_workers, num_ranges)
			// - The condition variable / mutex structure ensures that:
			//     awake_workers = cur_awake_workers + num_signals
			// - So we need the following number of signals for optimal concurrency:
			//     num_signals = min(max_workers, num_ranges) - cur_awake_workers
			// - If we re-enqueue a range that's currently being processed, the
			//   num_ranges does not change once the current iteration completes
			//   and the worker does not go back to sleep between the current
			//   iteration and the next iteration, so no change to num_signals
			//   is needed.
			s.shards[shard].queue.Push(id)
		}
	}
}

func (s *raftSchedulerShard) enqueue1Locked(
	addFlags raftScheduleFlags, id roachpb.RangeID, now int64,
) int {
	ticks := int((addFlags & stateRaftTick) / stateRaftTick) // 0 or 1

	prevState := s.state[id]
	if prevState.flags&addFlags == addFlags && ticks == 0 {
		return 0
	}
	var queued int
	newState := prevState
	newState.flags = newState.flags | addFlags
	newState.ticks += ticks
	if newState.ticks > s.maxTicks {
		newState.ticks = s.maxTicks
	}
	if newState.flags&stateQueued == 0 {
		newState.flags |= stateQueued
		queued++
		s.queue.Push(id)
	}
	if newState.begin == 0 {
		newState.begin = now
	}
	s.state[id] = newState
	return queued
}

func (s *raftScheduler) enqueue1(addFlags raftScheduleFlags, id roachpb.RangeID) {
	now := nowNanos()
	shard := &s.shards[int(id)%len(s.shards)]
	shard.Lock()
	n := shard.enqueue1Locked(addFlags, id, now)
	shard.Unlock()
	shard.signal(n)
}

func (s *raftScheduler) enqueueN(addFlags raftScheduleFlags, ids ...roachpb.RangeID) {
	// Enqueue the ids in chunks to avoid hold raftScheduler.mu for too long.
	const enqueueChunkSize = 128

	// Avoid locking for 0 new ranges.
	if len(ids) == 0 {
		return
	}

	for i := range s.shards {
		shard := &s.shards[i]
		now := nowNanos()
		shard.Lock()
		var count int
		for j, id := range ids {
			if int(id)%len(s.shards) != i {
				continue
			}
			count += shard.enqueue1Locked(addFlags, id, now)
			if (j+1)%enqueueChunkSize == 0 {
				shard.Unlock()
				now = nowNanos()
				shard.Lock()
			}
		}
		shard.Unlock()
		shard.signal(count)
	}
}

func (s *raftSchedulerShard) signal(count int) {
	if count >= s.numWorkers {
		s.cond.Broadcast()
	} else {
		for i := 0; i < count; i++ {
			s.cond.Signal()
		}
	}
}

func (s *raftScheduler) EnqueueRaftReady(id roachpb.RangeID) {
	s.enqueue1(stateRaftReady, id)
}

func (s *raftScheduler) EnqueueRaftRequest(id roachpb.RangeID) {
	s.enqueue1(stateRaftRequest, id)
}

func (s *raftScheduler) EnqueueRaftRequests(ids ...roachpb.RangeID) {
	s.enqueueN(stateRaftRequest, ids...)
}

func (s *raftScheduler) EnqueueRaftTicks(ids ...roachpb.RangeID) {
	s.enqueueN(stateRaftTick, ids...)
}

func nowNanos() int64 {
	return timeutil.Now().UnixNano()
}
