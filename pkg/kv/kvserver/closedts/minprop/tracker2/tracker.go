package tracker2

import (
	"sync"
	"sync/atomic"
)

type tracker struct {
	nextTime func(prevTime int64) int64

	mu         sync.RWMutex
	prev, next *bucket
}

type bucket struct {
	time int64 // atomic

	// Prevent false sharing between read-only (after initialization) and
	// read-write cache lines. This has a measurable impact on performance at
	// high concurrency levels.
	pad [64]byte

	ref   int32 // atomic
	shift int32 // atomic
}

func (b *bucket) reset() {
	// NOTE: assign directly to avoid re-writing pad.
	b.time = 0
	b.ref = 0
	b.shift = 0
}

func (t *tracker) Init(nextTime func(prevTime int64) int64) {
	*t = tracker{
		nextTime: nextTime,
		prev:     new(bucket),
		next:     new(bucket),
	}
}

func (t *tracker) Track() (*bucket, int64) {
	t.mu.RLock()
	prev, next := t.prev, t.next

	// Modify.
	ref := atomic.AddInt32(&next.ref, 1)
	var time int64
	for {
		time = atomic.LoadInt64(&next.time)
		if time != 0 {
			break
		}
		// Initialize.
		prevTime := atomic.LoadInt64(&prev.time)
		time = t.nextTime(prevTime)
		if atomic.CompareAndSwapInt64(&next.time, 0, time) {
			break
		}
	}

	// Try shift if prev is empty and this call initialized next. The latter
	// condition isn't strictly necessary, but it helps reduce contention.
	shift := ref == 1 && atomic.LoadInt32(&prev.ref) == 0
	if shift {
		// Reserve shift.
		shift = atomic.CompareAndSwapInt32(&prev.shift, 0, 1)
	}
	t.mu.RUnlock()

	if shift {
		t.shift()
	}

	return next, time
}

func (t *tracker) Release(b *bucket) {
	t.mu.RLock()
	prev, next := t.prev, t.next

	// Modify.
	ref := atomic.AddInt32(&b.ref, -1)

	// Try shift if we released from prev and prev is now empty. Only do so if
	// the next bucket has already been initialized with a valid time.
	shift := b == prev && ref == 0 && atomic.LoadInt64(&next.time) != 0
	if shift {
		// Reserve shift.
		shift = atomic.CompareAndSwapInt32(&prev.shift, 0, 1)
	}
	t.mu.RUnlock()

	if shift {
		t.shift()
	}
}

func (t *tracker) shift() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.prev.reset()
	t.prev, t.next = t.next, t.prev
}

func (t *tracker) ClosedTime() int64 {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return atomic.LoadInt64(&t.prev.time)
}
