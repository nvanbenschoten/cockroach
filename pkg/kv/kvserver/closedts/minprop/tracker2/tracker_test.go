package tracker2

import (
	"testing"
	"time"
)

// Results with go1.15.5 on a Mac with a 2.3 GHz 8-Core (16 vCPU) Intel Core i9:
//
// name        time/op
// Tracker      140ns ± 1%
// Tracker-2   62.2ns ± 3%
// Tracker-4    153ns ± 1%
// Tracker-8    200ns ± 2%
// Tracker-16   171ns ±15%
//
func BenchmarkTracker(b *testing.B) {
	var t tracker
	t.Init(func(prevTime int64) int64 {
		now := time.Now().UnixNano()
		if now < prevTime {
			now = prevTime
		}
		return now
	})
	b.RunParallel(func(b *testing.PB) {
		for b.Next() {
			bucket, minTime := t.Track()
			_ = minTime

			closedTime := t.ClosedTime()
			_ = closedTime

			t.Release(bucket)
		}
	})
}
