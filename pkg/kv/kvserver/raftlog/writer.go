// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package raftlog

import (
	"context"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/stateloader"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"go.etcd.io/etcd/raft/v3/raftpb"
)

// Writer is responsible for performing log writes to a collection of replicas'
// raft logs. It exposes an asynchronous interface so that replicas can enqueue
// log writes without waiting for their completion. Instead, completion is
// signalled using a callback interface.
type Writer struct {
	eng         storage.Engine
	cache       RaftEntryCache
	e2eLatency  *metric.Histogram
	syncLatency *metric.Histogram
	shards      []writerShard
	stopped     atomic.Bool
}

// writerShard is responsible for a subset of ranges, sharded by range ID.
type writerShard struct {
	w *Writer

	eventsMu   syncutil.Mutex
	eventsCond sync.Cond
	events     []event

	syncQueueMu   syncutil.Mutex
	syncQueueCond sync.Cond
	syncQueue     []syncBatch
}

// event is a union of different event types that the Writer goroutines needs
// to be informed of. It is used so that all events can be sent over the same
// channel, which is necessary to prevent reordering.
type event struct {
	app   appendEvent
	syncC chan struct{}
}

type appendEvent struct {
	rr      RaftRange
	rangeID roachpb.RangeID
	msg     raftpb.Message
	start   time.Time
}

type syncWaiter interface {
	SyncWait() error
	Close()
}

type syncBatch struct {
	wg     syncWaiter
	events []event
	start  time.Time
}

func NewWriter(
	eng storage.Engine, cache RaftEntryCache, e2eLatency, syncLatency *metric.Histogram,
) *Writer {
	const shards = 4
	w := &Writer{
		eng:         eng,
		cache:       cache,
		e2eLatency:  e2eLatency,
		syncLatency: syncLatency,
		shards:      make([]writerShard, shards),
	}
	for i := range w.shards {
		s := &w.shards[i]
		s.w = w
		s.eventsCond.L = &s.eventsMu
		s.syncQueueCond.L = &s.syncQueueMu
	}
	return w
}

func (w *Writer) Start(stopper *stop.Stopper) {
	ctx := context.Background()
	waitQuiesce := func(context.Context) {
		<-stopper.ShouldQuiesce()
		w.stop()
	}
	// TODO: hook up to scheduler.
	//_ = stopper.RunAsyncTaskEx(ctx,
	//	stop.TaskOpts{
	//		TaskName: "raftlog-writer-wait-quiesce",
	//		// This task doesn't reference a parent because it runs for the server's
	//		// lifetime.
	//		SpanOpt: stop.SterileRootSpan,
	//	},
	//	waitQuiesce)
	//
	//for i := 0; i < len(w.pipeline.stages); i++ {
	//	_ = stopper.RunAsyncTaskEx(ctx,
	//		stop.TaskOpts{
	//			TaskName: "raftlog-writer-worker",
	//			// This task doesn't reference a parent because it runs for the server's
	//			// lifetime.
	//			SpanOpt: stop.SterileRootSpan,
	//		},
	//		w.writerLoop)
	//}
	go waitQuiesce(ctx)
	for i := range w.shards {
		s := &w.shards[i]
		go s.writerLoop(ctx)
		go s.publisherLoop(ctx)
	}
}

func (w *Writer) pushEvent(rangeID roachpb.RangeID, ev event) {
	shard := &w.shards[int(rangeID)%len(w.shards)]
	shard.eventsMu.Lock()
	wasEmpty := len(shard.events) == 0
	shard.events = append(shard.events, ev)
	shard.eventsMu.Unlock()
	if wasEmpty {
		shard.eventsCond.Signal()
	}
}

func (w *Writer) Append(
	ctx context.Context, rangeID roachpb.RangeID, rr RaftRange, msg raftpb.Message,
) {
	if msg.Type != raftpb.MsgStorageAppend {
		log.Fatalf(ctx, "unexpected message type %s", msg.Type)
	}
	w.pushEvent(rangeID, event{app: appendEvent{
		rr:      rr,
		rangeID: rangeID,
		msg:     msg,
		start:   timeutil.Now(),
	}})
}

func (w *Writer) Sync(rangeID roachpb.RangeID) {
	ch := make(chan struct{})
	w.pushEvent(rangeID, event{syncC: ch})
	// TODO: handle shutdown?
	<-ch
}

func (s *writerShard) writerLoop(ctx context.Context) {
	var recycled []event
	var rangeIDs []roachpb.RangeID
	workByRangeID := make(map[roachpb.RangeID]work)
	for {
		events, ok := s.waitForEvents(recycled)
		if !ok {
			return
		}

		appends, _ := s.splitEvents(events)
		s.prepareAppends(appends, &rangeIDs, workByRangeID)
		batch := s.w.eng.NewUnindexedBatch(false /* writeOnly */)
		s.stageAppends(ctx, rangeIDs, workByRangeID, batch)

		err := batch.CommitNoSyncWait()
		if err != nil {
			panic(err)
		}

		s.pushSyncBatch(batch, events)

		// Recycle data structures.
		recycled = nil // events[:0]
		rangeIDs = rangeIDs[:0]
		for i := range workByRangeID {
			delete(workByRangeID, i)
		}
	}
}

func (s *writerShard) waitForEvents(recycled []event) ([]event, bool) {
	s.eventsMu.Lock()
	defer s.eventsMu.Unlock()
	for {
		if s.w.isStopped() {
			return nil, false
		}
		if len(s.events) > 0 {
			events := s.events
			s.events = recycled
			return events, true
		}
		s.eventsCond.Wait()
	}
}

func (s *writerShard) splitEvents(events []event) (appends, syncs []event) {
	// Stable sort, append up front, by range ID, then sync.
	sort.SliceStable(events, func(i, j int) bool {
		// Sync events sort last.
		if events[i].syncC != nil {
			return false
		}
		if events[j].syncC != nil {
			return true
		}

		// Append events sort by range ID.
		return events[i].app.rangeID < events[j].app.rangeID
	})
	i := sort.Search(len(events), func(i int) bool {
		return events[i].syncC != nil
	})
	return events[:i], events[i:]
}

type work struct {
	entSlices []event
	meta      RaftLogMetadata
}

func (s *writerShard) prepareAppends(
	appends []event, rangeIDs *[]roachpb.RangeID, workByRangeID map[roachpb.RangeID]work,
) {
	for i := 0; i < len(appends); {
		rangeID := appends[i].app.rangeID
		j := i + 1
		for j < len(appends) {
			if appends[j].app.rangeID != rangeID {
				break
			}
			j++
		}
		*rangeIDs = append(*rangeIDs, rangeID)
		workByRangeID[rangeID] = work{
			entSlices: appends[i:j],
			meta:      appends[i].app.rr.GetRaftLogMetadata(),
		}
		i = j
	}

	// Remove duplicate entries. Entries with an index from later messages replace
	// entries with the same index from earlier messages.
	for _, rangeID := range *rangeIDs {
		entrySlices := workByRangeID[rangeID].entSlices
		for i := len(entrySlices) - 1; i > 0; i-- {
			laterEnts := entrySlices[i].app.msg.Entries
			if len(laterEnts) == 0 {
				continue
			}
			for j := i - 1; j >= 0; j-- {
				earlierEnts := entrySlices[j].app.msg.Entries
				if len(earlierEnts) == 0 {
					continue
				}
				idxOffset := int(laterEnts[0].Index) - int(earlierEnts[0].Index)
				if idxOffset <= 0 {
					entrySlices[j].app.msg.Entries = nil
					continue
				}
				if idxOffset < len(earlierEnts) {
					entrySlices[j].app.msg.Entries = entrySlices[j].app.msg.Entries[:idxOffset]
				}
				break
			}
		}
	}
}

func (s *writerShard) stageAppends(
	ctx context.Context,
	rangeIDs []roachpb.RangeID,
	workByRangeID map[roachpb.RangeID]work,
	batch storage.Batch,
) {
	for _, rangeID := range rangeIDs {
		rangeWork := workByRangeID[rangeID]
		for _, app := range rangeWork.entSlices {
			var err error
			rangeWork.meta, err = s.processPreAppend(ctx, &app.app, rangeWork.meta, batch)
			if err != nil {
				panic(err)
			}
		}
		rangeWork.entSlices[0].app.rr.SetRaftLogMetadata(rangeWork.meta)
	}
}

func (s *writerShard) processPreAppend(
	ctx context.Context, app *appendEvent, meta RaftLogMetadata, batch storage.Batch,
) (RaftLogMetadata, error) {
	if app.msg.HardState != nil {
		if err := app.rr.StateLoader().SetHardState(ctx, batch, *app.msg.HardState); err != nil {
			return RaftLogMetadata{}, err
		}
	}

	if app.msg.Snapshot != nil {
		log.Fatalf(ctx, "unhandled")
	}

	thinEntries, sideLoadedSize, err := app.rr.MaybeSideloadEntries(ctx, app.msg.Entries)
	if err != nil {
		return RaftLogMetadata{}, err
	}
	meta.LogSize += sideLoadedSize

	meta.LastIndex, meta.LastTerm, meta.LogSize, err = appendEntries(
		ctx, batch, app.rr.StateLoader(), meta.LastIndex, meta.LastTerm, meta.LogSize, thinEntries)
	if err != nil {
		return RaftLogMetadata{}, err
	}

	// Update raft log entry cache. We clear any older, uncommitted log entries
	// and cache the latest ones.
	s.w.cache.Add(app.rangeID, app.msg.Entries, true /* truncate */)

	return meta, nil
}

// TODO: how does this work?
//func (w *Writer) processPostAppend(app *appendEvent) {
//	//// We may have just overwritten parts of the log which contain
//	//// sideloaded SSTables from a previous term (and perhaps discarded some
//	//// entries that we didn't overwrite). Remove any such leftover on-disk
//	//// payloads (we can do that now because we've committed the deletion
//	//// just above).
//	//firstPurge := app.entries[0].Index // first new entry written
//	//purgeTerm := app.entries[0].Term - 1
//	//lastPurge := prevLastIndex // old end of the log, include in deletion
//	//purgedSize, err := app.rr.MaybePurgeSideloaded(ctx, firstPurge, lastPurge, purgeTerm)
//	//if err != nil {
//	//	return RaftLogMetadata{}, err
//	//}
//	//meta.LogSize -= purgedSize
//
//	// Update raft log entry cache. We clear any older, uncommitted log entries
//	// and cache the latest ones.
//	w.cache.Add(app.rangeID, app.entries, true /* truncate */)
//
//	return
//}

func (s *writerShard) pushSyncBatch(wg syncWaiter, events []event) {
	s.syncQueueMu.Lock()
	wasEmpty := len(s.syncQueue) == 0
	s.syncQueue = append(s.syncQueue, syncBatch{wg, events, timeutil.Now()})
	s.syncQueueMu.Unlock()
	if wasEmpty {
		s.syncQueueCond.Signal()
	}
}

func (s *writerShard) waitForSyncBatch() (syncBatch, bool) {
	s.syncQueueMu.Lock()
	defer s.syncQueueMu.Unlock()
	for {
		if s.w.isStopped() {
			return syncBatch{}, false
		}
		if len(s.syncQueue) > 0 {
			sync := s.syncQueue[0]
			s.syncQueue = s.syncQueue[1:]
			return sync, true
		}
		s.syncQueueCond.Wait()
	}
}

func (s *writerShard) publisherLoop(ctx context.Context) {
	for {
		sync, ok := s.waitForSyncBatch()
		if !ok {
			return
		}
		err := sync.wg.SyncWait()
		if err != nil {
			log.Fatalf(ctx, "%+v", err)
		}
		sync.wg.Close()
		durSync := timeutil.Since(sync.start)
		s.w.syncLatency.RecordValue(durSync.Nanoseconds())

		for _, ev := range sync.events {
			switch {
			case ev.syncC != nil:
				close(ev.syncC)
			case ev.app.rr != nil:
				dur := timeutil.Since(ev.app.start)
				s.w.e2eLatency.RecordValue(dur.Nanoseconds())
				ev.app.rr.SendAppendResponseMsgs(ctx, ev.app.msg.Responses)
			default:
				panic("unexpected")
			}
		}
	}
}

func (w *Writer) stop() {
	w.stopped.Store(true)
	for i := range w.shards {
		w.shards[i].eventsCond.Signal()
		w.shards[i].syncQueueCond.Signal()
	}
}

func (w *Writer) isStopped() bool {
	return w.stopped.Load()
}

type RaftLogMetadata struct {
	LastIndex uint64
	LastTerm  uint64
	LogSize   int64
}

// RaftRange is a handle to a Replica.
type RaftRange interface {
	StateLoader() stateloader.StateLoader
	MaybeSideloadEntries(context.Context, []raftpb.Entry) (_ []raftpb.Entry, sideloadedEntriesSize int64, _ error)
	//MaybePurgeSideloaded(_ context.Context, firstIndex, LastIndex, term uint64) (size int64, _ error)
	GetRaftLogMetadata() RaftLogMetadata
	SetRaftLogMetadata(RaftLogMetadata)
	SendAppendResponseMsgs(context.Context, []raftpb.Message)
}

// RaftEntryCache is a specialized data structure for storing deserialized
// raftpb.Entry values tailored to the access patterns of the storage package.
type RaftEntryCache interface {
	Add(id roachpb.RangeID, ents []raftpb.Entry, truncate bool)
}

// append the given entries to the raft log. Takes the previous values of
// r.mu.LastIndex, r.mu.LastTerm, and r.mu.LogSize, and returns new values.
// We do this rather than modifying them directly because these modifications
// need to be atomic with the commit of the batch. This method requires that
// r.raftMu is held.
//
// append is intentionally oblivious to the existence of sideloaded proposals.
// They are managed by the caller, including cleaning up obsolete on-disk
// payloads in case the log tail is replaced.
func appendEntries(
	ctx context.Context,
	batch storage.Batch,
	stateLoader stateloader.StateLoader,
	prevLastIndex uint64,
	prevLastTerm uint64,
	prevRaftLogSize int64,
	entries []raftpb.Entry,
) (uint64, uint64, int64, error) {
	if len(entries) == 0 {
		return prevLastIndex, prevLastTerm, prevRaftLogSize, nil
	}
	prefix := stateLoader.RaftLogPrefix()
	var diff enginepb.MVCCStats
	var value roachpb.Value
	for i := range entries {
		ent := &entries[i]
		key := keys.RaftLogKeyFromPrefix(prefix, ent.Index)

		if err := value.SetProto(ent); err != nil {
			return 0, 0, 0, err
		}
		value.InitChecksum(key)
		var err error
		if ent.Index > prevLastIndex {
			err = storage.MVCCBlindPut(ctx, batch, &diff, key, hlc.Timestamp{}, hlc.ClockTimestamp{}, value, nil /* txn */)
		} else {
			err = storage.MVCCPut(ctx, batch, &diff, key, hlc.Timestamp{}, hlc.ClockTimestamp{}, value, nil /* txn */)
		}
		if err != nil {
			return 0, 0, 0, err
		}
	}

	lastIndex := entries[len(entries)-1].Index
	lastTerm := entries[len(entries)-1].Term
	// Delete any previously appended log entries which never committed.
	if prevLastIndex > 0 {
		for i := lastIndex + 1; i <= prevLastIndex; i++ {
			// Note that the caller is in charge of deleting any sideloaded payloads
			// (which they must only do *after* the batch has committed).
			key := keys.RaftLogKeyFromPrefix(prefix, i)
			_, err := storage.MVCCDelete(ctx, batch, &diff, key, hlc.Timestamp{}, hlc.ClockTimestamp{}, nil)
			if err != nil {
				return 0, 0, 0, err
			}
		}
	}

	raftLogSize := prevRaftLogSize + diff.SysBytes
	return lastIndex, lastTerm, raftLogSize, nil
}
