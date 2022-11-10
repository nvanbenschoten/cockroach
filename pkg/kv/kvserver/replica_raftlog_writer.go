// Copyright 2022 The Cockroach Authors.
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

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/raftlog"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/stateloader"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"go.etcd.io/etcd/raft/v3/raftpb"
)

type replicaRaftLog Replica

var _ raftlog.RaftRange = &replicaRaftLog{}

func (r *replicaRaftLog) StateLoader() stateloader.StateLoader {
	return r.raftMu.logWriterStateLoader
}

func (r *replicaRaftLog) MaybeSideloadEntries(
	ctx context.Context, entries []raftpb.Entry,
) (_ []raftpb.Entry, sideloadedEntriesSize int64, _ error) {
	thinEntries, _, sideloadedEntriesSize, _, err := maybeSideloadEntriesImpl(ctx, entries, r.raftMu.sideloaded)
	return thinEntries, sideloadedEntriesSize, err
}

func (r *replicaRaftLog) GetRaftLogMetadata() raftlog.RaftLogMetadata {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return raftlog.RaftLogMetadata{
		LastIndex: r.mu.lastIndex,
		LastTerm:  r.mu.lastTerm,
		LogSize:   r.mu.raftLogSize,
	}
}

func (r *replicaRaftLog) SetRaftLogMetadata(meta raftlog.RaftLogMetadata) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.mu.lastIndex = meta.LastIndex
	r.mu.lastTerm = meta.LastTerm
	r.mu.raftLogSize = meta.LogSize
}

func (r *replicaRaftLog) SendAppendResponseMsgs(ctx context.Context, msgs []raftpb.Message) {
	// This should just be a call to sendRaftMessagesRaftMuLocked.
	local, remote := splitLocalMsgs(msgs, r.replicaID)
	if len(local) > 0 {
		r.localMsgs.Lock()
		wasEmpty := len(r.localMsgs.active) == 0
		r.localMsgs.active = append(r.localMsgs.active, local...)
		r.localMsgs.Unlock()
		if wasEmpty {
			r.store.enqueueRaftUpdateCheck(r.RangeID)
		}
	}
	if len(remote) > 0 {
		// TODO: we aren't holding RaftMu. Do we need to? It looks like I'm to blame
		// with 410ef29941f29dac1ccb8a81124a2dab579d7b7d.
		(*Replica)(r).sendRaftMessagesRaftMuLocked(ctx, remote, nil /* pausedFollowers */)
	}
}

func splitLocalMsgs(
	msgs []raftpb.Message, localID roachpb.ReplicaID,
) (localMsgs, remoteMsgs []raftpb.Message) {
	splitIdx := 0
	for i, msg := range msgs {
		if msg.To == uint64(localID) {
			msgs[i], msgs[splitIdx] = msgs[splitIdx], msgs[i]
			splitIdx++
		}
	}
	return msgs[:splitIdx], msgs[splitIdx:]
}
