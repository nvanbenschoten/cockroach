// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tracker

import (
	"github.com/cockroachdb/cockroach/pkg/raft/quorum"
	pb "github.com/cockroachdb/cockroach/pkg/raft/raftpb"
	"github.com/cockroachdb/cockroach/pkg/raft/raftstoreliveness"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
)

type SupportTracker struct {
	config        *quorum.Config
	storeLiveness raftstoreliveness.StoreLiveness

	// support contains a map of nodes which have supported the leader through
	// fortification handshakes, and the corresponding Store Liveness epochs that
	// they have supported the leader in.
	support map[pb.PeerID]pb.Epoch

	// leaderMaxSupported is the maximum store liveness expiration that the leader
	// has ever supported. The tracking here ensures that LeaderSupportUntil never
	// regresses for a raft group. The interesting cases are around config
	// changes.
	leaderMaxSupported hlc.Timestamp
}

// MakeSupportTracker initializes a SupportTracker.
func MakeSupportTracker(
	config *quorum.Config, storeLiveness raftstoreliveness.StoreLiveness,
) SupportTracker {
	st := SupportTracker{
		config:        config,
		support:       map[pb.PeerID]pb.Epoch{},
		storeLiveness: storeLiveness,
	}
	return st
}

// ResetSupport resets the support map.
func (st *SupportTracker) ResetSupport() {
	for id := range st.support {
		delete(st.support, id)
	}
}

// RecordSupport records that the node with the given id supported this Raft
// instance until the supplied timestamp.
func (st *SupportTracker) RecordSupport(id pb.PeerID, epoch pb.Epoch) {
	// The supported epoch should never regress. Guard against out of order
	// delivery of fortify responses by using max.
	st.support[id] = max(st.support[id], epoch)
}

func (st *SupportTracker) IsSupportedBy(id pb.PeerID) (pb.Epoch, bool) {
	fortifiedEpoch, found := st.support[id]
	return fortifiedEpoch, found
}

func (st *SupportTracker) LeadSupportUntil() hlc.Timestamp {
	if !st.storeLiveness.SupportFromEnabled() {
		return hlc.Timestamp{}
	}
	supportExpMap := make(map[pb.PeerID]hlc.Timestamp)
	for id, supportEpoch := range st.support {
		curEpoch, curExp, ok := st.storeLiveness.SupportFrom(id)
		// TODO(arul): we can't actually make this assertion, as a
		// MsgFortifyLeaderResp may beat a store liveness heartbeat back to the
		// leader.
		//if curEpoch < supportEpoch {
		//	panic("supported epoch shouldn't regress in store liveness")
		//}
		if ok && curEpoch == supportEpoch {
			supportExpMap[id] = curExp
		}
	}
	supportUntil := st.config.Voters.LeadSupportExpiration(supportExpMap)

	st.leaderMaxSupported.Forward(supportUntil)
	return st.leaderMaxSupported
}

// QuorumActive returns whether the leader has fortified support from a quorum
// majority of replicas.
func (st *SupportTracker) QuorumActive() bool {
	if !st.storeLiveness.SupportFromEnabled() {
		return true
	}
	supportedUntil := st.LeadSupportUntil()
	return !supportedUntil.IsEmpty() && !st.storeLiveness.SupportExpired(supportedUntil)
}
