// Copyright 2019 The Cockroach Authors.
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

package storage

import (
	"sync"
	"sync/atomic"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"go.etcd.io/etcd/raft"
	"go.etcd.io/etcd/raft/raftpb"
)

const proposalBufSize = 128

type proposalBuf struct {
	lock           *syncutil.RWMutex
	full           sync.Cond
	leaseIndexBase uint64

	// Buffer
	b [proposalBufSize]*ProposalData
	// Atomically accessed. Bit layout:
	//  bits 0  - 31: index into array b
	//  bits 32 - 63: lease index offset
	i proposalBufCnt
}

type proposalBufCnt uint64
type proposalBufReq uint64
type proposalBufRes uint64

func (c *proposalBufCnt) handle(r proposalBufReq) proposalBufRes {
	return proposalBufRes(atomic.AddUint64((*uint64)(c), uint64(r)))
}

func (c *proposalBufCnt) clear() proposalBufRes {
	return proposalBufRes(atomic.SwapUint64((*uint64)(c), 0))
}

func makeProposalBufReq(incLeaseIndex bool) proposalBufReq {
	toAdd := proposalBufReq(1)
	if incLeaseIndex {
		toAdd |= (1 << 32)
	}
	return toAdd
}

func (r proposalBufRes) index() uint64 {
	//
	return uint64(r&(1<<32-1) - 1)
}

func (r proposalBufRes) leaseIndexOffset() uint64 {
	return uint64(r >> 32)
}

func (b *proposalBuf) init(r *Replica) {
	b.lock = &r.mu.RWMutex
	b.full.L = b.lock.RLocker()
}

func (b *proposalBuf) empty() bool {
	return atomic.LoadUint64((*uint64)(&b.i)) > 0
}

func (b *proposalBuf) add(r *Replica, p *ProposalData, data []byte) uint64 {
	req := makeProposalBufReq(p.Request.IsLeaseRequest())
	r.mu.RLock()
	defer r.mu.RUnlock()
	var idx, leaseIdxOff uint64
	for {
		res := b.i.handle(req)
		idx = res.index()
		leaseIdxOff = res.leaseIndexOffset()
		if idx < uint64(len(b.b)) {
			break
		} else if idx == uint64(len(b.b)) {
			b.lock.RUnlock()
			b.flushProposalBuf(r)
			b.lock.RLock()
		} else {
			b.full.Wait()
		}
	}
	f := &p.tmpFooter
	base := b.leaseIndexBase
	if base < r.mu.state.LeaseAppliedIndex {
		base = r.mu.state.LeaseAppliedIndex
	}
	f.MaxLeaseIndex = base + leaseIdxOff
	p.command.MaxLeaseIndex = f.MaxLeaseIndex

	preLen := len(data)
	footerLen := f.Size()
	data = data[:preLen+footerLen]
	i, err := protoutil.MarshalToWithoutFuzzing(f, data[preLen:])
	if err != nil {
		panic(roachpb.NewError(err))
	}
	if i != footerLen {
		panic("huh?")
	}
	p.commandWithFooter = data

	b.b[idx] = p
	if isLease {
		return 0
	}
	return f.MaxLeaseIndex
}

func (b *proposalBuf) reAddLocked(r *Replica, p *ProposalData) {
	toAdd := uint64(1)

	var idx uint64
	for {
		res := atomic.AddUint64(&b.i, toAdd)
		idx = res&(1<<32-1) - 1
		if idx < uint64(len(b.b)) {
			break
		} else if idx == uint64(len(b.b)) {
			if err := r.withRaftGroupLocked(true, func(raftGroup *raft.RawNode) (bool, error) {
				b.flushProposalBufLocked(r, raftGroup)
				return false, nil
			}); err != nil {
				panic(err)
			}
		} else {
			b.full.Wait()
		}
	}
	b.b[idx] = p
}

func (b *proposalBuf) flushProposalBuf(r *Replica) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.mu.internalRaftGroup == nil {
		// Unlock first before locking in {raft,replica}mu order.
		r.mu.Unlock()

		r.raftMu.Lock()
		defer r.raftMu.Unlock()
		r.mu.Lock()
	}
	if err := r.withRaftGroupLocked(true, func(raftGroup *raft.RawNode) (bool, error) {
		b.flushProposalBufLocked(r, raftGroup)
		return false, nil
	}); err != nil {
		panic(err)
	}
}

func (b *proposalBuf) flushProposalBufLocked(r *Replica, raftGroup *raft.RawNode) {
	res := atomic.LoadUint64(&b.i)
	idx := int(res & (1<<32 - 1))
	if idx == 0 {
		return
	}
	if idx >= len(b.b) {
		idx = len(b.b)
		b.full.Broadcast()
	}

	lastAssignedLeaseIndex := b.b[idx-1].tmpFooter.MaxLeaseIndex
	b.leaseIndexBase = lastAssignedLeaseIndex
	r.mu.lastAssignedLeaseIndex = lastAssignedLeaseIndex
	atomic.StoreUint64(&b.i, 0)

	r.unquiesceLocked()
	buf := b.b[:idx]
	ents := make([]raftpb.Entry, len(buf))
	entsIdx := 0
	for i, prop := range buf {
		prop.proposedAtTicks = r.mu.ticks
		if _, ok := r.mu.proposals[prop.idKey]; !ok {
			r.mu.proposals[prop.idKey] = prop
			if r.mu.commandSizes != nil {
				r.mu.commandSizes[prop.idKey] = prop.proposalSize
			}
		}

		if crt := prop.command.ReplicatedEvalResult.ChangeReplicas; crt != nil {
			confChangeCtx := ConfChangeContext{
				CommandID: string(prop.idKey),
				Payload:   prop.commandWithFooter[raftCommandPrefixLen:], // chop off prefix
				Replica:   crt.Replica,
			}
			encodedCtx, err := protoutil.Marshal(&confChangeCtx)
			if err != nil {
				panic(err)
			}

			if err := raftGroup.ProposeConfChange(raftpb.ConfChange{
				Type:    changeTypeInternalToRaft[crt.ChangeType],
				NodeID:  uint64(crt.Replica.ReplicaID),
				Context: encodedCtx,
			}); err != nil && err != raft.ErrProposalDropped {
				panic(err)
			}
		} else {
			ents[entsIdx].Data = prop.commandWithFooter
			entsIdx++
		}
		buf[i] = nil
	}
	if entsIdx == 0 {
		return
	}
	if err := raftGroup.Step(raftpb.Message{
		Type:    raftpb.MsgProp,
		From:    uint64(r.mu.replicaID),
		Entries: ents[:entsIdx],
	}); err != nil && err != raft.ErrProposalDropped {
		panic(err)
	}
}
