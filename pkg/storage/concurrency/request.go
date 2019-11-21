// Copyright 2019 The Cockroach Authors.
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

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/engine/enginepb"
	"github.com/cockroachdb/cockroach/pkg/storage/spanset"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
)

// RequestImpl is an implementation of Request.
// TODO(WIP): this is just to support the prototype.
type RequestImpl struct {
	Ctx    context.Context
	Spans  *spanset.SpanSet
	Ts     hlc.Timestamp
	Txn    enginepb.TxnMeta
	Header roachpb.Header
}

func (r *RequestImpl) context() context.Context {
	return r.Ctx
}

func (r *RequestImpl) txn() uuid.UUID {
	return r.Txn.ID
}

func (r *RequestImpl) txnMeta() enginepb.TxnMeta {
	return r.Txn
}

func (r *RequestImpl) spans() *spanset.SpanSet {
	return r.Spans
}

func (r *RequestImpl) ts() hlc.Timestamp {
	return r.Ts
}

func (r *RequestImpl) header() roachpb.Header {
	return r.Header
}

// RequestGuard is returned from Manager.SequenceRequest.
type RequestGuard struct {
	req  Request
	lg   latchGuard
	wqgs []lockWaitQueueGuard
}

func (g *RequestGuard) moveLatchGuard() latchGuard {
	lg := g.lg
	g.lg = nil
	return lg
}

func (g *RequestGuard) moveWaitQueueGuards() []lockWaitQueueGuard {
	wqgs := g.wqgs
	g.wqgs = nil
	return wqgs
}
