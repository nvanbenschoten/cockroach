// Copyright 2014 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package batcheval

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/engine/enginepb"
	"github.com/cockroachdb/cockroach/pkg/storage/spanset"
)

// DefaultDeclareKeys is the default implementation of Command.DeclareKeys.
func DefaultDeclareKeys(
	_ *roachpb.RangeDescriptor,
	header roachpb.Header,
	req roachpb.Request,
	latchSpans, _ *spanset.SpanSet,
) {
	var access spanset.SpanAccess
	if roachpb.IsReadOnly(req) {
		access = spanset.SpanReadOnly
	} else {
		access = spanset.SpanReadWrite
	}
	latchSpans.AddMVCC(access, req.Header().Span(), header.Timestamp)
}

// DefaultDeclareIsolatedKeys is the default implementation of Command.DeclareKeys.
func DefaultDeclareIsolatedKeys(
	_ *roachpb.RangeDescriptor,
	header roachpb.Header,
	req roachpb.Request,
	latchSpans, lockSpans *spanset.SpanSet,
) {
	var access spanset.SpanAccess
	if roachpb.IsReadOnly(req) {
		access = spanset.SpanReadOnly
	} else {
		access = spanset.SpanReadWrite
	}
	latchSpans.AddMVCC(access, req.Header().Span(), header.Timestamp)
	lockSpans.AddMVCC(access, req.Header().Span(), header.Timestamp)
}

// DeclareKeysForBatch adds all keys that the batch with the provided header
// touches to the given SpanSet. This does not include keys touched during the
// processing of the batch's individual commands.
func DeclareKeysForBatch(
	desc *roachpb.RangeDescriptor, header roachpb.Header, latchSpans *spanset.SpanSet,
) {
	if header.Txn != nil {
		header.Txn.AssertInitialized(context.TODO())
		latchSpans.AddNonMVCC(spanset.SpanReadOnly, roachpb.Span{
			Key: keys.AbortSpanKey(header.RangeID, header.Txn.ID),
		})
	}
	if header.ReturnRangeInfo {
		latchSpans.AddNonMVCC(spanset.SpanReadOnly, roachpb.Span{Key: keys.RangeLeaseKey(header.RangeID)})
		latchSpans.AddNonMVCC(spanset.SpanReadOnly, roachpb.Span{Key: keys.RangeDescriptorKey(desc.StartKey)})
	}
}

// CommandArgs contains all the arguments to a command.
// TODO(bdarnell): consider merging with storagebase.FilterArgs (which
// would probably require removing the EvalCtx field due to import order
// constraints).
type CommandArgs struct {
	EvalCtx EvalContext
	Header  roachpb.Header
	Args    roachpb.Request

	// If MaxKeys is non-zero, span requests should limit themselves to
	// that many keys. Commands using this feature should also set
	// NumKeys and ResumeSpan in their responses.
	MaxKeys int64

	// *Stats should be mutated to reflect any writes made by the command.
	Stats *enginepb.MVCCStats
}
