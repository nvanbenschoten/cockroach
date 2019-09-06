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

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/batcheval/result"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/storage/spanset"
)

func init() {
	RegisterCommand(roachpb.ForwardHLC, declareKeysForwardHLC, ForwardHLC)
}

func declareKeysForwardHLC(
	_ *roachpb.RangeDescriptor, _ roachpb.Header, _ roachpb.Request, _ *spanset.SpanSet,
) {
	// ForwardHLC declares no keys, so it does not synchronize with any other
	// requests other than lease transfers, which are synchronized outside of
	// the latching system.
}

// ForwardHLC is a no-op. Evaluation of the request does nothing,
// but it can be used to enforce causality through the HLC update
// performed when it is routed to the leaseholder(s) for the
// Range(s) it is addressed to.
func ForwardHLC(
	ctx context.Context, batch engine.ReadWriter, cArgs CommandArgs, resp roachpb.Response,
) (result.Result, error) {
	// The local HLC clock was already forwarded and the fact that we were able
	// to evalute this request proves that this is the leaseholder. There's
	// nothing else to do.
	return result.Result{}, nil
}
