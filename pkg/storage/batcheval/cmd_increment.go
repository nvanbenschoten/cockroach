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
)

func init() {
	RegisterReadWriteCommand(roachpb.Increment, DefaultDeclareIsolatedKeys, Increment)
}

// Increment increments the value (interpreted as varint64 encoded) and
// returns the newly incremented value (encoded as varint64). If no value
// exists for the key, zero is incremented.
func Increment(
	ctx context.Context, readWriter engine.ReadWriter, cArgs CommandArgs, resp roachpb.Response,
) (result.Result, error) {
	args := cArgs.Args.(*roachpb.IncrementRequest)
	h := cArgs.Header
	reply := resp.(*roachpb.IncrementResponse)

	newVal, err := engine.MVCCIncrement(ctx, readWriter, cArgs.Stats, args.Key, h.Timestamp, h.Txn, args.Increment)
	if err != nil {
		return result.Result{}, err
	}
	reply.NewValue = newVal
	return result.FromWrittenIntent(h.Txn, args.Key), nil
}
