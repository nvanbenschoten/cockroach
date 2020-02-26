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
	RegisterReadWriteCommand(roachpb.InitPut, DefaultDeclareIsolatedKeys, InitPut)
}

// InitPut sets the value for a specified key only if it doesn't exist. It
// returns a ConditionFailedError if the key exists with an existing value that
// is different from the value provided. If FailOnTombstone is set to true,
// tombstones count as mismatched values and will cause a ConditionFailedError.
func InitPut(
	ctx context.Context, readWriter engine.ReadWriter, cArgs CommandArgs, resp roachpb.Response,
) (result.Result, error) {
	args := cArgs.Args.(*roachpb.InitPutRequest)
	h := cArgs.Header

	if h.DistinctSpans {
		if b, ok := readWriter.(engine.Batch); ok {
			// Use the distinct batch for both blind and normal ops so that we don't
			// accidentally flush mutations to make them visible to the distinct
			// batch.
			readWriter = b.Distinct()
			defer readWriter.Close()
		}
	}
	var err error
	if args.Blind {
		err = engine.MVCCBlindInitPut(ctx, readWriter, cArgs.Stats, args.Key, h.Timestamp, args.Value, args.FailOnTombstones, h.Txn)
	} else {
		err = engine.MVCCInitPut(ctx, readWriter, cArgs.Stats, args.Key, h.Timestamp, args.Value, args.FailOnTombstones, h.Txn)
	}
	if err != nil {
		return result.Result{}, err
	}
	return result.FromWrittenIntent(h.Txn, args.Key), nil
}
