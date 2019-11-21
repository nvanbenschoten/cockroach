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
	RegisterCommand(roachpb.ResolveIntentRange, declareKeysResolveIntentRange, ResolveIntentRange)
}

func declareKeysResolveIntentRange(
	desc *roachpb.RangeDescriptor, header roachpb.Header, req roachpb.Request, spans *spanset.SpanSet,
) {
	declareKeysResolveIntentCombined(desc, header, req, spans)
}

// ResolveIntentRange resolves write intents in the specified
// key range according to the status of the transaction which created it.
func ResolveIntentRange(
	ctx context.Context, batch engine.ReadWriter, cArgs CommandArgs, resp roachpb.Response,
) (result.Result, error) {
	args := cArgs.Args.(*roachpb.ResolveIntentRangeRequest)
	h := cArgs.Header
	ms := cArgs.Stats

	if h.Txn != nil {
		return result.Result{}, ErrTransactionUnsupported
	}

	intent := roachpb.Intent{
		Span:   args.Span(),
		Txn:    args.IntentTxn,
		Status: args.Status,
	}
	cm := cArgs.EvalCtx.ConcurrencyManager()
	numKeys, resumeSpan, err := cm.RemoveLocks(ctx, batch, ms, intent, cArgs.MaxKeys, func(curKey engine.MVCCKey, intent roachpb.Intent) error {
		switch intent.Status {
		case roachpb.COMMITTED:
			// Rewrite the versioned value at the new timestamp.
			valBytes, err := batch.Get(curKey)
			if err != nil {
				return err
			}
			newKey := engine.MVCCKey{Key: curKey.Key, Timestamp: intent.Txn.WriteTimestamp}
			if err = batch.Put(newKey, valBytes); err != nil {
				return err
			}
			return batch.Clear(curKey)
		case roachpb.ABORTED:
			return batch.Clear(curKey)
		default:
			panic("unexpected")
		}
	})
	if err != nil {
		return result.Result{}, err
	}
	reply := resp.(*roachpb.ResolveIntentRangeResponse)
	reply.NumKeys = numKeys
	if resumeSpan != nil {
		reply.ResumeSpan = resumeSpan
		reply.ResumeReason = roachpb.RESUME_KEY_LIMIT
	}

	var res result.Result
	res.Local.Metrics = resolveToMetricType(args.Status, args.Poison)

	if WriteAbortSpanOnResolve(args.Status) {
		if err := SetAbortSpan(ctx, cArgs.EvalCtx, batch, ms, args.IntentTxn, args.Poison); err != nil {
			return result.Result{}, err
		}
	}
	return res, nil
}
