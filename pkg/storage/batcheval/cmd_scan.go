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
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/batcheval/result"
	"github.com/cockroachdb/cockroach/pkg/storage/concurrency/lock"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/storage/spanset"
)

func init() {
	RegisterReadOnlyCommand(roachpb.Scan, scanDeclareKeys, Scan)
}

func scanDeclareKeys(
	desc *roachpb.RangeDescriptor, header roachpb.Header, req roachpb.Request, spans *spanset.SpanSet,
) {
	scan := req.(*roachpb.ScanRequest)
	var access spanset.SpanAccess
	if scan.KeyLocking != lock.None {
		access = spanset.SpanReadWrite
	} else {
		access = spanset.SpanReadOnly
	}

	if keys.IsLocal(scan.Key) {
		spans.AddNonMVCC(access, scan.Span())
	} else {
		spans.AddMVCC(access, scan.Span(), header.Timestamp)
	}
}

// Scan scans the key range specified by start key through end key
// in ascending order up to some maximum number of results. maxKeys
// stores the number of scan results remaining for this batch
// (MaxInt64 for no limit).
func Scan(
	ctx context.Context, reader engine.Reader, cArgs CommandArgs, resp roachpb.Response,
) (result.Result, error) {
	args := cArgs.Args.(*roachpb.ScanRequest)
	h := cArgs.Header
	reply := resp.(*roachpb.ScanResponse)

	var failOnMoreRecent bool
	if args.KeyLocking != lock.None {
		failOnMoreRecent = true
	} else {
		failOnMoreRecent = false
	}

	var scanRes engine.MVCCScanResult
	var err error
	var res result.Result

	switch args.ScanFormat {
	case roachpb.BATCH_RESPONSE:
		scanRes, err = engine.MVCCScanToBytes(
			ctx, reader, args.Key, args.EndKey, cArgs.MaxKeys, h.Timestamp,
			engine.MVCCScanOptions{
				Inconsistent:     h.ReadConsistency != roachpb.CONSISTENT,
				Txn:              h.Txn,
				TargetBytes:      h.TargetBytes,
				FailOnMoreRecent: failOnMoreRecent,
			})
		if err != nil {
			return result.Result{}, err
		}
		reply.BatchResponses = scanRes.KVData
		if args.KeyLocking != lock.None && h.Txn != nil {
			res.Local.UpdatedIntents = make([]roachpb.Intent, scanRes.NumKeys)
			var i int
			if err := engine.MVCCScanDecodeKeyValues(scanRes.KVData, func(key engine.MVCCKey, _ []byte) error {
				res.Local.UpdatedIntents[i] = roachpb.Intent{
					Span: roachpb.Span{Key: key.Key}, Txn: h.Txn.TxnMeta, Status: roachpb.PENDING,
				}
				return nil
			}); err != nil {
				return result.Result{}, err
			}
		}
	case roachpb.KEY_VALUES:
		scanRes, err = engine.MVCCScan(
			ctx, reader, args.Key, args.EndKey, cArgs.MaxKeys, h.Timestamp, engine.MVCCScanOptions{
				Inconsistent:     h.ReadConsistency != roachpb.CONSISTENT,
				Txn:              h.Txn,
				TargetBytes:      h.TargetBytes,
				FailOnMoreRecent: failOnMoreRecent,
			})
		if err != nil {
			return result.Result{}, err
		}
		reply.Rows = scanRes.KVs
		if args.KeyLocking != lock.None && h.Txn != nil {
			res.Local.UpdatedIntents = make([]roachpb.Intent, scanRes.NumKeys)
			for i, row := range scanRes.KVs {
				res.Local.UpdatedIntents[i] = roachpb.Intent{
					Span: roachpb.Span{Key: row.Key}, Txn: h.Txn.TxnMeta, Status: roachpb.PENDING,
				}
			}
		}
	default:
		panic(fmt.Sprintf("Unknown scanFormat %d", args.ScanFormat))
	}

	reply.NumKeys = scanRes.NumKeys
	reply.NumBytes = scanRes.NumBytes

	if scanRes.ResumeSpan != nil {
		reply.ResumeSpan = scanRes.ResumeSpan
		reply.ResumeReason = roachpb.RESUME_KEY_LIMIT
	}

	if h.ReadConsistency == roachpb.READ_UNCOMMITTED {
		reply.IntentRows, err = CollectIntentRows(ctx, reader, cArgs, scanRes.Intents)
	}
	res.Local.EncounteredIntents = scanRes.Intents
	return res, err
}
