// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sql

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/util/fsm"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/errors"
)

// execPrepareTransactionInOpenState runs a PREPARE TRANSACTION statement inside
// an open txn.
func (ex *connExecutor) execPrepareTransactionInOpenState(
	ctx context.Context, s *tree.PrepareTransaction, res RestrictedCommandResult,
) (fsm.Event, fsm.EventPayload) {
	ctx, sp := tracing.EnsureChildSpan(ctx, ex.server.cfg.AmbientCtx.Tracer, "prepare sql txn")
	defer sp.Finish()

	if err := ex.extraTxnState.sqlCursors.closeAll(cursorCloseForTxnCommit); err != nil {
		return ex.makeErrEvent(err, s)
	}

	ex.extraTxnState.prepStmtsNamespace.closeAllPortals(ctx, &ex.extraTxnState.prepStmtsNamespaceMemAcc)

	if ex.extraTxnState.descCollection.HasUncommittedDescriptors() {
		err := errors.Errorf("cannot prepare transaction: uncommitted descriptors")
		return ex.makeErrEvent(err, s)
	}

	txnPending := ex.state.mu.txn.TestingCloneTxn()
	if txnPending.Status != roachpb.PENDING {
		err := errors.AssertionFailedf("cannot prepare transaction: not in PENDING state")
		return ex.makeErrEvent(err, s)
	}

	globalID := s.Transaction.RawString()
	err := insertPreparedTransaction(
		ctx,
		ex.server.cfg.InternalDB.Executor(),
		globalID,
		txnPending,
		ex.sessionData().User().Normalized(),
		ex.sessionData().Database,
	)
	if err != nil {
		return ex.makeErrEvent(err, s)
	}

	if _, err := ex.state.mu.txn.Prepare(ctx); err != nil {
		return ex.makeErrEvent(err, s)
	}

	if err := ex.reportSessionDataChanges(func() error {
		ex.sessionDataStack.PopAll()
		return nil
	}); err != nil {
		return ex.makeErrEvent(err, s)
	}

	return eventTxnFinishPrepared{}, nil
}

func insertPreparedTransaction(
	ctx context.Context,
	ie isql.Executor,
	globalID string,
	txn *roachpb.Transaction,
	owner, database string,
) error {
	_, err := ie.ExecEx(
		ctx,
		"insert-prepared-transaction",
		nil, /* txn */
		sessiondata.NodeUserSessionDataOverride,
		`INSERT INTO system.prepared_transactions
     (global_id, transaction_id, transaction_key, prepared, owner, database)
		 VALUES ($1, $2, $3, NOW(), $4, $5)`,
		globalID,
		txn.ID,
		txn.Key,
		owner,
		database,
	)
	return err
}
