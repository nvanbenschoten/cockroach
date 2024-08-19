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
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/util/fsm"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/errors"
)

// maxPreparedTxnGlobalIDLen is the maximum length of a prepared transaction's
// global ID. Taken from Postgres, see GIDSIZE.
const maxPreparedTxnGlobalIDLen = 200

// execPrepareTransactionInOpenState runs a PREPARE TRANSACTION statement inside
// an open txn.
func (ex *connExecutor) execPrepareTransactionInOpenState(
	ctx context.Context, s *tree.PrepareTransaction,
) (fsm.Event, fsm.EventPayload) {
	err := ex.execPrepareTransactionInOpenStateInternal(ctx, s)

	var p fsm.EventPayload
	if err != nil {
		_ = ex.state.mu.txn.Rollback(ctx)
		p = eventTxnFinishPreparedErrPayload{err: err}
	}
	return eventTxnFinishPrepared{}, p
}

func (ex *connExecutor) execPrepareTransactionInOpenStateInternal(
	ctx context.Context, s *tree.PrepareTransaction,
) error {
	ctx, sp := tracing.EnsureChildSpan(ctx, ex.server.cfg.AmbientCtx.Tracer, "prepare sql txn")
	defer sp.Finish()

	if err := ex.extraTxnState.sqlCursors.closeAll(cursorCloseForTxnCommit); err != nil {
		return err
	}

	ex.extraTxnState.prepStmtsNamespace.closeAllPortals(ctx, &ex.extraTxnState.prepStmtsNamespaceMemAcc)

	if ex.extraTxnState.descCollection.HasUncommittedDescriptors() {
		return errors.Errorf("cannot prepare transaction: uncommitted descriptors")
	}

	txnPending := ex.state.mu.txn.TestingCloneTxn()
	if txnPending.Status != roachpb.PENDING {
		return errors.AssertionFailedf("cannot prepare transaction: not in PENDING state")
	}

	globalID := s.Transaction.RawString()
	if len(globalID) >= maxPreparedTxnGlobalIDLen {
		return pgerror.Newf(pgcode.InvalidParameterValue, "transaction identifier %q is too long", globalID)
	}

	if err := insertPreparedTransaction(
		ctx,
		ex.server.cfg.InternalDB.Executor(),
		globalID,
		txnPending,
		ex.sessionData().User().Normalized(),
		ex.sessionData().Database,
	); err != nil {
		if pgerror.GetPGCode(err) == pgcode.UniqueViolation {
			return pgerror.Newf(pgcode.DuplicateObject, "transaction identifier %q is already in use", globalID)
		}
		return err
	}

	if _, err := ex.state.mu.txn.Prepare(ctx); err != nil {
		return err
	}

	if err := ex.reportSessionDataChanges(func() error {
		ex.sessionDataStack.PopAll()
		return nil
	}); err != nil {
		return err
	}

	return nil
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
