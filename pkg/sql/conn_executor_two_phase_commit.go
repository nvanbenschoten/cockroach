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

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/fsm"
)

// execPrepareTransactionInOpenState runs a PREPARE TRANSACTION statement inside
// an open txn.
func (ex *connExecutor) execPrepareTransactionInOpenState(
	ctx context.Context, s *tree.PrepareTransaction, res RestrictedCommandResult,
) (fsm.Event, fsm.EventPayload) {
	//if !txnState.TxnIsOpen() {
	//	panic(fmt.Sprintf("prepareXATransaction called on non-open txn: %+v", txnState.mu.txn))
	//}
	//if err := func() error {
	//	// Prepare the transaction.
	//	if err := txnState.mu.txn.Prepare(txnState.Ctx); err != nil {
	//		return err
	//	}
	//	// Create a separate transaction to insert the prepared_xact entry.
	//	internalExecutor := InternalExecutor{ExecCfg: &e.cfg}
	//	xactTxn := client.NewTxn(e.cfg.DB, e.cfg.NodeID.Get(), client.RootTxn)
	//	// Marshal the bytes from the current transaction's proto.
	//	proto := txnState.mu.txn.Proto()
	//	txnBytes, err := protoutil.Marshal(proto)
	//	if err != nil {
	//		return err
	//	}
	//	if _, err = internalExecutor.ExecuteStatementInTransaction(
	//		txnState.Ctx,
	//		"insert-prepared-xact",
	//		xactTxn,
	//		`INSERT INTO system.prepared_xacts VALUES ($1, $2, $3, NOW(), $4, $5, '')`,
	//		txnName,
	//		proto.ID.GetBytes(),
	//		txnBytes,
	//		session.data.User,
	//		session.data.Database,
	//	); err != nil {
	//		return err
	//	}
	//	return xactTxn.Commit(txnState.Ctx)
	//}(); err != nil {
	//	// Rollback the possibly-prepared transaction to avoid dangling state.
	//	if rbErr := txnState.mu.txn.Rollback(txnState.Ctx); rbErr != nil {
	//		log.Warningf(txnState.Ctx, "unable to rollback prepared transaction: %s", rbErr)
	//	}
	//	return stateTransition{
	//		err:         errors.Wrapf(err, "unable to prepare transaction as %q", txnName),
	//		targetState: Aborted,
	//	}
	//}
	//
	//// We're done with this txn.
	//transition := stateTransition{targetState: NoTxn}
	//res.BeginResult((*tree.PrepareTransaction)(nil))
	//if err := res.CloseResult(); err != nil {
	//	transition = stateTransition{
	//		err:         err,
	//		targetState: Aborted,
	//	}
	//}
	//return transition

	return eventNonRetriableErr{}, nil
}
