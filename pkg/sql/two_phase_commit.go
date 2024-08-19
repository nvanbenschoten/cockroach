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
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
)

// CommitPrepared commits a previously prepared transaction and deletes its
// associated entry from the system.prepared_xacts table. This is called from
// COMMIT PREPARED.
func (p *planner) CommitPrepared(ctx context.Context, n *tree.CommitPrepared) (planNode, error) {
	return p.endPreparedTxnNode(ctx, n.Transaction, true /* commit */)
}

// RollbackPrepared aborts a previously prepared transaction and deletes its
// associated entry from the system.prepared_xacts table. This is called from
// ROLLBACK PREPARED.
func (p *planner) RollbackPrepared(
	ctx context.Context, n *tree.RollbackPrepared,
) (planNode, error) {
	return p.endPreparedTxnNode(ctx, n.Transaction, false /* commit */)
}

type endPreparedTxnNode struct {
	globalID string
	commit   bool
}

func (p *planner) endPreparedTxnNode(
	ctx context.Context, globalID *tree.StrVal, commit bool,
) (*endPreparedTxnNode, error) {
	// TODO(nvanbenschoten): privileges on tables.

	return &endPreparedTxnNode{
		globalID: globalID.RawString(),
		commit:   commit,
	}, nil
}

func (f *endPreparedTxnNode) startExec(params runParams) error {
	if err := f.checkNoActiveTxn(params); err != nil {
		return err
	}

	txnID, txnKey, owner, database, err := f.selectPreparedTxn(params)
	if err != nil {
		return err
	}

	// TODO: Priv check.
	_ = owner
	_ = database

	if txnKey != nil {
		if err := f.endPreparedTxn(params, txnID, txnKey); err != nil {
			return err
		}
	}

	return f.deletePreparedTxn(params)
}

func (f *endPreparedTxnNode) checkNoActiveTxn(params runParams) error {
	if params.p.autoCommit {
		return nil
	}
	stmt := "COMMIT PREPARED"
	if !f.commit {
		stmt = "ROLLBACK PREPARED"
	}
	return pgerror.Newf(pgcode.ActiveSQLTransaction,
		"%s cannot run inside a transaction block", stmt)
}

// selectPreparedTxn queries the prepared transaction from the system table and,
// if found, returns the transaction object and the owner.
func (f *endPreparedTxnNode) selectPreparedTxn(
	params runParams,
) (txnID uuid.UUID, txnKey roachpb.Key, owner, database string, err error) {
	row, err := params.p.QueryRowEx(
		params.ctx,
		"select-prepared-txn",
		sessiondata.NodeUserSessionDataOverride,
		`SELECT transaction_id, transaction_key, owner, database FROM system.prepared_transactions WHERE global_id = $1 FOR UPDATE`,
		f.globalID,
	)
	if err != nil {
		return uuid.UUID{}, nil, "", "", err
	}
	if row == nil {
		return uuid.UUID{}, nil, "", "", pgerror.Newf(pgcode.UndefinedObject,
			"prepared transaction with identifier %q does not exist", f.globalID)
	}

	txnID = tree.MustBeDUuid(row[0]).UUID
	if row[1] != tree.DNull {
		txnKey = roachpb.Key(tree.MustBeDBytes(row[1]))
	}
	owner = string(tree.MustBeDString(row[2]))
	database = string(tree.MustBeDString(row[3]))
	return txnID, txnKey, owner, database, nil
}

// endPreparedTxn ends the prepared transaction by either committing or rolling
// back the transaction.
func (f *endPreparedTxnNode) endPreparedTxn(
	params runParams, txnID uuid.UUID, txnKey roachpb.Key,
) error {
	db := params.ExecCfg().DB

	preparedTxn, err := db.QueryTxn(params.ctx, txnID, txnKey)
	if err != nil {
		return err
	}
	if preparedTxn == nil {
		// WIP: this isn't right. We still want to clean up the record.
		return pgerror.Newf(pgcode.UndefinedObject,
			"prepared transaction with identifier %q has not record", f.globalID)
	}
	if preparedTxn.Status != roachpb.PREPARED {
		// WIP: this isn't right. We still want to clean up the record.
		return pgerror.Newf(pgcode.UndefinedObject,
			"prepared transaction with identifier %q not in PREPARED state", f.globalID)
	}

	// WIP: hack to set batch timestamp.
	preparedTxn.ReadTimestamp = preparedTxn.WriteTimestamp

	if f.commit {
		err = db.CommitPrepared(params.ctx, preparedTxn)
	} else {
		err = db.RollbackPrepared(params.ctx, preparedTxn)
	}
	return err
}

// deletePreparedTxn deletes the prepared transaction from the system table.
func (f *endPreparedTxnNode) deletePreparedTxn(params runParams) error {
	_, err := params.p.ExecEx(
		params.ctx,
		"delete-prepared-txn",
		sessiondata.NodeUserSessionDataOverride,
		`DELETE FROM system.prepared_transactions WHERE global_id = $1`,
		f.globalID,
	)
	return err
}

func (f *endPreparedTxnNode) Next(params runParams) (bool, error) { return false, nil }
func (f *endPreparedTxnNode) Values() tree.Datums                 { return tree.Datums{} }
func (f *endPreparedTxnNode) Close(ctx context.Context)           {}
