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
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/errors"
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
	txnName string
	commit  bool
}

func (p *planner) endPreparedTxnNode(
	ctx context.Context, txnName *tree.StrVal, commit bool,
) (*endPreparedTxnNode, error) {
	// TODO(nvanbenschoten): privileges.

	return &endPreparedTxnNode{
		txnName: txnName.RawString(),
		commit:  commit,
	}, nil
}

func (f *endPreparedTxnNode) startExec(params runParams) error {
	if err := f.checkNoActiveTxn(params); err != nil {
		return err
	}

	//TODO implement me
	panic("implement me")
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
) (*roachpb.Transaction, string, error) {
	row, err := params.p.InternalSQLTxn().QueryRowEx(
		params.ctx,
		"select-prepared-txn",
		params.p.Txn(),
		sessiondata.NodeUserSessionDataOverride,
		`SELECT transaction, owner, status FROM system.prepared_transactions WHERE gid = $1`,
		f.txnName,
	)
	if err != nil {
		return nil, "", err
	}
	if row == nil {
		return nil, "", pgerror.Newf(pgcode.UndefinedObject,
			"prepared transaction with identifier %q does not exist", f.txnName)
	}

	var txn roachpb.Transaction
	if err := protoutil.Unmarshal(([]byte)(tree.MustBeDBytes(row[0])), &txn); err != nil {
		return nil, "", err
	}
	owner := tree.MustBeDString(row[1])
	status := tree.MustBeDString(row[2])
	if len(status) > 0 {
		return nil, "", errors.Errorf("prepared transaction %s in state %q", txn, status)
	}
	return &txn, string(owner), nil
}

// deletePreparedTxn deletes the prepared transaction from the system table.
func (f *endPreparedTxnNode) deletePreparedTxn(params runParams) error {
	_, err := params.p.InternalSQLTxn().ExecEx(
		params.ctx,
		"delete-prepared-txn",
		params.p.Txn(),
		sessiondata.NodeUserSessionDataOverride,
		`DELETE FROM system.prepared_transactions WHERE gid = $1`,
		f.txnName,
	)
	return err
}

func (f *endPreparedTxnNode) Next(params runParams) (bool, error) { return false, nil }
func (f *endPreparedTxnNode) Values() tree.Datums                 { return tree.Datums{} }
func (f *endPreparedTxnNode) Close(ctx context.Context)           {}
