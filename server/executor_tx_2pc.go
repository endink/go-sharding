/*
 * Copyright 2021. Go-Sharding Author All Rights Reserved.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 *  File author: Anders Xiao
 */

package server

import (
	"context"
	"errors"
	"fmt"
	"github.com/XiaoMi/Gaea/database"
	"github.com/XiaoMi/Gaea/mysql/types"
	"github.com/XiaoMi/Gaea/telemetry"
	"go.opentelemetry.io/otel/label"
)

func (ec *Executor) ResolveTransaction(ctx context.Context, dtid string) error {
	return ec.runTargets(ctx, ec.topology.GetAllTargets(), func(c context.Context, t *database.Target) error {
		engine, err := ec.topology.GetTxEngine(t)
		if err != nil {
			return err
		}
		transaction, err := engine.ReadTransaction(ctx, dtid)
		if err != nil {
			return err
		}

		if transaction == nil || transaction.Dtid == "" {
			// It was already resolved.
			return nil
		}

		mmShard, err := database.NewDbSession(dtid)
		if err != nil {
			return err
		}

		switch transaction.State {
		case database.TransactionStatePrepare:
			// If state is PREPARE, make a decision to rollback and
			// fallthrough to the rollback workflow.
			if err = ec.Complete(ctx, mmShard.Target, transaction.Dtid, mmShard.TransactionId); err != nil {
				return err
			}
			fallthrough
		case database.TransactionStateRollback:
			if err = ec.resumeRollback(ctx, mmShard.Target, transaction); err != nil {
				return err
			}
		case database.TransactionStateCommit:
			if err = ec.resumeCommit(ctx, mmShard.Target, transaction); err != nil {
				return err
			}
		default:
			// never happen.
			return fmt.Errorf("invalid state: %v", transaction.State)
		}
		return nil
	})
}

func (ec *Executor) resumeRollback(ctx context.Context, target *database.Target, transaction *database.TransactionMetadata) error {
	err := ec.runTargets(ctx, transaction.Participants, func(c context.Context, t *database.Target) error {
		return ec.RollbackPrepared(c, t, transaction.Dtid, 0)
	})
	if err != nil {
		return err
	}
	return ec.ConcludeTransaction(ctx, target, transaction.Dtid)
}

func (ec *Executor) resumeCommit(ctx context.Context, target *database.Target, transaction *database.TransactionMetadata) error {
	err := ec.runTargets(ctx, transaction.Participants, func(c context.Context, t *database.Target) error {
		return ec.CommitPrepared(ctx, t, transaction.Dtid)
	})
	if err != nil {
		return err
	}
	return ec.ConcludeTransaction(ctx, target, transaction.Dtid)
}

// CommitPrepared is part of the queryservice.QueryServer interface
func (ec *Executor) CommitPrepared(ctx context.Context, target *database.Target, dtid string) error {
	return ec.exec(
		ctx,
		"CommitPrepared", "commit_prepared", nil,
		target, nil,
		func(e executionContext) error {
			return e.te.CommitPrepared(e.context, dtid)
		},
	)
}

// RollbackPrepared commits the prepared transaction.
func (ec *Executor) RollbackPrepared(ctx context.Context, target *database.Target, dtid string, originalID int64) error {
	return ec.exec(
		ctx,
		"RollbackPrepared", "rollback_prepared", nil,
		target, nil,
		func(e executionContext) error {
			return e.te.RollbackPrepared(e.context, dtid, originalID)
		},
	)
}

// Complete transitions the 2pc transaction to the Rollback state.
// If a transaction id is provided, that transaction is also rolled back.
func (ec *Executor) Complete(ctx context.Context, target *database.Target, dtid string, transactionID int64) (err error) {
	return ec.exec(
		ctx,
		"Complete", "set_rollback", nil,
		target, nil, /* allowOnShutdown */
		func(e executionContext) error {
			return e.te.Complete(e.context, dtid, transactionID)
		},
	)
}

func (ec *Executor) ConcludeTransaction(ctx context.Context, target *database.Target, dtid string) (err error) {
	return ec.exec(
		ctx,
		"ConcludeTransaction", "conclude_transaction", nil,
		target, nil,
		func(e executionContext) error {
			return e.te.ConcludeTransaction(e.context, dtid)
		},
	)
}

// StartCommit atomically commits the transaction along with the
// decision to commit the associated 2pc transaction.
func (ec *Executor) StartCommit(ctx context.Context, target *database.Target, transactionID int64, dtid string) (err error) {
	return ec.exec(
		ctx,
		"StartCommit", "start_commit", nil,
		target, nil,
		func(e executionContext) error {
			return e.te.StartCommit(e.context, transactionID, dtid)
		},
	)
}

// ReadTransaction returns the metadata for the specified dtid.
func (ec *Executor) ReadTransaction(ctx context.Context, target *database.Target, dtid string) (metadata *database.TransactionMetadata, err error) {
	err = ec.exec(
		ctx,
		"ReadTransaction", "read_transaction", nil,
		target, nil,
		func(e executionContext) error {
			metadata, err = e.te.ReadTransaction(ctx, dtid)
			return err
		},
	)
	return metadata, err
}

// Prepare prepares the specified transaction.
func (ec *Executor) Prepare(ctx context.Context, target *database.Target, transactionID int64, dtid string) (err error) {
	return ec.exec(
		ctx,
		"Prepare", "prepare", nil,
		target, nil,
		func(e executionContext) error {
			return e.te.Prepare(ctx, transactionID, dtid)
		},
	)
}

// CreateTransaction creates the metadata for a 2PC transaction.
func (ec *Executor) CreateTransaction(ctx context.Context, target *database.Target, dtid string, participants []*database.Target) (err error) {
	return ec.exec(
		ctx,
		"CreateTransaction", "create_transaction", nil,
		target, nil,
		func(e executionContext) error {
			return e.te.CreateTransaction(ctx, dtid, participants)
		},
	)
}

// BeginExecute combines Begin and Execute.
func (ec *Executor) BeginExecute(ctx context.Context, target *database.Target, preQueries []string, sql string, bindVariables map[string]*types.BindVariable, reservedID int64, options *types.ExecuteOptions) (*types.Result, int64, *database.Target, error) {

	// Disable hot row protection in case of reserve connection.
	//if ec.enableHotRowProtection && reservedID == 0 {
	//	txDone, err := ec.beginWaitForSameRangeTransactions(ctx, target, options, sql, bindVariables)
	//	if err != nil {
	//		return nil, 0, nil, err
	//	}
	//	if txDone != nil {
	//		defer txDone()
	//	}
	//}

	transactionID, err := ec.begin(ctx, target, preQueries, reservedID, options)
	if err != nil {
		return nil, 0, nil, err
	}

	result, err := ec.Execute(ctx, target, sql, bindVariables, transactionID, reservedID, options)
	return result, transactionID, alias, err
}

// Execute executes the query and returns the result as response.
