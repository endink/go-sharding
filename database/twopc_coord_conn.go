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

package database

import (
	"context"
	"fmt"
	"github.com/XiaoMi/Gaea/core"
	"github.com/XiaoMi/Gaea/mysql/types"
	"github.com/XiaoMi/Gaea/telemetry"
	"go.opentelemetry.io/otel/label"
	"sync"
)

type coordConnImpl struct {
	executor *TxExecutor
}

func (cd *coordConnImpl) Close() {
	panic("implement me")
}

func (cd *coordConnImpl) ResolveTransaction(ctx context.Context, dtid string) error {
	transaction, err := cd.executor.ReadTransaction(ctx, dtid)
	if err != nil {
		return err
	}

	if transaction == nil || transaction.Dtid == "" {
		// It was already resolved.
		return nil
	}

	mmShard, err := dbSession(dtid)
	if err != nil {
		return err
	}

	switch transaction.State {
	case TransactionStatePrepare:
		// If state is PREPARE, make a decision to rollback and
		// fallthrough to the rollback workflow.
		if err = cd.setRollback(ctx, mmShard.Target, transaction.Dtid, mmShard.TransactionId); err != nil {
			return err
		}
		fallthrough
	case TransactionStateRollback:
		if err = cd.rollbackPrepared(ctx, mmShard.Target, transaction.Dtid, 0); err != nil {
			return err
		}
	case TransactionStateCommit:
		if err = cd.resumeCommit(ctx, mmShard.Target, transaction.Dtid); err != nil {
			return err
		}
	default:
		// never happen.
		return fmt.Errorf("invalid state: %v", transaction.State)
	}
	return nil
}

// CommitPrepared is part of the queryservice.QueryServer interface
func (cd *coordConnImpl) resumeCommit(ctx context.Context, target *Target, dtid string) error {
	return cd.exec(
		ctx,
		"CommitPrepared", "commit_prepared", nil,
		target, nil,
		func(c context.Context) error {
			return cd.executor.CommitPrepared(c, dtid)
		},
	)
}

// RollbackPrepared commits the prepared transaction.
func (cd *coordConnImpl) rollbackPrepared(ctx context.Context, target *Target, dtid string, originalID int64) (err error) {
	return cd.exec(
		ctx,
		"RollbackPrepared", "rollback_prepared", nil,
		target, nil,
		func(c context.Context) error {
			return cd.executor.RollbackPrepared(c, dtid, originalID)
		},
	)
}

// SetRollback transitions the 2pc transaction to the Rollback state.
// If a transaction id is provided, that transaction is also rolled back.
func (cd *coordConnImpl) setRollback(ctx context.Context, target *Target, dtid string, transactionID int64) (err error) {
	return cd.exec(
		ctx,
		"SetRollback", "set_rollback", nil,
		target, nil, /* allowOnShutdown */
		func(c context.Context) error {
			return cd.executor.SetRollback(c, dtid, transactionID)
		},
	)
}

func (cd *coordConnImpl) exec(
	c context.Context,
	actionName, sql string,
	bindVariables map[string]*types.BindVariable,
	target *Target,
	options *types.ExecuteOptions,
	exec func(ctx context.Context) error,
) (err error) {
	ctx, span := telemetry.GlobalTracer.Start(c, "TxCoord"+actionName)
	if options != nil {
		span.SetAttributes(label.String("isolation-level", options.TransactionIsolation.String()))
	}
	span.SetAttributes(label.String("sql", sql))
	if target != nil {
		span.SetAttributes(label.String("schema", target.Schema))
		span.SetAttributes(label.String("datasource", target.DataSource))
	}
	defer func() {
		defer span.End()
		defer RecoverError(log, ctx)
	}()
	err = exec(ctx)
	if err != nil {
		return sqlError(ctx, sql, bindVariables, err)
	}
	return nil
}

// runTargets executes the action for all targets in parallel and returns a consolildated error.
// Flow is identical to runSessions.
func (cd *coordConnImpl) runTargets(targets []*Target, action func(*Target) error) error {
	if len(targets) == 1 {
		return action(targets[0])
	}
	allErrors := new(core.AllErrorRecorder)
	var wg sync.WaitGroup
	for _, t := range targets {
		wg.Add(1)
		go func(t *Target) {
			defer wg.Done()
			if err := action(t); err != nil {
				allErrors.RecordError(err)
			}
		}(t)
	}
	wg.Wait()
	return allErrors.Error()
}
