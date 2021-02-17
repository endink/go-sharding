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
	"github.com/XiaoMi/Gaea/database"
	"github.com/XiaoMi/Gaea/mysql/types"
	"github.com/XiaoMi/Gaea/parser"
	"github.com/XiaoMi/Gaea/server/txserializer"
	"github.com/XiaoMi/Gaea/telemetry"
	"go.opentelemetry.io/otel/label"
)

func (ec *Executor) Execute(ctx context.Context, target *database.Target, sql string, bindVariables map[string]*types.BindVariable, transactionID, reservedID int64, options *types.ExecuteOptions) (result *types.Result, err error) {
	ctx, span := telemetry.GlobalTracer.Start(ctx, "Executor.Execute")
	span.SetAttributes(label.String("sql", sql))
	defer span.End()

	if transactionID != 0 && reservedID != 0 && transactionID != reservedID {
		return nil, errors.New("transactionID and reserveID must match if both are non-zero")
	}

	err = ec.exec(
		ctx,
		"Execute", sql, bindVariables,
		target, options,
		func(e executionContext) error {
			if bindVariables == nil {
				bindVariables = make(map[string]*types.BindVariable)
			}
			query, comments := parser.SplitMarginComments(sql)
			plan, err := ec.qe.GetPlan(ctx, logStats, query, skipQueryPlanCache(options), reservedID != 0)
			if err != nil {
				return err
			}
			// If both the values are non-zero then by design they are same value. So, it is safe to overwrite.
			connID := reservedID
			if transactionID != 0 {
				connID = transactionID
			}
			qre := &QueryExecutor{
				query:          query,
				marginComments: comments,
				bindVars:       bindVariables,
				connID:         connID,
				options:        options,
				plan:           plan,
				ctx:            ctx,
				executor:       ec,
				target:         target,
			}
			result, err = qre.Execute()
			if err != nil {
				return err
			}
			result = result.StripMetadata(types.IncludeFieldsOrDefault(options))

			// Change database name in mysql output to the keyspace name
			if types.IncludeFieldsOrDefault(options) == types.IncludeFieldsAll {
				for _, f := range result.Fields {
					if f.Database != "" {
						f.Database = ec.proxyDbName
					}
				}
			}
			return nil
		},
	)
	return result, err
}

// BeginExecute combines Begin and Execute.
func (ec *Executor) BeginExecute(ctx context.Context, target *database.Target, preQueries []string, sql string, bindVariables map[string]*types.BindVariable, reservedID int64, options *types.ExecuteOptions) (*types.Result, int64, error) {

	// Disable hot row protection in case of reserve connection.
	if ec.enableHotRowProtection && reservedID == 0 {
		txDone, err := ec.beginWaitForSameRangeTransactions(ctx, target, options, sql, bindVariables)
		if err != nil {
			return nil, 0, err
		}
		if txDone != nil {
			defer txDone()
		}
	}

	transactionID, err := ec.begin(ctx, target, preQueries, reservedID, options)
	if err != nil {
		return nil, 0, err
	}

	result, err := ec.Execute(ctx, target, sql, bindVariables, transactionID, reservedID, options)
	return result, transactionID, err
}

func (ec *Executor) beginWaitForSameRangeTransactions(ctx context.Context, target *database.Target, options *types.ExecuteOptions, sql string, bindVariables map[string]*types.BindVariable) (txserializer.DoneFunc, error) {
	// Serialize the creation of new transactions *if* the first
	// UPDATE or DELETE query has the same WHERE clause as a query which is
	// already running in a transaction (only other BeginExecute() calls are
	// considered). This avoids exhausting all txpool slots due to a hot row.
	//
	// Known Issue: There can be more than one transaction pool slot in use for
	// the same row because the next transaction is unblocked after this
	// BeginExecute() call is done and before Commit() on this transaction has
	// been called. Due to the additional MySQL locking, this should result into
	// two transaction pool slots per row at most. (This transaction pending on
	// COMMIT, the next one waiting for MySQL in BEGIN+EXECUTE.)
	var txDone txserializer.DoneFunc

	err := ec.exec(
		// Use (potentially longer) -queryserver-config-query-timeout and not
		// -queryserver-config-txpool-timeout (defaults to 1s) to limit the waiting.
		ctx,
		"", "waitForSameRangeTransactions", nil,
		target, options,
		func(e executionContext) error {
			k, table := ec.computeTxSerializerKey(ctx, sql, bindVariables)
			if k == "" {
				// Query is not subject to tx serialization/hot row protection.
				return nil
			}

			startTime := time.Now()
			done, waited, waitErr := ec.qe.txSerializer.Wait(ctx, k, table)
			txDone = done
			if waited {
				ec.stats.WaitTimings.Record("TxSerializer", startTime)
			}

			return waitErr
		})
	return txDone, err
}
