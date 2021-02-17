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
	"github.com/XiaoMi/Gaea/core"
	"github.com/XiaoMi/Gaea/database"
	"github.com/XiaoMi/Gaea/logging"
	"github.com/XiaoMi/Gaea/mysql/types"
	"github.com/XiaoMi/Gaea/telemetry"
	"go.opentelemetry.io/otel/label"
	"sync"
)

type Executor struct {
	topology               DbTopology
	proxyDbName            string
	enableHotRowProtection bool
}

type executionContext struct {
	target  database.Target
	context context.Context
	te      *database.TxEngine
}

func (ec *Executor) exec(
	c context.Context,
	actionName, sql string,
	bindVariables map[string]*types.BindVariable,
	target *database.Target,
	options *types.ExecuteOptions,
	action func(ctx executionContext) error,
) error {
	ctx, span := telemetry.GlobalTracer.Start(c, "TxCoord"+actionName)
	if options != nil {
		span.SetAttributes(label.String("isolation-level", options.TransactionIsolation.String()))
	}
	span.SetAttributes(label.String("sql", sql))
	span.SetAttributes(label.String("schema", target.Schema))
	span.SetAttributes(label.String("datasource", target.DataSource))
	defer func() {
		defer span.End()
		defer database.RecoverError(logging.DefaultLogger, ctx)
	}()

	engine, err := ec.topology.GetTxEngine(target)
	if err != nil {
		return err
	}

	exeContext := executionContext{
		target:  *target,
		context: ctx,
		te:      engine,
	}
	err = action(exeContext)
	if err != nil {
		return database.NewSqlError(ctx, sql, bindVariables, err)
	}
	return nil
}

// runTargets executes the action for all targets in parallel and returns a consolildated error.
// Flow is identical to runSessions.
func (ec *Executor) runTargets(ctx context.Context, targets []*database.Target, action func(context.Context, *database.Target) error) error {
	if len(targets) == 1 {
		return action(ctx, targets[0])
	}
	allErrors := new(core.AllErrorRecorder)
	var wg sync.WaitGroup
	for _, t := range targets {
		wg.Add(1)
		go func(t *database.Target) {
			defer wg.Done()
			if err := action(ctx, t); err != nil {
				allErrors.RecordError(err)
			}
		}(t)
	}
	wg.Wait()
	return allErrors.Error()
}
