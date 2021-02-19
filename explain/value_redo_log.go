/*
 *
 *  * Copyright 2021. Go-Sharding Author All Rights Reserved.
 *  *
 *  *  Licensed under the Apache License, Version 2.0 (the "License");
 *  *  you may not use this file except in compliance with the License.
 *  *  You may obtain a copy of the License at
 *  *
 *  *      http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  *  Unless required by applicable law or agreed to in writing, software
 *  *  distributed under the License is distributed on an "AS IS" BASIS,
 *  *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  *  See the License for the specific language governing permissions and
 *  *  limitations under the License.
 *  *
 *  *  File author: Anders Xiao
 *
 */

package explain

import (
	"errors"
	"github.com/XiaoMi/Gaea/core"
	"github.com/XiaoMi/Gaea/mysql/types"
	"sync"
)

type redoPushValue struct {
	table  string
	column string
	logic  core.BinaryLogic
	values []ValueReference
}

type redoBeginParentheses struct {
}

type redoEndParentheses struct {
}

type redoBeginLogic struct {
	logic core.BinaryLogic
}

type redoEndLogic struct {
}

type valueRedoLogs struct {
	mu   sync.RWMutex
	logs []interface{}
}

func newValueRedoLogs() *valueRedoLogs {
	return &valueRedoLogs{}
}

func (vr *valueRedoLogs) append(logs ...interface{}) error {
	vr.mu.Lock()
	defer vr.mu.Unlock()
	for _, log := range logs {
		switch log.(type) {
		case *redoPushValue, *redoBeginParentheses, *redoEndParentheses, *redoBeginLogic, *redoEndLogic:
		default:
			return errors.New("invalid value redo log type")
		}
		vr.logs = append(vr.logs, log)
	}
	return nil
}

func (vr *valueRedoLogs) getShardingValues(s *valueRedoContext) map[string]*core.ShardingValues {
	scope := s.currentValueScope()
	values := make(map[string]*core.ShardingValues, len(scope.builders))
	for tableName, builder := range scope.builders {
		values[tableName] = builder.Build()
	}
	return values
}

func (vr *valueRedoLogs) Redo(
	redoCtx *valueRedoContext,
	bindVars map[string]*types.BindVariable) (map[string]*core.ShardingValues, error) {

	vr.mu.RLock()
	defer vr.mu.RUnlock()
	var err error
	for _, l := range vr.logs {
		if err != nil {
			return nil, err
		}
		switch log := l.(type) {
		case *redoPushValue:
			var val interface{}
			if len(log.values) == 1 {
				val, err = log.values[0].GetValue(bindVars)
				if err == nil {
					err = redoCtx.pushValueWitLogic(log.table, log.column, val, log.logic)
				}
			}
			if len(log.values) > 1 {
				for _, valRef := range log.values {
					val, err = valRef.GetValue(bindVars)
					if err == nil {
						err = redoCtx.pushValueWitLogic(log.table, log.column, val, log.logic)
					} else {
						break
					}
				}
			}
		case *redoBeginParentheses:
			redoCtx.beginValueGroup()
		case *redoEndParentheses:
			err = redoCtx.endValueGroup()
		case *redoBeginLogic:
			redoCtx.pushLogic(log.logic)
		case *redoEndLogic:
			redoCtx.popLogic()
		}
	}
	if err != nil {
		return nil, err
	}
	return vr.getShardingValues(redoCtx), nil
}
