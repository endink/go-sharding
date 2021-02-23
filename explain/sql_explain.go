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

package explain

import (
	"github.com/XiaoMi/Gaea/core"
	"github.com/XiaoMi/Gaea/util/sync2"
	"github.com/emirpasic/gods/stacks/arraystack"
)

type SqlExplain struct {
	shardingTableProvider ShardingTableProvider
	ctx                   Context
	logicStack            *arraystack.Stack
	subQueryDepth         sync2.AtomicInt32
	maxSubQueryDepth      int32
	bindVarsCount         int
	valueRedoLogs         *valueRedoLogs
}

func MockSqlExplain(shardingTables ...*ShardingTableMocked) *SqlExplain {
	provider := MockShardingTableProvider(shardingTables...)
	exp := NewSqlExplain(provider)
	return exp
}

func NewSqlExplain(stProvider ShardingTableProvider) *SqlExplain {
	valueStack := arraystack.New()
	valueStack.Push(newValueScope(core.LogicAnd))
	redoLogs := newValueRedoLogs()
	_ = redoLogs.append(redoBeginLogic{logic: core.LogicAnd})
	return &SqlExplain{
		shardingTableProvider: stProvider,
		logicStack:            arraystack.New(),
		ctx:                   NewContext(),
		maxSubQueryDepth:      int32(5),
		valueRedoLogs:         newValueRedoLogs(),
	}
}

func (s *SqlExplain) GetShardingTable(table string) (*core.ShardingTable, bool) {
	return s.shardingTableProvider.GetShardingTable(table)
}

func (s *SqlExplain) BeginValueGroup() {
	_ = s.valueRedoLogs.append(new(redoBeginParentheses))
}

func (s *SqlExplain) EndValueGroup() {
	_ = s.valueRedoLogs.append(new(redoEndParentheses))
}

func (s *SqlExplain) currentContext() Context {
	return s.ctx
}

func (s *SqlExplain) PushOrValueGroup(table string, column string, values ...ValueReference) {
	s.pushValueGroupWithLogic(table, column, core.LogicOr, values...)
}

func (s *SqlExplain) PushAndValueGroup(table string, column string, values ...ValueReference) {
	s.pushValueGroupWithLogic(table, column, core.LogicAnd, values...)
}

func (s *SqlExplain) PushOrValue(table string, column string, value ValueReference) {
	s.pushValueGroupWithLogic(table, column, core.LogicOr, value)
}

func (s *SqlExplain) PushAndValue(table string, column string, value ValueReference) {
	s.pushValueGroupWithLogic(table, column, core.LogicAnd, value)
}

func (s *SqlExplain) pushValueGroupWithLogic(table string, column string, logic core.BinaryLogic, values ...ValueReference) {
	_ = s.valueRedoLogs.append(new(redoBeginParentheses))

	for _, v := range values {
		s.PushValueWithLogic(table, column, v, logic)
	}
	_ = s.valueRedoLogs.append(new(redoEndParentheses))
}

func (s *SqlExplain) PushValue(table string, column string, value ValueReference) {
	s.PushValueWithLogic(table, column, value, s.currentLogic())
}

func (s *SqlExplain) PushValueWithLogic(table string, column string, value ValueReference, logic core.BinaryLogic) {
	log := &redoPushValue{
		table:  table,
		column: column,
		logic:  logic,
		values: []ValueReference{value},
	}
	_ = s.valueRedoLogs.append(log)
}

func (s *SqlExplain) pushLogic(logic core.BinaryLogic) {
	_ = s.valueRedoLogs.append(&redoBeginLogic{logic})
	s.logicStack.Push(logic)
}

func (s *SqlExplain) popLogic() {
	if _, ok := s.logicStack.Pop(); ok {
		_ = s.valueRedoLogs.append(new(redoEndLogic))
	}
}

func (s *SqlExplain) currentLogic() core.BinaryLogic {
	v, ok := s.logicStack.Peek()
	if !ok {
		return core.LogicAnd
	}
	return v.(core.BinaryLogic)
}
