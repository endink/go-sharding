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

type ShardingProvider func(table string) (*core.ShardingTable, bool)

type SqlExplain struct {
	valueStack       *arraystack.Stack
	shardingProvider ShardingProvider
	ctx              Context
	logicStack       *arraystack.Stack
	subQueryDepth    sync2.AtomicInt32
	maxSubQueryDepth int32
	valuesChanged    bool
	values           map[string]*core.ShardingValues
}

func NewSqlExplain(shardingProvider ShardingProvider) *SqlExplain {
	valueStack := arraystack.New()
	valueStack.Push(newValueScope(core.LogicAnd))
	return &SqlExplain{
		valueStack:       valueStack,
		shardingProvider: shardingProvider,
		logicStack:       arraystack.New(),
		ctx:              NewContext(),
		maxSubQueryDepth: int32(5),
		values:           make(map[string]*core.ShardingValues, 0),
	}
}

func (s *SqlExplain) BeginValueGroup() {
	ns := newValueScope(s.currentLogic())
	s.valueStack.Push(ns)
}

func (s *SqlExplain) EndValueGroup() error {
	if s.valueStack.Size() >= 2 {
		ns := s.currentValueScope()
		s.valueStack.Pop()
		pre := s.currentValueScope()
		for table, builder := range ns.builders {
			if err := pre.table(table).Merge(builder, ns.logic); err != nil {
				return err
			}
		}
	}
	return nil
}

func (s *SqlExplain) currentValueScope() *valueScope {
	current, _ := s.valueStack.Peek()
	return current.(*valueScope)
}

func (s *SqlExplain) CurrentContext() Context {
	return s.ctx
}

func (s *SqlExplain) GetShardingValues() map[string]*core.ShardingValues {
	scope := s.currentValueScope()
	if s.valuesChanged { //暂时不需要线程安全
		values := make(map[string]*core.ShardingValues, len(scope.builders))
		for tableName, builder := range scope.builders {
			values[tableName] = builder.Build()
		}
		s.values = values
	}
	return s.values
}

func (s *SqlExplain) PushOrValueGroup(table string, column string, values ...interface{}) error {
	return s.pushOrValueGroupWithLogic(table, column, core.LogicOr, values...)
}

func (s *SqlExplain) PushAndValueGroup(table string, column string, values ...interface{}) error {
	return s.pushOrValueGroupWithLogic(table, column, core.LogicAnd, values...)
}

func (s *SqlExplain) PushOrValue(table string, column string, value interface{}) error {
	return s.pushOrValueGroupWithLogic(table, column, core.LogicOr, value)
}

func (s *SqlExplain) PushAndValue(table string, column string, value interface{}) error {
	return s.pushOrValueGroupWithLogic(table, column, core.LogicAnd, value)
}

func (s *SqlExplain) pushOrValueGroupWithLogic(table string, column string, logic core.BinaryLogic, values ...interface{}) error {
	s.BeginValueGroup()
	for _, v := range values {
		if err := s.pushValueWitLogic(table, column, v, logic); err != nil {
			return err
		}
	}
	return s.EndValueGroup()
}

func (s *SqlExplain) pushValueWitLogic(table string, column string, value interface{}, logic core.BinaryLogic) error {
	var err error
	if rg, ok := value.(core.Range); ok {
		err = s.pushRange(table, column, rg, logic)
	} else {
		s.pushScalar(table, column, value, logic)
	}
	if err == nil {
		s.valuesChanged = true
	}
	return err
}

func (s *SqlExplain) PushValue(table string, column string, value interface{}) error {
	return s.pushValueWitLogic(table, column, value, s.currentLogic())
}

func (s *SqlExplain) pushScalar(table string, column string, value interface{}, logic core.BinaryLogic) {
	scope := s.currentValueScope()
	b := scope.table(table)
	switch logic {
	case core.LogicOr:
		b.OrValue(column, value)
	case core.LogicAnd:
		b.AndValue(column, value)
	}
}

func (s *SqlExplain) pushRange(table string, column string, value core.Range, logic core.BinaryLogic) error {
	scope := s.currentValueScope()
	b := scope.table(table)
	switch logic {
	case core.LogicOr:
		return b.OrRange(column, value)
	case core.LogicAnd:
		return b.AndRange(column, value)
	}
	return nil
}

func (s *SqlExplain) pushLogic(logic core.BinaryLogic) {
	s.logicStack.Push(logic)
}

func (s *SqlExplain) popLogic() {
	s.logicStack.Pop()
}

func (s *SqlExplain) currentLogic() core.BinaryLogic {
	v, ok := s.logicStack.Peek()
	if !ok {
		return core.LogicAnd
	}
	return v.(core.BinaryLogic)
}
