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
	"sync"
)

type ShardingProvider func(table string) (*core.ShardingTable, bool)

type SqlExplain struct {
	tableLock        sync.Mutex
	builders         map[string]*core.ShardingValuesBuilder
	shardingProvider ShardingProvider
	ctx              Context
	logicStack       *arraystack.Stack
	subQueryDepth    sync2.AtomicInt32
	maxSubQueryDepth int32
	valuesChanged    bool
	values           map[string]*core.ShardingValues
}

func NewSqlExplain(shardingProvider ShardingProvider) *SqlExplain {
	return &SqlExplain{
		builders:         make(map[string]*core.ShardingValuesBuilder),
		shardingProvider: shardingProvider,
		logicStack:       arraystack.New(),
		ctx:              NewContext(),
		maxSubQueryDepth: int32(5),
		values:           make(map[string]*core.ShardingValues, 0),
	}
}

func (s *SqlExplain) CurrentContext() Context {
	return s.ctx
}

func (s *SqlExplain) GetShardingValues() map[string]*core.ShardingValues {
	if s.valuesChanged { //暂时不需要线程安全
		values := make(map[string]*core.ShardingValues, len(s.builders))
		for tableName, builder := range s.builders {
			values[tableName] = builder.Build()
		}
		s.values = values
	}
	return s.values
}

func (s *SqlExplain) PushValue(table string, column string, value interface{}) error {
	var err error
	if rg, ok := value.(core.Range); ok {
		err = s.pushRange(table, column, rg)
	} else {
		s.pushScalar(table, column, value)
	}
	if err == nil {
		s.valuesChanged = true
	}
	return err
}

func (s *SqlExplain) pushScalar(table string, column string, value interface{}) {
	b := s.table(table)
	switch s.currentLogic() {
	case core.LogicOr:
		b.OrValue(column, value)
	case core.LogicAnd:
		b.AndValue(column, value)
	}
}

func (s *SqlExplain) pushRange(table string, column string, value core.Range) error {
	b := s.table(table)
	switch s.currentLogic() {
	case core.LogicOr:
		return b.OrRange(column, value)
	case core.LogicAnd:
		return b.AndRange(column, value)
	}
	return nil
}

func (s *SqlExplain) table(tableName string) *core.ShardingValuesBuilder {
	var builder *core.ShardingValuesBuilder
	var ok bool
	if builder, ok = s.builders[tableName]; !ok {
		s.tableLock.Lock()
		defer s.tableLock.Unlock()
		if builder, ok = s.builders[tableName]; !ok {
			builder = core.NewShardingValuesBuilder(tableName)
			s.builders[tableName] = builder
		}
	}
	return builder
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
