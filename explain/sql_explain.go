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
	"errors"
	"github.com/XiaoMi/Gaea/core"
	"github.com/emirpasic/gods/stacks/arraystack"
	"github.com/pingcap/parser/ast"
	"sync"
)

type ShardingProvider func(table string) (*core.ShardingTable, bool)

type SqlExplain struct {
	lock             sync.Mutex
	builders         map[string]*core.ShardingValuesBuilder
	shardingProvider ShardingProvider
	context          *Context
	logicStack       *arraystack.Stack
}

func NewSqlExplain(shardingProvider ShardingProvider) *SqlExplain {
	return &SqlExplain{
		builders:         make(map[string]*core.ShardingValuesBuilder),
		shardingProvider: shardingProvider,
		logicStack:       arraystack.New(),
	}
}

func (s *SqlExplain) PushValue(table string, column string, value interface{}) error {
	if rg, ok := value.(core.Range); ok {
		return s.pushRange(table, column, rg)
	} else {
		s.pushScalar(table, column, value)
		return nil
	}
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
		s.lock.Lock()
		defer s.lock.Unlock()
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

func (s *SqlExplain) ExplainJoinOn(on *ast.OnCondition, rewriter Rewriter) error {
	newExpr, err := s.explainCondition(on.Expr, rewriter, core.LogicAnd)
	if err != nil {
		return err
	}
	on.Expr = newExpr
	return nil
}

func (s *SqlExplain) ExplainWhere(sel *ast.SelectStmt, rewriter Rewriter) error {
	where := sel.Where
	if where != nil {
		expr, err := s.explainCondition(where, rewriter, core.LogicAnd)
		if err != nil {
			sel.Where = expr
		}
	}
	return nil
}

func (s *SqlExplain) ExplainHaving(stmt *ast.SelectStmt, rewriter Rewriter) error {

	having := stmt.Having
	if having == nil {
		return nil
	}

	return s.rewriteColumn(having, rewriter, "explain having fault !")
}

func (s *SqlExplain) ExplainLimit(stmt *ast.SelectStmt, rewriter Rewriter) error {
	if stmt.Limit != nil {
		result, err := rewriter.RewriteLimit(stmt.Limit, s.context)
		if err != nil {
			return err
		}
		if result.IsRewrote() {
			l, ok := result.GetNewNode().(*ast.Limit)
			if !ok {
				return errors.New("rewrite limit succeed, but result is not a limit node")
			}
			stmt.Limit = l
		}
	}
	return nil
}
