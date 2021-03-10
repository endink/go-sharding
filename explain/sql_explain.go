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
	"github.com/XiaoMi/Gaea/mysql/types"
	"github.com/XiaoMi/Gaea/util/sync2"
	"github.com/emirpasic/gods/stacks/arraystack"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/format"
	"strings"
)

type SqlExplain struct {
	shardingTableProvider ShardingTableProvider
	ctx                   Context
	logicStack            *arraystack.Stack
	subQueryDepth         sync2.AtomicInt32
	maxSubQueryDepth      int32
	bindVarCount          int
	valueRedoLogs         *valueRedoLogs
	AstNode               ast.Node
	rewriter              Rewriter
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
		bindVarCount:          0,
	}
}

func (s *SqlExplain) Context() Context {
	return s.ctx
}

func (s *SqlExplain) Schema() string {
	return s.shardingTableProvider.Schema()
}

func (s *SqlExplain) ExplainSelect(sel *ast.SelectStmt, rewriter Rewriter) error {
	s.AstNode = sel
	s.rewriter = rewriter

	var err error
	if err = s.orderParams(sel); err != nil {
		return err
	}
	if err = s.explainTables(sel, rewriter); err != nil {
		return err
	}
	if err = s.explainFields(sel, rewriter); err != nil {
		return err
	}
	if err = s.explainGroupBy(sel, rewriter); err != nil {
		return err
	}
	if err = s.explainOrderBy(sel, rewriter); err != nil {
		return err
	}
	if err = s.explainWhere(sel, rewriter); err != nil {
		return err
	}

	if err = s.explainHaving(sel, rewriter); err != nil {
		return err
	}
	return nil
}

func (s *SqlExplain) SetVars(bindVariables []*types.BindVariable) error {
	return s.rewriter.PrepareBindVariables(bindVariables)
}

func (s *SqlExplain) RestoreSql(runtime Runtime) (string, error) {
	sb := new(strings.Builder)
	rstCtx := &format.RestoreCtx{
		Flags: runtime.GetRestoreFlags(),
		In:    wrapWriter(sb, runtime, s.currentContext()),
	}
	err := s.AstNode.Restore(rstCtx)
	if err != nil {
		return "", err
	}
	return sb.String(), nil
}

func (s *SqlExplain) RestoreShardingValues(bindVariables []*types.BindVariable) (map[string]*core.ShardingValues, error) {
	ctx := newValueRedoContext()
	vars := bindVariables
	if bindVariables == nil {
		vars = make([]*types.BindVariable, 0)
	}
	return s.valueRedoLogs.Redo(ctx, vars)
}

func (s *SqlExplain) GetShardingTable(table string) (*core.ShardingTable, bool) {
	return s.shardingTableProvider.GetShardingTable(table)
}

func (s *SqlExplain) beginValueGroup() {
	_ = s.valueRedoLogs.append(new(redoBeginParentheses))
}

func (s *SqlExplain) endValueGroup() {
	_ = s.valueRedoLogs.append(new(redoEndParentheses))
}

func (s *SqlExplain) currentContext() Context {
	return s.ctx
}

func (s *SqlExplain) pushOrValueGroup(table string, column string, values ...ValueReference) {
	s.pushValueGroupWithLogic(table, column, core.LogicOr, values...)
}

func (s *SqlExplain) pushAndValueGroup(table string, column string, values ...ValueReference) {
	s.pushValueGroupWithLogic(table, column, core.LogicAnd, values...)
}

func (s *SqlExplain) pushOrValue(table string, column string, value ValueReference) {
	s.pushValueGroupWithLogic(table, column, core.LogicOr, value)
}

func (s *SqlExplain) pushAndValue(table string, column string, value ValueReference) {
	s.pushValueGroupWithLogic(table, column, core.LogicAnd, value)
}

func (s *SqlExplain) pushValue(table string, column string, values ...ValueReference) {
	s.pushValueWithLogic(table, column, s.currentLogic(), values...)
}

func (s *SqlExplain) pushValueGroupWithLogic(table string, column string, logic core.BinaryLogic, values ...ValueReference) {
	_ = s.valueRedoLogs.append(new(redoBeginParentheses))
	s.pushValueWithLogic(table, column, logic, values...)
	_ = s.valueRedoLogs.append(new(redoEndParentheses))
}

func (s *SqlExplain) pushValueWithLogic(table string, column string, logic core.BinaryLogic, values ...ValueReference) {
	log := &redoPushValue{
		table:  table,
		column: column,
		logic:  logic,
		values: values,
	}
	_ = s.valueRedoLogs.append(log)
	for _, value := range values {
		if !value.IsLiteral() {
			s.bindVarCount++
		}
	}
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
