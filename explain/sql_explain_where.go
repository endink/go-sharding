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
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/opcode"
)

func (s *SqlExplain) explainWhere(sel *ast.SelectStmt, rewriter Rewriter) error {
	where := sel.Where
	if where != nil {
		property := NewNodeProperty(sel.Where, func(n ast.ExprNode) {
			sel.Where = n
		})
		return s.rewriteCondition(property, rewriter)
	}
	return nil
}

func (s *SqlExplain) rewriteCondition(where NodeProperty, rewriter Rewriter) error {

	logic := core.LogicAnd
	_ = s.valueRedoLogs.append(redoBeginLogic{logic: logic})
	s.logicStack.Push(logic)
	defer func() {
		s.logicStack.Pop()
		_ = s.valueRedoLogs.append(new(redoEndLogic))
	}()

	logicStack := newLogicPriorityStack()

	err := s.explainCondition(where, rewriter, logicStack)
	if err != nil {
		return err
	}
	return logicStack.Calc()
}

func getLogicFromCode(op opcode.Op) (core.BinaryLogic, bool) {
	switch op {
	case opcode.LogicAnd:
		return core.LogicAnd, true
	case opcode.LogicOr:
		return core.LogicOr, true
	}
	return core.LogicAnd, false
}

func getLogicOperation(expr ast.ExprNode) (*ast.BinaryOperationExpr, bool) {
	if b, ok := expr.(*ast.BinaryOperationExpr); ok {
		switch b.Op {
		case opcode.LogicAnd, opcode.LogicOr:
			return b, true
		}
	}
	return nil, false
}
