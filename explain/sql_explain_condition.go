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
	"fmt"
	"github.com/XiaoMi/Gaea/core"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/opcode"
)

func (s *SqlExplain) explainCondition(node ast.ExprNode, rewriter Rewriter) (ast.ExprNode, error) {
	switch expr := node.(type) {
	case *ast.BinaryOperationExpr:
		return s.explainBinary(expr, rewriter)
	case *ast.PatternInExpr:
		return s.explainPatternIn(expr, rewriter)
	case *ast.BetweenExpr:
		return s.explainBetween(expr, rewriter)
	case *ast.ParenthesesExpr:
		s.beginValueGroup()
		newExpr, err := s.explainCondition(expr.Expr, rewriter)
		if err != nil {
			return nil, err
		}
		expr.Expr = newExpr
		s.endValueGroup()
		return expr, nil
	default:
		// 其他情况只替换表名 (但是不处理根节点是ColumnNameExpr的情况, 理论上也不会出现这种情况)
		err := s.rewriteField(rewriter, "explain condition fault !", expr)
		return node, err
	}
}

func (s *SqlExplain) rewriteField(rewriter Rewriter, errMsg string, expr ...ast.Node) error {
	for _, node := range expr {
		if node != nil {
			v := NewFieldVisitor(rewriter, s.currentContext())
			node.Accept(v)
			if v.err != nil {
				msg := core.IfBlank(errMsg, "visitor rewrite fault !")
				return errors.New(fmt.Sprint(msg, core.LineSeparator, fmt.Sprintf("%v", v.err)))
			}
		}
	}

	return nil
}

// column in (xxx, xxx) 解释器
func (s *SqlExplain) explainBetween(expr *ast.BetweenExpr, rewriter Rewriter) (ast.ExprNode, error) {
	result, err := rewriter.RewriteBetween(expr, s.currentContext())
	if err != nil {
		return nil, err
	}
	if result.IsRewrote() {
		ranges, e := GetValueFromValueFromBetween(expr)

		if e != nil {
			return nil, e
		}

		s.pushOrValueGroup(result.GetShardingTable(), result.GetColumn(), ranges...)

		return wrapFormatter(result.GetFormatter()), nil
	}
	return expr, nil
}

// column in (xxx, xxx) 解释器
func (s *SqlExplain) explainPatternIn(expr *ast.PatternInExpr, rewriter Rewriter) (ast.ExprNode, error) {
	result, err := rewriter.RewritePatterIn(expr, s.currentContext())
	if err != nil {
		return nil, err
	}
	if result.IsRewrote() && !expr.Not {
		values, e := GetValueFromPatternIn(expr, false)
		if e != nil {
			return nil, e
		}
		s.pushOrValueGroup(result.GetShardingTable(), result.GetColumn(), values...)
		return wrapFormatter(result.GetFormatter()), nil
	}
	return expr, nil
}

//二元运算 or and = < ...
func (s *SqlExplain) explainBinary(expr *ast.BinaryOperationExpr, rewriter Rewriter) (ast.ExprNode, error) {
	//_, ok := opcode.Ops[expr.Op]
	//if !ok {
	//	return false, nil, nil, fmt.Errorf("unknown BinaryOperationExpr.Op: %v", expr.Op)
	//}

	switch expr.Op {
	case opcode.LogicAnd:
		return s.explainBinaryLogic(expr, rewriter, core.LogicAnd)
	case opcode.LogicOr:
		return s.explainBinaryLogic(expr, rewriter, core.LogicOr)
	case opcode.EQ, opcode.GT, opcode.GE, opcode.LT, opcode.LE: //不支持不等于： opcode.NE
		return s.explainBinaryMath(expr, rewriter)
	default:
		//其他情况尝试改写列名
		if _, err := s.rewriteLeftColumn(expr, rewriter); err != nil {
			return nil, err
		}
		if _, err := s.rewriteLeftColumn(expr, rewriter); err != nil {
			return nil, err
		}
		return expr, nil
	}
}

//处理逻辑运算符 or , and
func (s *SqlExplain) explainBinaryLogic(expr *ast.BinaryOperationExpr, rewriter Rewriter, logic core.BinaryLogic) (ast.ExprNode, error) {
	leftNode, lErr := s.explainCondition(expr.L, rewriter) //最左边的操作数要保持当前逻辑
	if lErr != nil {
		return nil, fmt.Errorf("handle BinaryOperationExpr.L error: %v", lErr)
	}
	s.pushLogic(logic)
	rightNode, rErr := s.explainCondition(expr.R, rewriter)
	s.popLogic()
	if rErr != nil {
		return nil, fmt.Errorf("handle BinaryOperationExpr.R error: %v", rErr)
	}

	if leftNode != nil {
		expr.L = leftNode
	}
	if rightNode != nil {
		expr.R = rightNode
	}

	return expr, nil
}

// 处理算术比较运算
// 如果出现列名, 则必须为列名与列名比较, 列名与值比较, 否则会报错 (比如 id + 2 = 3 就会报错, 因为 id + 2 处理不了)
// 如果是其他情况, 则直接返回 (如 1 = 1 这种)
func (s *SqlExplain) explainBinaryMath(expr *ast.BinaryOperationExpr, rewriter Rewriter) (ast.ExprNode, error) {
	lType := getExprNodeTypeInBinaryOperation(expr.L)
	rType := getExprNodeTypeInBinaryOperation(expr.R)

	// handle hint database function: SELECT * from tbl where DATABASE() = db_0 / 'db_0' / `db_0`
	//TODO: 不再支持函数处理
	//if expr.Op == opcode.EQ {
	//	if lType == FuncCallExpr {
	//		hintDB, err := getDatabaseFuncHint(expr.L.(*ast.FuncCallExpr), expr.R)
	//		if err != nil {
	//			return false, nil, nil, fmt.Errorf("get database function hint error: %v", err)
	//		}
	//		if hintDB != "" {
	//			p.hintPhyDB = hintDB
	//			return false, nil, expr, nil
	//		}
	//	} else if rType == FuncCallExpr {
	//		hintDB, err := getDatabaseFuncHint(expr.R.(*ast.FuncCallExpr), expr.L)
	//		if err != nil {
	//			return false, nil, nil, fmt.Errorf("get database function hint error: %v", err)
	//		}
	//		if hintDB != "" {
	//			p.hintPhyDB = hintDB
	//			return false, nil, expr, nil
	//		}
	//	}
	//}

	if lType == ColumnNameExpr && rType == ColumnNameExpr {
		_, err := s.rewriteLeftColumn(expr, rewriter)
		if err != nil {
			return nil, err
		}
		_, err = s.rewriteRightColumn(expr, rewriter)
		if err != nil {
			return nil, err
		}
	} else {
		if lType == ColumnNameExpr {
			return s.explainColumnWithValue(expr, rewriter, true)
		}

		if rType == ColumnNameExpr {
			return s.explainColumnWithValue(expr, rewriter, false)
		}
	}
	return expr, nil
}

func (s *SqlExplain) explainColumnWithValue(expr *ast.BinaryOperationExpr, rewriter Rewriter, columnLeft bool) (ast.ExprNode, error) {
	var columnNode, valueNode ast.ExprNode
	var err error
	var r RewriteResult

	if columnLeft {
		columnNode = expr.L
		valueNode = expr.R
	} else {
		columnNode = expr.R
		valueNode = expr.L
	}

	columnName := GetColumn(columnNode.(*ast.ColumnNameExpr).Name)

	if columnLeft {
		r, err = s.rewriteLeftColumn(expr, rewriter)
	} else {
		r, err = s.rewriteRightColumn(expr, rewriter)
	}
	if err != nil {
		return nil, err
	}
	if r.IsRewrote() {
		if v, ok := valueNode.(ast.ValueExpr); ok {
			if IsSupportedValue(v) && IsSupportedOp(expr.Op) {
				value, e := GetValueFromOpValue(expr.Op, v)
				if e != nil {
					return nil, err
				}
				s.pushValue(r.GetShardingTable(), columnName, value)
			}
		}
	}
	return expr, nil
}

func (s *SqlExplain) rewriteLeftColumn(expr *ast.BinaryOperationExpr, rewriter Rewriter) (RewriteResult, error) {
	leftCol, ok := expr.L.(*ast.ColumnNameExpr)
	if !ok {
		return NoneRewriteResult, nil
	}
	result, err := rewriter.RewriteColumn(leftCol, s.currentContext())
	if err != nil {
		return nil, err
	}
	if result != nil && result.IsRewrote() {
		expr.L = wrapFormatter(result.GetFormatter())
	}
	return result, nil
}

func (s *SqlExplain) rewriteRightColumn(expr *ast.BinaryOperationExpr, rewriter Rewriter) (RewriteResult, error) {
	col, ok := expr.R.(*ast.ColumnNameExpr)
	if !ok {
		return NoneRewriteResult, nil
	}
	result, err := rewriter.RewriteColumn(col, s.currentContext())
	if err != nil {
		return nil, err
	}
	if result != nil && result.IsRewrote() {
		expr.R = wrapFormatter(result.GetFormatter())
	}
	return result, nil
}
