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
	"github.com/XiaoMi/Gaea/core/comparison"
	"github.com/XiaoMi/Gaea/util"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/format"
	"github.com/pingcap/parser/opcode"
	"github.com/pingcap/tidb/types"
	driver "github.com/pingcap/tidb/types/parser_driver"
	"strings"
)

// BinaryOperationFieldtype declares field type of binary operation
type BinaryOperationFieldtype int

var EscapeRestoreFlags = format.RestoreStringSingleQuotes | format.RestoreStringEscapeBackslash | format.RestoreKeyWordUppercase | format.RestoreNameBackQuotes

// Expr type
const (
	UnsupportExpr BinaryOperationFieldtype = iota
	ValueExpr
	ColumnNameExpr
	FuncCallExpr
)

func getExprNodeTypeInBinaryOperation(n ast.ExprNode) BinaryOperationFieldtype {
	switch n.(type) {
	case *ast.ColumnNameExpr:
		return ColumnNameExpr
	case *driver.ValueExpr:
		return ValueExpr
	case *ast.FuncCallExpr:
		return FuncCallExpr
	default:
		return UnsupportExpr
	}
}

func IsSupportedValue(n *driver.ValueExpr) bool {
	switch n.Kind() {
	case types.KindInt64, types.KindUint64, types.KindFloat32, types.KindFloat64, types.KindString:
		return true
	}
	return false
}

func IsSupportedOp(op opcode.Op) bool {
	switch op {
	case opcode.EQ, opcode.GT, opcode.GE, opcode.LT, opcode.LE:
		return true
	}
	return false
}

// GetValueExprResult copy from ValueExpr.Restore()
// TODO: 分表列是否需要支持等值比较NULL
func getValueFromExpr(n *driver.ValueExpr) (interface{}, error) {
	return getValueFromExprEx(n, true, "")
}

//将算数运算符转换为可以理解的 value 值( scalar 或者 range )
func GetValueFromOpValue(op opcode.Op, valueExpr *driver.ValueExpr) (interface{}, error) {
	value, e := getValueFromExpr(valueExpr)
	if e != nil {
		return nil, e
	}
	switch op {
	case opcode.EQ:
		return value, nil
	case opcode.GT:
		rng, err := core.NewRangeOpen(value, nil)
		if err != nil {
			return nil, err
		}
		return rng, nil
	case opcode.GE:
		rng, err := core.NewRangeClose(value, nil)
		if err != nil {
			return nil, err
		}
		return rng, nil
	case opcode.LT:
		rng, err := core.NewRangeOpen(nil, value)
		if err != nil {
			return nil, err
		}
		return rng, nil
	case opcode.LE:
		rng, err := core.NewRangeClose(nil, value)
		if err != nil {
			return nil, err
		}
		return rng, nil
	}
	return nil, fmt.Errorf("explain value fault, known opcode: %s", op.String())
}

func GetValueFromValueFromBetween(n *ast.BetweenExpr) ([]core.Range, error) {
	leftValueExpr, ok := n.Left.(*driver.ValueExpr)
	if !ok {
		return nil, fmt.Errorf("n.Left is not a ValueExpr, type: %T", n.Left)
	}
	leftValue, err := util.GetValueExprResult(leftValueExpr)
	if err != nil {
		return nil, fmt.Errorf("get value from n.Left error: %v", err)
	}

	rightValueExpr, ok := n.Right.(*driver.ValueExpr)
	if !ok {
		return nil, fmt.Errorf("n.Left is not a ValueExpr, type: %T", n.Right)
	}

	rightValue, err := util.GetValueExprResult(rightValueExpr)
	if err != nil {
		return nil, fmt.Errorf("get value from n.Right error: %v", err)
	}

	cm, cmpErr := comparison.Compare(leftValue, rightValue)
	if cmpErr != nil {
		return nil, err
	}

	if cm > 0 {
		return nil, fmt.Errorf("the 'between and' start value must be less than or equal to the end value (%v, %v)", leftValue, rightValue)
	}

	var r1, r2 core.Range
	if n.Not {
		r1, err = core.NewRangeOpen(nil, leftValue)
		if err != nil {
			return nil, err
		}

		r2, err = core.NewRangeOpen(rightValue, nil)
		if err != nil {
			return nil, err
		}
		return []core.Range{r1, r2}, nil
	} else {
		r1, err = core.NewRangeClose(leftValue, rightValue)
		if err != nil {
			return nil, err
		}
		return []core.Range{r1}, nil
	}
}

func getValueFromExprEx(n *driver.ValueExpr, allowNull bool, nullErrorMsg string) (interface{}, error) {
	switch n.Kind() {
	case types.KindNull:
		if !allowNull {
			return nil, errors.New(core.IfBlankAndTrim(nullErrorMsg, "column value can not be null"))
		} else {
			return nil, nil
		}
	case types.KindInt64:
		return n.GetInt64(), nil
	case types.KindUint64:
		return n.GetUint64(), nil
	case types.KindFloat32:
		return n.GetFloat32(), nil
	case types.KindFloat64:
		return n.GetFloat64(), nil
	case types.KindString, types.KindBytes:
		return n.GetString(), nil
	default:
		s := &strings.Builder{}
		ctx := format.NewRestoreCtx(EscapeRestoreFlags, s)
		err := n.Restore(ctx)
		if err != nil {
			return nil, err
		}
		return s.String(), nil
	}
}

func getFullColumnInfo(columnName *ast.ColumnName) (schema string, table string, name string) {
	return columnName.Schema.L, columnName.Table.L, columnName.Name.L
}

func getTable(columnName *ast.ColumnName) string {
	return columnName.Table.L
}

func getTableAndColumn(columnName *ast.ColumnName) (string, string) {
	return columnName.Table.L, columnName.Name.L
}

func getColumn(columnName *ast.ColumnName) string {
	return columnName.Name.L
}
