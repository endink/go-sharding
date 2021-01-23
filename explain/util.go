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
