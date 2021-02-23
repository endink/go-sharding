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
	myTypes "github.com/XiaoMi/Gaea/mysql/types"
	"github.com/XiaoMi/Gaea/parser"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/format"
	"github.com/pingcap/parser/opcode"
	"github.com/pingcap/tidb/types"
	driver "github.com/pingcap/tidb/types/parser_driver"
	"strings"
)

// BinaryOperationFieldtype declares field type of binary operation
type BinaryOperationFieldtype int

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
//func GetExprValue(n *driver.ValueExpr) (interface{}, error) {
//	return getValueFromExprStrictly(n, true, "")
//}

func astTypeToMySqlType(astType byte) (myTypes.MySqlType, error) {
	switch astType {
	case types.KindNull:
		return myTypes.Null, nil
	case types.KindInt64:
		return myTypes.Int64, nil
	case types.KindUint64:
		return myTypes.Uint64, nil
	case types.KindFloat32:
		return myTypes.Float32, nil
	case types.KindFloat64:
		return myTypes.Float64, nil
	case types.KindString:
		return myTypes.VarChar, nil
	case types.KindBytes:
		return myTypes.VarBinary, nil
	case types.KindMysqlDecimal:
		return myTypes.Decimal, nil
	case types.KindMysqlDuration:
		return myTypes.Timestamp, nil
	case types.KindMysqlEnum:
		return myTypes.Enum, nil
	case types.KindMysqlBit:
		return myTypes.Bit, nil
	case types.KindMysqlSet:
		return myTypes.Set, nil
	case types.KindMysqlTime:
		return myTypes.Time, nil
	case types.KindMysqlJSON:
		return myTypes.Json, nil
	default:
		return myTypes.Null, errors.New(fmt.Sprintf("unsupport ast type '%v' to mysql type", astType))
	}
}

func getParamFromExprStrictly(n *driver.ParamMarkerExpr) (*ArgScalarRef, error) {
	argName := fmt.Sprintf("p%d", n.Offset)
	argType, err := astTypeToMySqlType(n.Kind())
	if err != nil {
		return nil, err
	}
	return &ArgScalarRef{
		argName: argName,
		varType: argType,
	}, nil
}

func getValueFromExprStrictly(n *driver.ValueExpr, allowNull bool, nullErrorMsg string) (*ConstRef, error) {
	switch n.Kind() {
	case types.KindNull:
		if !allowNull {
			return nil, errors.New(core.IfBlankAndTrim(nullErrorMsg, "column value can not be null"))
		} else {
			return NewConstRef(nil), nil
		}
	case types.KindInt64:
		return NewConstRef(n.GetInt64()), nil
	case types.KindUint64:
		return NewConstRef(n.GetUint64()), nil
	case types.KindFloat32:
		return NewConstRef(n.GetFloat32()), nil
	case types.KindFloat64:
		return NewConstRef(n.GetFloat64()), nil
	case types.KindString, types.KindBytes:
		return NewConstRef(n.GetString()), nil
	default:
		s := &strings.Builder{}
		ctx := format.NewRestoreCtx(parser.EscapeRestoreFlags, s)
		err := n.Restore(ctx)
		if err != nil {
			return nil, err
		}
		return NewConstRef(s.String()), nil
	}
}

func GetValueFromExpr(n ast.ValueExpr) (ValueReference, error) {
	v, isValue := n.(*driver.ValueExpr)
	if isValue {
		return getValueFromExprStrictly(v, true, "")
	}

	p, isParam := n.(*driver.ParamMarkerExpr)
	if isParam {
		return getParamFromExprStrictly(p)
	}

	return nil, errors.New("only ValueExpr or ParamMarkerExpr support value extract")
}

type ConstRangeCreateFunc func(lower interface{}, upper interface{}) (core.Range, error)
type ParamRangeCreateFunc func(lowerArgName, upperArgName string, valueType myTypes.MySqlType) *ArgRangeRef

func makeRangeReference(lower ValueReference, upper ValueReference, constFunc ConstRangeCreateFunc, paramFunc ParamRangeCreateFunc) (ValueReference, error) {
	if lower == nil && upper == nil {
		return nil, errors.New("for making range reference, lower and upper require at least one that is not nil")
	}

	if lower != nil && upper != nil && lower.IsConst() != upper.IsConst() {
		return nil, errors.New("lower and upper must are all constants or all variables")
	}

	cLower, lowerIsConst := lower.(*ConstRef)
	cUpper, upperIsConst := upper.(*ConstRef)

	if lowerIsConst || upperIsConst {
		var l, u interface{}
		if cLower != nil {
			l = cLower.value
		}

		if cUpper != nil {
			u = cUpper.value
		}
		r, err := constFunc(l, u)
		if err != nil {
			return nil, err
		}
		return NewConstRef(r), nil
	}

	aLower, lowerIsArg := lower.(*ArgScalarRef)
	aUpper, upperIsArg := upper.(*ArgScalarRef)

	if lowerIsArg || upperIsArg {
		var l, u string
		var t myTypes.MySqlType

		if aLower != nil {
			l = aLower.argName
			t = aLower.varType
		}

		if aUpper != nil {
			u = aUpper.argName
			t = aUpper.varType
		}

		return paramFunc(l, u, t), nil
	}
	return nil, errors.New("lower and upper must be ConstRef or ArgScalarRef")
}

//将算数运算符转换为可以理解的 value 值( scalar 或者 range )
func GetValueFromOpValue(op opcode.Op, valueExpr ast.ValueExpr) (ValueReference, error) {
	value, e := GetValueFromExpr(valueExpr)
	if e != nil {
		return nil, e
	}
	switch op {
	case opcode.EQ:
		return value, nil
	case opcode.GT:
		return makeRangeReference(value, nil, core.NewRangeOpen, NewArgRangeOpen)
	case opcode.GE:
		return makeRangeReference(value, nil, core.NewRangeClose, NewArgRangeClose)
	case opcode.LT:
		return makeRangeReference(nil, value, core.NewRangeOpen, NewArgRangeOpen)
	case opcode.LE:
		return makeRangeReference(nil, value, core.NewRangeClose, NewArgRangeClose)
	}
	return nil, fmt.Errorf("explain value fault, known opcode: %s", op.String())
}

func GetValueFromPatternIn(n *ast.PatternInExpr, allowNull bool) ([]ValueReference, error) {
	if len(n.List) == 0 {
		return nil, nil
	}
	values := make([]ValueReference, 0, len(n.List))
	for _, value := range n.List {
		switch value.(type) {
		case *driver.ValueExpr, *driver.ParamMarkerExpr:
			vv, err := GetValueFromExpr(value.(ast.ValueExpr))
			if err != nil {
				return nil, err
			}
			values = append(values, vv)
		default:
			return nil, nil
		}
	}
	return values, nil
}

func CaseValueOrParamExpr(node ast.ExprNode) (ast.ValueExpr, bool) {
	switch node.(type) {
	case *driver.ValueExpr, *driver.ParamMarkerExpr:
		return node.(ast.ValueExpr), true
	default:
		return nil, false
	}
}

func GetValueFromValueFromBetween(n *ast.BetweenExpr) ([]ValueReference, error) {
	leftValueExpr, ok := CaseValueOrParamExpr(n.Left)
	if !ok {
		return nil, fmt.Errorf("n.Left is not a ValueExpr or ParamMarkerExpr, type: %T", n.Left)
	}
	leftValue, err := GetValueFromExpr(leftValueExpr)
	if err != nil {
		return nil, fmt.Errorf("get value from n.Left error: %v", err)
	}

	rightValueExpr, ok := CaseValueOrParamExpr(n.Right)
	if !ok {
		return nil, fmt.Errorf("n.Left is not a ValueExpr, type: %T", n.Right)
	}

	rightValue, err := GetValueFromExpr(rightValueExpr)
	if err != nil {
		return nil, fmt.Errorf("get value from n.Right error: %v", err)
	}

	if leftValue.IsConst() && rightValue.IsConst() {
		lv, _ := leftValue.GetValue(nil)
		rv, _ := rightValue.GetValue(nil)
		cm, cmpErr := comparison.Compare(lv, rv)
		if cmpErr != nil {
			return nil, err
		}

		if cm > 0 {
			return nil, fmt.Errorf("the 'between and' start value must be less than or equal to the end value (%v, %v)", leftValue, rightValue)
		}
	}

	var r1, r2 ValueReference
	if n.Not {
		r1, err = makeRangeReference(nil, leftValue, core.NewRangeOpen, NewArgRangeOpen)
		if err != nil {
			return nil, err
		}

		r2, err = makeRangeReference(rightValue, nil, core.NewRangeOpen, NewArgRangeOpen)
		if err != nil {
			return nil, err
		}
		return []ValueReference{r1, r2}, nil
	} else {
		r1, err = makeRangeReference(nil, leftValue, core.NewRangeClose, NewArgRangeClose)
		if err != nil {
			return nil, err
		}
		return []ValueReference{r1}, nil
	}
}

func GetColumn(columnName *ast.ColumnName) string {
	return columnName.Name.L
}

func GetTable(c *ast.ColumnName, allowedDbName string) (string, error) {
	db := c.Schema.O
	if db != "" && db != allowedDbName {
		return "", fmt.Errorf("cross database is not supported")
	}

	return c.Table.L, nil
}

func FindShardingTableByTable(t *ast.TableName, context Context, allowedDb string) (*core.ShardingTable, bool, error) {
	db := t.Schema.O
	if db != "" && db != allowedDb {
		return nil, false, fmt.Errorf("cross database is not supported")
	}
	shardingTable, ok := context.TableLookup().FindShardingTable(t.Name.L)
	return shardingTable, ok, nil
}

func FindShardingTableByColumn(columnName *ast.ColumnNameExpr, explainContext Context, explicit bool) (*core.ShardingTable, bool, error) {
	c := GetColumn(columnName.Name)
	if columnName.Name.Table.O != "" {
		sd, hasTable := explainContext.TableLookup().FindShardingTable(columnName.Name.Table.O)
		if !hasTable || (!sd.HasTableShardingColumn(c) && !sd.HasDbShardingColumn(c)) {
			return nil, false, nil
		}
		return sd, true, nil
	} else if explicit {
		sd, err := explainContext.TableLookup().ExplicitShardingTableByColumn(c)
		return sd, sd != nil, err
	}
	return nil, false, nil
}
