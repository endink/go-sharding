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

package rewriting

import (
	"errors"
	"fmt"
	"github.com/XiaoMi/Gaea/core"
	"github.com/XiaoMi/Gaea/explain"
	myTypes "github.com/XiaoMi/Gaea/mysql/types"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/types"
	driver "github.com/pingcap/tidb/types/parser_driver"
)

var _ explain.StatementFormatter = &PatternInWriter{}

// PatternInWriter decorate PatternInExpr
// 需要反向查找，不同的分片表查询不同的值
type PatternInWriter struct {
	colFormatter explain.StatementFormatter
	Not          bool

	tables      []string
	tableValues map[string][]ast.ValueExpr // table - columnValue

	originValues  []ast.ValueExpr
	shardingTable *core.ShardingTable
	//是否需要根据分片改写值
	isScattered bool

	colName      string
	paramIndexes []int
}

func (p *PatternInWriter) GetFlag() uint64 {
	panic("implement me")
}

func NewPatternInWriter(
	n *ast.PatternInExpr,
	shardingTable *core.ShardingTable) (*PatternInWriter, error) {
	columnNameExpr := n.Expr.(*ast.ColumnNameExpr)
	colWriter, colErr := NewColumnNameWriter(columnNameExpr, shardingTable.Name)
	if colErr != nil {
		return nil, fmt.Errorf("create pattern in writer fault: %v", colErr)
	}
	colName := explain.GetColumn(columnNameExpr.Name)

	isScattered := !n.Not && shardingTable.HasTableShardingColumn(colName) && shardingTable.TableStrategy.IsScalarValueSupported()

	allValues, caseErr := caseValueExpr(n.List)
	if caseErr != nil {
		return nil, fmt.Errorf("invalid value type in pattern 'in' value list: %v", caseErr)
	}

	var tables []string
	var valueMap map[string][]ast.ValueExpr

	if !isScattered { //如果不需要改写 in 的值可以创建时就固定, 否则，推迟到 Prepare 阶段
		tables = shardingTable.GetTables()
		valueMap = getBroadcastValueMap(tables, allValues)
	}

	ret := &PatternInWriter{
		colName:       colName,
		colFormatter:  colWriter,
		Not:           n.Not,
		shardingTable: shardingTable,
		tables:        tables,
		tableValues:   valueMap,
		isScattered:   isScattered,
		originValues:  allValues,
	}

	return ret, nil
}

func (p *PatternInWriter) prepare(bindVariables []*myTypes.BindVariable) error {
	if !p.isScattered {
		return nil
	}

	var usedTables []string
	valueMap := make(map[string][]ast.ValueExpr)
	varMap := make(map[string][]int)
	params := make([]int, 0, len(p.originValues))

	if len(p.originValues) > 0 {
		shardingValue := core.ShardingValuesForSingleScalar(p.shardingTable.Name, p.colName)
		for _, v := range p.originValues {

			argIndex := -1
			switch typedValue := v.(type) {
			case *driver.ParamMarkerExpr:
				argIndex = typedValue.Order
				params = append(params, argIndex)
			}

			value, err := explain.GetValueFromExpr(v)
			if err != nil {
				return err
			}
			if value.IsLiteral() {
				if constVal, _ := value.GetValue(nil); constVal == nil {
					return errors.New(fmt.Sprintf("sharding column '%s' value can not be null when use 'in' expresion", p.colName))
				}
			}

			var goValue interface{}
			goValue, err = value.GetValue(bindVariables)
			if err != nil {
				return err
			}
			shardingValue.ScalarValues[p.colName][0] = goValue
			tables, e := p.shardingTable.TableStrategy.Shard(p.shardingTable.GetTables(), shardingValue)
			if e != nil {
				return e
			}
			for _, t := range tables {
				if _, ok := valueMap[t]; !ok {
					usedTables = append(usedTables, t)
				}
				valueMap[t] = append(valueMap[t], v)

				if argIndex >= 0 {
					for _, n := range value.ParamIndexes() {
						if n < 0 || n >= len(bindVariables) {
							return errors.New(fmt.Sprintf("Parameter index '%d' out of range of bind variables list", n))
						}
						varMap[t] = append(varMap[t], n)
					}
				}

			}
		}
	}

	p.tables = usedTables
	p.tableValues = valueMap
	p.paramIndexes = params

	return nil
}

// 所有的值类型必须为*driver.ValueExpr
func caseValueExpr(values []ast.ExprNode) ([]ast.ValueExpr, error) {
	list := make([]ast.ValueExpr, len(values))
	for i, v := range values {
		if vv, ok := explain.CaseValueOrParamExpr(v); !ok {
			return nil, fmt.Errorf("value is not ValueExpr, index: %d, type: %T", i, v)
		} else {
			list[i] = vv
		}
	}
	return list, nil
}

func getBroadcastValueMap(tables []string, nodes []ast.ValueExpr) map[string][]ast.ValueExpr {
	ret := make(map[string][]ast.ValueExpr)
	for _, t := range tables {
		ret[t] = nodes
	}
	return ret
}

func (p *PatternInWriter) Format(ctx explain.StatementContext) error {
	rstCtx := ctx.CreateRestoreCtx()

	table, err := ctx.GetRuntime().GetCurrentTable(p.shardingTable.Name)
	if err != nil {
		return err
	}

	if err = p.colFormatter.Format(ctx); err != nil {
		return fmt.Errorf("an error occurred while restore PatternInExpr.Expr: %v", err)
	}

	values := p.tableValues[table]
	if len(values) > 1 {
		if p.Not {
			ctx.WriteKeyWord(" NOT IN ")
		} else {
			ctx.WriteKeyWord(" IN ")
		}
	} else {
		if p.Not {
			ctx.WriteKeyWord("!=")
		} else {
			ctx.WriteKeyWord("=")
		}
	}

	var usedArgs []int

	if len(values) > 1 {
		ctx.WritePlain("(")
	}
	for i, expr := range p.tableValues[table] {
		if i != 0 {
			ctx.WritePlain(",")
		}
		if err = expr.Restore(rstCtx); err != nil {
			return fmt.Errorf("an error occurred while restore PatternInExpr.List[%d], err: %v", i, err)
		}
		if n, ok := TryGetParameterIndex(expr); ok {
			usedArgs = append(usedArgs, n)
		}
	}
	if len(values) > 1 {
		ctx.WritePlain(")")
	}
	removed := core.DifferenceInt(p.paramIndexes, usedArgs)
	for _, s := range removed {
		ctx.GetRuntime().RemoveParameter(s)
	}
	return nil
}

// Text implement ast.Node
func (p *PatternInWriter) Text() string {
	return "in"
}

func (p *PatternInWriter) GetType() *types.FieldType {
	return p.colFormatter.GetType()
}
