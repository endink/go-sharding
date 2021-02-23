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
	"github.com/pingcap/parser/format"
	"github.com/pingcap/parser/types"
	driver "github.com/pingcap/tidb/types/parser_driver"
	"io"
)

var _ ast.ExprNode = &PatternInWriter{}

// PatternInWriter decorate PatternInExpr
// 需要反向查找，不同的分片表查询不同的值
type PatternInWriter struct {
	Expr ast.ExprNode
	Not  bool

	tables      []string
	tableValues map[string][]ast.ValueExpr // table - columnValue

	originValues  []ast.ValueExpr
	shardingTable *core.ShardingTable
	runtime       Runtime
	//是否需要根据分片改写值
	isScattered bool

	colName string
}

func NewPatternInWriter(
	n *ast.PatternInExpr,
	context explain.Context,
	runtime Runtime,
	shardingTable *core.ShardingTable) (*PatternInWriter, error) {
	columnNameExpr := n.Expr.(*ast.ColumnNameExpr)
	colWriter, colErr := NewColumnNameWriter(columnNameExpr, context, runtime, shardingTable.Name)
	if colErr != nil {
		return nil, fmt.Errorf("create pattern in writer fault: %v", colErr)
	}
	colName := explain.GetColumn(columnNameExpr.Name)

	isScattered := n.Not || !shardingTable.HasTableShardingColumn(colName) || !shardingTable.TableStrategy.IsScalarValueSupported()

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
		Expr:          colWriter,
		Not:           n.Not,
		shardingTable: shardingTable,
		runtime:       runtime,
		tables:        tables,
		tableValues:   valueMap,
		isScattered:   isScattered,
		originValues:  allValues,
	}

	return ret, nil
}

func (p *PatternInWriter) rewriteBindVars(bindVariables map[string]*myTypes.BindVariable) (explain.RewriteBindVarsResult, error) {
	var usedTables []string
	valueMap := make(map[string][]ast.ValueExpr)
	varMap := make(map[string][]string)
	params := make([]string, 0, len(p.originValues))

	if len(p.originValues) > 0 {
		shardingValue := core.ShardingValuesForSingleScalar(p.shardingTable.Name, p.colName)
		for _, v := range p.originValues {

			argName := ""
			switch typedValue := v.(type) {
			case *driver.ParamMarkerExpr:
				argName = fmt.Sprintf("p%d", typedValue.Order)
				params = append(params, argName)
			}

			value, err := explain.GetValueFromExpr(v)
			if err != nil {
				return nil, err
			}
			if value.IsConst() {
				if constVal, _ := value.GetValue(nil); constVal == nil {
					return nil, errors.New(fmt.Sprintf("sharding column '%s' value can not be null when use 'in' expresion", p.colName))
				}
			}

			var goValue interface{}
			goValue, err = value.GetValue(bindVariables)
			if err != nil {
				return nil, err
			}
			shardingValue.ScalarValues[p.colName][0] = goValue
			tables, e := p.shardingTable.TableStrategy.Shard(p.shardingTable.GetTables(), shardingValue)
			if e != nil {
				return nil, e
			}
			for _, t := range tables {
				if _, ok := valueMap[t]; !ok {
					usedTables = append(usedTables, t)
				}
				valueMap[t] = append(valueMap[t], v)

				if argName != "" {
					for _, n := range value.ParamNames() {
						if _, found := bindVariables[n]; found {
							varMap[t] = append(varMap[t], n)
						} else {
							return nil, errors.New(fmt.Sprintf("Parameter '%s' not in bind variables list", n))
						}
					}
				}

			}
		}
	}

	p.tables = usedTables
	p.tableValues = valueMap

	return explain.ResultFromScatterVars(params, varMap), nil
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

// Restore implement ast.Node
func (p *PatternInWriter) Restore(ctx *format.RestoreCtx) error {
	table, err := p.runtime.GetCurrentTable(p.shardingTable.Name)
	if err != nil {
		return err
	}

	if err = p.Expr.Restore(ctx); err != nil {
		return fmt.Errorf("an error occurred while restore PatternInExpr.Expr: %v", err)
	}
	if p.Not {
		ctx.WriteKeyWord(" NOT IN ")
	} else {
		ctx.WriteKeyWord(" IN ")
	}

	ctx.WritePlain("(")
	for i, expr := range p.tableValues[table] {
		if i != 0 {
			ctx.WritePlain(",")
		}
		if err = expr.Restore(ctx); err != nil {
			return fmt.Errorf("an error occurred while restore PatternInExpr.List[%d], err: %v", i, err)
		}
	}
	ctx.WritePlain(")")

	return nil
}

// Accept implement ast.Node
func (p *PatternInWriter) Accept(v ast.Visitor) (node ast.Node, ok bool) {
	return p, ok
}

// Text implement ast.Node
func (p *PatternInWriter) Text() string {
	return ""
}

// SetText implement ast.Node
func (p *PatternInWriter) SetText(text string) {
	return
}

// SetType implement ast.ExprNode
func (p *PatternInWriter) SetType(tp *types.FieldType) {
	return
}

// GetType implement ast.ExprNode
func (p *PatternInWriter) GetType() *types.FieldType {
	return nil
}

// SetFlag implement ast.ExprNode
func (p *PatternInWriter) SetFlag(flag uint64) {
	return
}

// GetFlag implement ast.ExprNode
func (p *PatternInWriter) GetFlag() uint64 {
	return 0
}

// Format implement ast.ExprNode
func (p *PatternInWriter) Format(w io.Writer) {
	return
}
