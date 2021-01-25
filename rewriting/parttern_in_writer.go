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
	"fmt"
	"github.com/XiaoMi/Gaea/core"
	"github.com/XiaoMi/Gaea/explain"
	"github.com/XiaoMi/Gaea/util"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/format"
	"github.com/pingcap/parser/types"
	driver "github.com/pingcap/tidb/types/parser_driver"
	"io"
)

// type check
var _ ast.ExprNode = &PatternInWriter{}

// PatternInWriter decorate PatternInExpr
// 这里记录tableIndexes和indexValueMap是没有问题的, 因为如果是OR条件, 导致路由索引[]int变宽,
// 改写的SQL只是IN这一项没有值, 并不会影响SQL正确性和执行结果.
type PatternInWriter struct {
	Expr ast.ExprNode
	Not  bool

	tables      []string
	tableValues map[string][]ast.ExprNode // table - columnValue

	rule    *core.ShardingTable
	runtime explain.Runtime
}

func NewPatternInWriter(
	n *ast.PatternInExpr,
	rule *core.ShardingTable,
	context explain.Context) (*PatternInWriter, error) {

	columnNameExpr := n.Expr.(*ast.ColumnNameExpr)
	colWriter, colErr := NewColumnNameWriter(columnNameExpr, context)
	if colErr != nil {
		return nil, fmt.Errorf("create pattern in writer fault: %v", colErr)
	}

	tables, valueMap, err := getPatternInRouteResult(columnNameExpr.Name.Name.L, n.Not, rule, n.List)
	if err != nil {
		return nil, fmt.Errorf("getPatternInRouteResult error: %v", err)
	}

	ret := &PatternInWriter{
		Expr: colWriter,
		//List:        n.List,
		Not:         n.Not,
		rule:        rule,
		runtime:     context.Runtime(),
		tables:      tables,
		tableValues: valueMap,
	}

	return ret, nil
}

// 返回路由, 并构建路由索引到值的映射.
// 如果是分片条件, 则构建值到索引的映射.
// 例如, 1,2,3,4分别映射到索引0,2则[]int = [0,2], map=[0:[1,2], 2:[3,4]]
// 如果是全路由, 则每个分片都要返回所有的值.
func getPatternInRouteResult(
	column string,
	isNotIn bool,
	rule *core.ShardingTable,
	values []ast.ExprNode) ([]string, map[string][]ast.ExprNode, error) {

	if err := checkValueType(values); err != nil {
		return nil, nil, fmt.Errorf("check value error: %v", err)
	}

	if isNotIn {
		tables := rule.GetTables()
		valueMap := getBroadcastValueMap(tables, values)
		return tables, valueMap, nil
	}
	if !rule.IsTableSharding() || !rule.HasTableShardingColumn(column) || !rule.TableStrategy.IsScalarValueSupported() { //不支持明确值分片或者不分片
		tables := rule.GetTables()
		valueMap := getBroadcastValueMap(tables, values)
		return tables, valueMap, nil
	}

	var usedTables []string
	valueMap := make(map[string][]ast.ExprNode)
	nullErr := fmt.Sprintf("sharding column '%s' value can not be null", column)
	if len(values) > 0 {
		shardingValue := core.ShardingValuesForSingleScalar(rule.Name, column)
		for _, vi := range values {
			v, _ := vi.(*driver.ValueExpr)
			value, err := util.GetValueExprResultEx(v, false, nullErr)
			if err != nil {
				return nil, nil, err
			}
			//idx, err := rule.FindTableIndex(value)
			shardingValue.ScalarValues[column][0] = value
			tables, e := rule.TableStrategy.Shard(rule.GetTables(), shardingValue)
			if e != nil {
				return nil, nil, e
			}
			for _, t := range tables {
				if _, ok := valueMap[t]; !ok {
					usedTables = append(usedTables, t)
				}
				valueMap[t] = append(valueMap[t], vi)
			}
		}
	}
	return usedTables, valueMap, nil
}

// 所有的值类型必须为*driver.ValueExpr
func checkValueType(values []ast.ExprNode) error {
	for i, v := range values {
		if _, ok := v.(*driver.ValueExpr); !ok {
			return fmt.Errorf("value is not ValueExpr, index: %d, type: %T", i, v)
		}
	}
	return nil
}

func getBroadcastValueMap(tables []string, nodes []ast.ExprNode) map[string][]ast.ExprNode {
	ret := make(map[string][]ast.ExprNode)
	for _, t := range tables {
		ret[t] = nodes
	}
	return ret
}

// GetCurrentRouteResult get route result of current decorator
func (p *PatternInWriter) GetCurrentRouteResult() []string {
	return p.tables
}

// Restore implement ast.Node
func (p *PatternInWriter) Restore(ctx *format.RestoreCtx) error {
	table, err := p.runtime.GetCurrentTable()
	if err != nil {
		return err
	}

	if err := p.Expr.Restore(ctx); err != nil {
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
		if err := expr.Restore(ctx); err != nil {
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
