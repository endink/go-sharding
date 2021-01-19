// Copyright 2019 The Gaea Authors. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package plan

import (
	"fmt"
	"github.com/XiaoMi/Gaea/core"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/format"
)

// ColumnNameExprDecorator decorate ColumnNameExpr to rewrite table name
type ColumnNameExprDecorator struct {
	*ast.ColumnNameExpr
	Name *ColumnNameDecorator
}

// ColumnNameDecorator decorate ColumnName to rewrite table name
type ColumnNameDecorator struct {
	origin  *ast.ColumnName
	rule    *core.ShardingTable
	result  *RouteResult
	isAlias bool
}

// CreateColumnNameExprDecorator create ColumnNameExprDecorator
func CreateColumnNameExprDecorator(n *ast.ColumnNameExpr, rule *core.ShardingTable, isAlias bool, result *RouteResult) *ColumnNameExprDecorator {
	columnName := createColumnNameDecorator(n.Name, rule, isAlias, result)
	return &ColumnNameExprDecorator{
		ColumnNameExpr: n,
		Name:           columnName,
	}
}

func createColumnNameDecorator(n *ast.ColumnName, table *core.ShardingTable, isAlias bool, result *RouteResult) *ColumnNameDecorator {
	ret := &ColumnNameDecorator{
		origin:  n,
		rule:    table,
		result:  result,
		isAlias: isAlias,
	}
	return ret
}

// GetColumnInfo get column info, return db, table, column
func (c *ColumnNameDecorator) GetColumnInfo() (string, string, string) {
	return getColumnInfoFromColumnName(c.origin)
}

// Restore implement ast.Node
func (c *ColumnNameDecorator) Restore(ctx *format.RestoreCtx) error {
	db, table, err := c.result.GetCurrent()
	if err != nil {
		return err
	}

	if c.rule.IsSharding() {
		ctx.WriteName(db)
		ctx.WritePlain(".")
	} else {
		ctx.WriteName(c.origin.Schema.String())
		ctx.WritePlain(".")
	}

	if c.isAlias || !c.rule.IsSharding() {
		ctx.WriteName(c.origin.Table.String())
		ctx.WritePlain(".")
	} else {
		ctx.WriteName(table)
		ctx.WritePlain(".")
	}

	// 列名不需要改写
	ctx.WriteName(c.origin.Name.O)

	return nil
}

// Accept implement ast.Node
// do nothing and return current decorator
func (c *ColumnNameDecorator) Accept(v ast.Visitor) (ast.Node, bool) {
	return c, true
}

// Text implement ast.Node
func (c *ColumnNameDecorator) Text() string {
	return c.origin.Text()
}

// SetText implement ast.Node
func (c *ColumnNameDecorator) SetText(text string) {
	c.origin.SetText(text)
}

// Restore implement ast.Node
func (cc *ColumnNameExprDecorator) Restore(ctx *format.RestoreCtx) error {
	if err := cc.Name.Restore(ctx); err != nil {
		return fmt.Errorf("restore ColumnNameExprDecorator error: %v", err)
	}
	return nil
}
