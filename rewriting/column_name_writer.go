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
	"github.com/XiaoMi/Gaea/explain"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/format"
)

var _ ast.ExprNode = &ColumnNameWriter{}

// ColumnNameWriter decorate ColumnNameExpr to rewrite table name
type ColumnNameWriter struct {
	*ast.ColumnNameExpr
	columnName *ast.ColumnName
	isAlias    bool
	runtime    explain.Runtime
}

func NewColumnNameWriter(n *ast.ColumnNameExpr, context explain.Context) (*ColumnNameWriter, error) {
	name := n.Name
	tableName, err := GetColumnTableName(name, context)
	if err != nil {
		return nil, err
	}

	return &ColumnNameWriter{
		ColumnNameExpr: n,
		columnName:     n.Name,
		isAlias:        context.TableLookup().HasAlias(tableName),
		runtime:        context.Runtime(),
	}, nil
}

func (c *ColumnNameWriter) Restore(ctx *format.RestoreCtx) error {
	db, table, err := c.runtime.GetCurrent()
	if err != nil {
		return err
	}

	if c.columnName.Schema.String() != "" {
		ctx.WriteName(db)
		ctx.WritePlain(".")
	}

	if c.isAlias {
		ctx.WriteName(c.columnName.Table.String())
		ctx.WritePlain(".")
	} else {
		ctx.WriteName(table)
		ctx.WritePlain(".")
	}

	// 列名不需要改写
	ctx.WriteName(c.columnName.Name.O)

	return nil
}

func (c *ColumnNameWriter) Accept(v ast.Visitor) (ast.Node, bool) {
	return c, true
}

func (c *ColumnNameWriter) Text() string {
	return c.columnName.Text()
}

func (c *ColumnNameWriter) SetText(text string) {
	c.columnName.SetText(text)
}
