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
	"github.com/endink/go-sharding/explain"
	"github.com/pingcap/parser/ast"
)

var _ explain.StatementFormatter = &ColumnNameWriter{}

// ColumnNameWriter decorate ColumnNameExpr to rewrite table name
type ColumnNameWriter struct {
	*ast.ColumnNameExpr
	columnName    *ast.ColumnName
	shardingTable string
}

func NewColumnNameWriter(n *ast.ColumnNameExpr, shardingTable string) (*ColumnNameWriter, error) {
	return &ColumnNameWriter{
		ColumnNameExpr: n,
		columnName:     n.Name,
		shardingTable:  shardingTable,
	}, nil
}

func (c *ColumnNameWriter) Text() string {
	return c.columnName.Text()
}

func (c *ColumnNameWriter) Format(ctx explain.StatementContext) error {
	tableName, err := explain.GetTable(c.columnName, ctx.GetRuntime().GetServerSchema())
	if err != nil {
		return err
	}
	isAlias := ctx.GetContext().TableLookup().HasAlias(tableName)

	table, err := ctx.GetRuntime().GetCurrentTable(c.shardingTable)
	if err != nil {
		return err
	}

	//if c.columnName.Schema.String() != "" {
	//	ctx.WriteName(db)
	//	ctx.WritePlain(".")
	//}

	if isAlias {
		ctx.WriteName(c.columnName.Table.String())
		ctx.WritePlain(".")
	} else {
		if c.Name.Table.O != "" {
			ctx.WriteName(table)
			ctx.WritePlain(".")
		}
	}

	// 列名不需要改写
	ctx.WriteName(c.columnName.Name.O)

	return nil
}
