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
	"github.com/endink/go-sharding/core"
	"github.com/endink/go-sharding/explain"
	"github.com/endink/go-sharding/mysql/types"
	"github.com/pingcap/errors"
	"github.com/pingcap/parser/ast"
)

func NewRewriter() explain.Rewriter {
	return &engine{}
}

type engine struct {
	preparerList []preparer
}



func (engine *engine) PrepareBindVariables(bindVars []*types.BindVariable) error {
	if len(engine.preparerList) > 0 {
		for _, rw := range engine.preparerList {
			err := rw.prepare(bindVars)
			if err != nil {
				return nil
			}
		}
	}
	return nil
}

func (engine *engine) RewriteTable(tableName *ast.TableName, explainContext explain.Context) (explain.RewriteFormattedResult, error) {
	sd, ok, fe := explain.FindShardingTableByTable(tableName, explainContext, "")
	if fe != nil {
		return nil, fe
	}
	if ok {
		if writer, err := NewTableNameWriter(tableName); err == nil {
			return explain.ResultFromFormatter(writer, sd.Name, ""), nil
		} else {
			return nil, err
		}
	}
	return explain.NoneRewriteFormattedResult, nil
}

func (engine *engine) RewriteField(columnName *ast.ColumnNameExpr, explainContext explain.Context) (explain.RewriteFormattedResult, error) {
	sd, ok, err := explain.FindShardingTableByColumn(columnName.Name, explainContext, false)
	if err != nil {
		return nil, err
	}
	if ok {
		if writer, e := NewColumnNameWriter(columnName, sd.Name); e == nil {
			return explain.ResultFromFormatter(writer, sd.Name, explain.GetColumn(columnName.Name)), nil
		} else {
			return nil, e
		}
	}
	return explain.NoneRewriteFormattedResult, nil
}

func (engine *engine) RewriteColumn(columnName *ast.ColumnNameExpr, explainContext explain.Context) (explain.RewriteFormattedResult, error) {
	sd, ok, err := explain.FindShardingTableByColumn(columnName.Name, explainContext, true)
	if err != nil {
		return nil, err
	}
	if ok {
		if writer, e := NewColumnNameWriter(columnName, sd.Name); e == nil {
			return explain.ResultFromFormatter(writer, sd.Name, explain.GetColumn(columnName.Name)), nil
		} else {
			return nil, e
		}
	}
	return explain.NoneRewriteFormattedResult, nil
}

func (engine *engine) RewritePatterIn(patternIn *ast.PatternInExpr, explainContext explain.Context) (explain.RewriteFormattedResult, error) {
	columnNameExpr, ok := patternIn.Expr.(*ast.ColumnNameExpr)
	if !ok {
		return nil, errors.New("pattern in statement required ColumnNameExpr")
	}
	sd, ok, err := explain.FindShardingTableByColumn(columnNameExpr.Name, explainContext, true)
	if err != nil {
		return nil, err
	}
	if ok {
		if writer, e := NewPatternInWriter(patternIn, sd); e == nil {
			engine.preparerList = append(engine.preparerList, writer)
			return explain.ResultFromFormatter(writer, sd.Name, explain.GetColumn(columnNameExpr.Name)), nil
		} else {
			return nil, e
		}
	}
	return explain.NoneRewriteFormattedResult, nil
}

func removeSchemaAndTableInfoInColumnName(column *ast.ColumnName) {
	column.Schema.O = ""
	column.Schema.L = ""
	column.Table.O = ""
	column.Table.L = ""
}

func (engine *engine) RewriteInsertValues(
	insertStmt *ast.InsertStmt,
	explainContext explain.Context) (explain.RewriteFormattedResult, error) {

	shardingColIndex := -1
	var shardingCol *ast.ColumnName
	var shardingTable *core.ShardingTable
	for i, col := range insertStmt.Columns {
		removeSchemaAndTableInfoInColumnName(col)
		columnName := col.Name.L

		sd, ok, e := explain.FindShardingTableByColumn(col, explainContext, true)
		if e != nil {
			return nil, e
		}
		if ok && sd.HasShardingColumn(columnName) {
			shardingTable = sd
			shardingColIndex = i
			shardingCol= col
		}
	}

	if shardingColIndex < 0 {
		return explain.NoneRewriteFormattedResult, nil
	}
	writer, err:= NewInsertValuesWriter(insertStmt, shardingCol.Name.O, shardingColIndex, insertStmt.Lists, shardingTable)
	if err != nil {
		return nil, err
	}
	engine.preparerList = append(engine.preparerList, writer)
	return explain.ResultFromFormatter(writer, shardingTable.Name, shardingCol.Name.O), nil
}

func (engine *engine) RewriteBetween(between *ast.BetweenExpr, explainContext explain.Context) (explain.RewriteFormattedResult, error) {
	columnNameExpr, ok := between.Expr.(*ast.ColumnNameExpr)
	if !ok {
		return nil, errors.New("between and statement required ColumnNameExpr")
	}
	sd, ok, err := explain.FindShardingTableByColumn(columnNameExpr.Name, explainContext, true)
	if err != nil {
		return nil, err
	}
	if ok {
		if writer, e := NewBetweenWriter(between, sd); e == nil {
			return explain.ResultFromFormatter(writer, sd.Name, explain.GetColumn(columnNameExpr.Name)), nil
		} else {
			return nil, e
		}
	}
	return explain.NoneRewriteFormattedResult, nil
}

func (engine *engine) RewriteLimit(limit *ast.Limit, explainContext explain.Context) (explain.RewriteLimitResult, error) {
	lookup := explainContext.LimitLookup()
	if lookup.HasLimit() && lookup.HasOffset() {
		count, err := newLimit(explainContext)
		if err != nil {
			return nil, err
		}
		return explain.ResultFromLimit(count), nil
	}
	return explain.NoneRewriteLimitResult, nil
}
