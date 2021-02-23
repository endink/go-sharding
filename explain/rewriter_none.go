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
	"github.com/XiaoMi/Gaea/mysql/types"
	"github.com/pingcap/parser/ast"
)

var NoneRewriter Rewriter = &noneRewriter{}

type noneRewriter struct {
}

func (m *noneRewriter) RewriteBindVariable(bindVars []*types.BindVariable) (RewriteBindVarsResult, error) {
	return NoneRewriteBindVarsResult, nil
}

func (m *noneRewriter) containsTable(table string, explainContext Context) bool {
	_, ok := explainContext.TableLookup().FindShardingTable(table)
	return ok
}

func (m *noneRewriter) findTable(columnName *ast.ColumnNameExpr, explainContext Context, explicit bool) (string, bool) {
	sd, ok, _ := FindShardingTableByColumn(columnName, explainContext, explicit)
	if ok {
		return sd.Name, true
	}
	return "", false
}

func (m *noneRewriter) RewriteTable(table *ast.TableName, explainContext Context) (RewriteNodeResult, error) {
	tableName := table.Name.L
	if m.containsTable(tableName, explainContext) {
		return ResultFromNode(table, tableName), nil
	}
	return NoneRewriteNodeResult, nil
}

func (m *noneRewriter) RewriteField(columnName *ast.ColumnNameExpr, explainContext Context) (RewriteExprResult, error) {
	colName := GetColumn(columnName.Name)
	if t, ok := m.findTable(columnName, explainContext, false); ok {
		return ResultFromExrp(columnName, t, colName), nil
	}
	return NoneRewriteExprResult, nil
}

func (m *noneRewriter) RewriteColumn(columnName *ast.ColumnNameExpr, explainContext Context) (RewriteExprResult, error) {
	colName := GetColumn(columnName.Name)
	if t, ok := m.findTable(columnName, explainContext, true); ok {
		return ResultFromExrp(columnName, t, colName), nil
	}
	return NoneRewriteExprResult, nil
}

func (m *noneRewriter) RewritePatterIn(patternIn *ast.PatternInExpr, explainContext Context) (RewriteExprResult, error) {
	columnName, ok := patternIn.Expr.(*ast.ColumnNameExpr)
	if !ok {
		return nil, errors.New("pattern in statement required ColumnNameExpr")
	}
	colName := GetColumn(columnName.Name)
	if t, ok := m.findTable(columnName, explainContext, true); ok {
		return ResultFromExrp(columnName, t, colName), nil
	}
	return NoneRewriteExprResult, nil
}

func (m *noneRewriter) RewriteBetween(between *ast.BetweenExpr, explainContext Context) (RewriteExprResult, error) {
	columnName, ok := between.Expr.(*ast.ColumnNameExpr)
	if !ok {
		return nil, errors.New("between and statement required ColumnNameExpr")
	}
	colName := GetColumn(columnName.Name)
	if t, ok := m.findTable(columnName, explainContext, true); ok {
		return ResultFromExrp(columnName, t, colName), nil
	}
	return NoneRewriteExprResult, nil
}

func (m *noneRewriter) RewriteLimit(limit *ast.Limit, explainContext Context) (RewriteLimitResult, error) {
	return NoneRewriteLimitResult, nil
}
