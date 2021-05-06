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
	"github.com/endink/go-sharding/mysql/types"
	"github.com/pingcap/parser/ast"
)

type RewriteResult interface {
	IsRewrote() bool
	GetShardingTable() string
}

type RewriteColumnResult interface {
	GetColumn() string
	IsRewrote() bool
	GetShardingTable() string
}

type RewriteFormattedResult interface {
	RewriteColumnResult
	GetFormatter() StatementFormatter
}

type RewriteLimitResult interface {
	RewriteResult
	GetLimit() int64
}

//SQL 改写器
type Rewriter interface {
	PrepareBindVariables(bindVars []*types.BindVariable) error

	RewriteTable(table *ast.TableName, explainContext Context) (RewriteFormattedResult, error)
	RewriteField(columnName *ast.ColumnNameExpr, explainContext Context) (RewriteFormattedResult, error)

	RewriteColumn(columnName *ast.ColumnNameExpr, explainContext Context) (RewriteFormattedResult, error)

	RewritePatterIn(patternIn *ast.PatternInExpr, explainContext Context) (RewriteFormattedResult, error)
	RewriteBetween(patternIn *ast.BetweenExpr, explainContext Context) (RewriteFormattedResult, error)
	RewriteLimit(limit *ast.Limit, explainContext Context) (RewriteLimitResult, error)
}
