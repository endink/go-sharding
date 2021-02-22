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
	"github.com/XiaoMi/Gaea/mysql/types"
	"github.com/pingcap/parser/ast"
)

type RewriteResult interface {
	IsRewrote() bool
	GetShardingTable() string
}

type RewriteNodeResult interface {
	RewriteResult
	GetNewNode() ast.Node
}

type RewriteExprResult interface {
	GetColumn() string
	RewriteResult
	GetNewNode() ast.ExprNode
}

type RewriteLimitResult interface {
	RewriteResult
	GetNewNode() *ast.Limit
}

type RewriteBindVarsResult interface {
	//改写过的参数索引
	GetRewroteVarIndexes() []int
	// 根据分片表得到的索引
	GetScatterVarIndexes() map[string][]int //key: physical table, value: change array
	IsRewrote() bool
}

//SQL 改写器
type Rewriter interface {
	RewriteBindVariable(bindVars []*types.BindVariable) (RewriteBindVarsResult, error)

	RewriteTable(table *ast.TableName, explainContext Context) (RewriteNodeResult, error)
	RewriteField(columnName *ast.ColumnNameExpr, explainContext Context) (RewriteExprResult, error)
	//改写列，返回值为改写后的节点（装饰器）， 标志位 true 表示改写成功
	RewriteColumn(columnName *ast.ColumnNameExpr, explainContext Context) (RewriteExprResult, error)
	RewritePatterIn(patternIn *ast.PatternInExpr, explainContext Context) (RewriteExprResult, error)
	RewriteBetween(patternIn *ast.BetweenExpr, explainContext Context) (RewriteExprResult, error)
	RewriteLimit(limit *ast.Limit, explainContext Context) (RewriteLimitResult, error)
}
