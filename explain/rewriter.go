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
	"github.com/XiaoMi/Gaea/core"
	"github.com/pingcap/parser/ast"
)

type RewriteResult interface {
	IsRewrote() bool
	Table() *core.ShardingTable
}

type RewriteNodeResult interface {
	RewriteResult
	GetNewNode() ast.Node
}

type RewriteExprResult interface {
	RewriteResult
	GetNewNode() ast.ExprNode
}

type RewriteLimitResult interface {
	IsRewrote() bool
	Table() *core.ShardingTable
	GetNewNode() *ast.Limit
}

var NoneRewrote RewriteResult = &noneRewriteResult{}

type noneRewriteResult struct {
}

func (n *noneRewriteResult) IsRewrote() bool {
	return false
}

func (n *noneRewriteResult) Table() *core.ShardingTable {
	return nil
}

//SQL 改写器
type Rewriter interface {
	RewriteTable(table *ast.TableSource, explainContext Context) (RewriteNodeResult, error)
	RewriteField(columnName *ast.ColumnNameExpr, explainContext Context) (RewriteExprResult, error)
	//改写列，返回值为改写后的节点（装饰器）， 标志位 true 表示改写成功
	RewriteColumn(columnName *ast.ColumnNameExpr, explainContext Context) (RewriteExprResult, error)
	RewritePatterIn(patternIn *ast.PatternInExpr, explainContext Context) (RewriteExprResult, error)
	RewriteBetween(patternIn *ast.BetweenExpr, explainContext Context) (RewriteExprResult, error)
	RewriteLimit(limit *ast.Limit, explainContext Context) (RewriteLimitResult, error)
}
