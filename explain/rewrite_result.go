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

var _ RewriteNodeResult = &rewriteNodeResult{}
var _ RewriteExprResult = &rewriteExprResult{}
var _ RewriteLimitResult = &rewriteLimitResult{}

var NoneRewriteNodeResult = ResultFromNode(nil, nil)
var NoneRewriteExprNodeResult = ResultFromExprNode(nil, nil, "")
var NoneRewriteLimitResult = ResultFromLimit(nil)
var NoneRewroteResult RewriteResult = &noneRewriteResult{}

type rewriteNodeResult struct {
	isRewrote     bool
	shardingTable *core.ShardingTable
	node          ast.Node
}

type rewriteExprResult struct {
	column string
	*rewriteNodeResult
}

type noneRewriteResult struct {
}

func (n *noneRewriteResult) IsRewrote() bool {
	return false
}

func (n *noneRewriteResult) Table() *core.ShardingTable {
	return nil
}

func (r *rewriteExprResult) GetShardingTable() string {
	if r.shardingTable != nil {
		return r.shardingTable.Name
	} else {
		return ""
	}
}

func (r *rewriteExprResult) GetColumn() string {
	return r.column
}

type rewriteLimitResult struct {
	*rewriteNodeResult
}

func (r *rewriteNodeResult) IsRewrote() bool {
	return r.isRewrote
}

func (r *rewriteNodeResult) Table() *core.ShardingTable {
	return r.shardingTable
}

func (r *rewriteNodeResult) GetNewNode() ast.Node {
	return r.node
}

func (r *rewriteExprResult) GetNewNode() ast.ExprNode {
	expr := r.node.(ast.ExprNode)
	return expr
}

func (r *rewriteLimitResult) GetNewNode() *ast.Limit {
	expr := r.node.(*ast.Limit)
	return expr
}

func ResultFromNode(node ast.Node, table *core.ShardingTable) *rewriteNodeResult {
	return &rewriteNodeResult{
		isRewrote:     node != nil,
		shardingTable: table,
		node:          node,
	}
}

func ResultFromExprNode(node ast.ExprNode, table *core.ShardingTable, column string) *rewriteExprResult {
	r := ResultFromNode(node, table)
	return &rewriteExprResult{
		rewriteNodeResult: r,
		column:            column,
	}
}

func ResultFromLimit(node *ast.Limit) *rewriteLimitResult {
	r := ResultFromNode(node, nil)
	return &rewriteLimitResult{
		r,
	}
}
