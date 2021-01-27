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
	"github.com/pingcap/parser/ast"
)

var _ RewriteNodeResult = &rewriteNodeResult{}
var _ RewriteExprResult = &rewriteExprResult{}
var _ RewriteLimitResult = &rewriteLimitResult{}

var NoneRewriteNodeResult = ResultFromNode(nil, "")
var NoneRewriteExprResult = ResultFromExrp(nil, "", "")
var NoneRewriteLimitResult = ResultFromLimit(nil)
var NoneRewriteResult RewriteResult = &noneRewriteResult{}

type rewriteNodeResult struct {
	isRewrote     bool
	shardingTable string
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

func (n *noneRewriteResult) GetShardingTable() string {
	return ""
}

func (r *rewriteNodeResult) GetShardingTable() string {
	return r.shardingTable
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

func ResultFromNode(node ast.Node, shardingTable string) *rewriteNodeResult {
	return &rewriteNodeResult{
		isRewrote:     node != nil,
		shardingTable: shardingTable,
		node:          node,
	}
}

func ResultFromExrp(node ast.ExprNode, shardingTable string, column string) *rewriteExprResult {
	r := ResultFromNode(node, shardingTable)
	return &rewriteExprResult{
		rewriteNodeResult: r,
		column:            column,
	}
}

func ResultFromLimit(node *ast.Limit) *rewriteLimitResult {
	r := ResultFromNode(node, "")
	return &rewriteLimitResult{
		r,
	}
}
