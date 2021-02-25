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

import "github.com/pingcap/parser/ast"

var _ RewriteFormattedResult = &rewriteFormattedResult{}
var _ RewriteLimitResult = &rewriteLimitResult{}

var NoneRewriteFormattedResult = ResultFromFormatter(nil, "", "")
var NoneRewriteLimitResult = ResultFromLimit(-1)
var NoneRewriteResult RewriteResult = &rewriteResultBase{isRewrote: false}
var NoneRewriteBindVarsResult RewriteBindVarsResult = &rewriteVarsResult{rewroteParams: nil}

type rewriteResultBase struct {
	isRewrote     bool
	shardingTable string
}

func (r *rewriteResultBase) IsRewrote() bool {
	return r.isRewrote
}

func (r *rewriteResultBase) GetShardingTable() string {
	return r.shardingTable
}

type rewriteFormattedResult struct {
	*rewriteResultBase
	column string
	f      StatementFormatter
}

func (r *rewriteFormattedResult) GetColumn() string {
	return r.column
}

func (r *rewriteFormattedResult) GetFormatter() StatementFormatter {
	return r.f
}

type rewriteLimitResult struct {
	*rewriteResultBase
	count int64
}

func (r *rewriteLimitResult) GetLimit() int64 {
	return r.count
}

func ResultFromNode(node ast.Node, shardingTable string, column string) RewriteFormattedResult {
	formatter := &nodeFormatter{node}
	return ResultFromFormatter(formatter, shardingTable, column)
}

func ResultFromFormatter(formatter StatementFormatter, shardingTable string, column string) RewriteFormattedResult {
	base := &rewriteResultBase{
		isRewrote:     formatter != nil,
		shardingTable: shardingTable,
	}
	return &rewriteFormattedResult{
		rewriteResultBase: base,
		f:                 formatter,
		column:            column,
	}
}

func ResultFromLimit(count int64) *rewriteLimitResult {
	base := &rewriteResultBase{
		isRewrote: count >= 0,
	}
	return &rewriteLimitResult{
		rewriteResultBase: base,
		count:             count,
	}
}

type rewriteVarsResult struct {
	rewroteParams []string
	scatterParams map[string][]string
}

func (r *rewriteVarsResult) RewroteVariables() []string {
	return r.rewroteParams
}

func (r *rewriteVarsResult) ScatterVariables() map[string][]string {
	return r.scatterParams
}

func (r *rewriteVarsResult) IsRewrote() bool {
	return len(r.rewroteParams) > 0
}

func ResultFromScatterVars(rewroteVariables []string, scatterVariables map[string][]string) RewriteBindVarsResult {
	r := &rewriteVarsResult{
		rewroteParams: rewroteVariables,
		scatterParams: scatterVariables,
	}
	return r
}
