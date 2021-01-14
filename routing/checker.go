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

package routing

import (
	"github.com/pingcap/parser/ast"
)

// Checker 用于检查SelectStmt是不是分表的Visitor, 以及是否包含DB信息
type Checker struct {
	context       *ShardingContext
	hasShardTable bool // 是否包含分片表
	dbInvalid     bool // SQL是否No database selected
	tableNames    []*ast.TableName
}

// NewChecker db为USE db中设置的DB名. 如果没有执行USE db, 则为空字符串
func NewChecker(context *ShardingContext) *Checker {
	return &Checker{
		hasShardTable: false,
		dbInvalid:     false,
	}
}

func (s *Checker) GetUnsharedTableNames() []*ast.TableName {
	return s.tableNames
}

// IsDatabaseInvalid 判断执行计划中是否包含db信息, 如果不包含, 且又含有表名, 则是一个错的执行计划, 应该返回以下错误:
// ERROR 1046 (3D000): No database selected
func (s *Checker) IsDatabaseInvalid() bool {
	return s.dbInvalid
}

// IsShard if is shard table
func (s *Checker) IsShard() bool {
	return s.hasShardTable
}

// Enter for node visit
func (s *Checker) Enter(n ast.Node) (node ast.Node, skipChildren bool) {
	if s.hasShardTable {
		return n, true
	}
	switch nn := n.(type) {
	case *ast.TableName:
		if s.isTableNameDatabaseInvalid(nn) {
			s.dbInvalid = true
			return n, true
		}
		has := s.hasShardTableInTableName(nn)
		if has {
			s.hasShardTable = true
			return n, true
		}
		s.tableNames = append(s.tableNames, nn)
	}
	return n, false
}

// Leave for node visit
func (s *Checker) Leave(n ast.Node) (node ast.Node, ok bool) {
	return n, !s.dbInvalid && !s.hasShardTable
}

// 不允许进行跨库查询
func (s *Checker) isTableNameDatabaseInvalid(n *ast.TableName) bool {
	return n.Schema.L != "" && s.context.GetSchema() != n.Schema.L
}

func (s *Checker) hasShardTableInTableName(n *ast.TableName) bool {
	table := n.Name.L
	return s.context.IsShardingTable(table)
}
