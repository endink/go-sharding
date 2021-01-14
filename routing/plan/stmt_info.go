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

package plan

import (
	"fmt"
	"github.com/XiaoMi/Gaea/core"
	"github.com/XiaoMi/Gaea/routing"
	"github.com/pingcap/parser/ast"
)

// StmtInfo 各种Plan的一些公共属性
type StmtInfo struct {
	sql        string // origin sql
	context    *routing.ShardingContext
	tableRules map[string]*core.ShardingTable // key = table name, value = router.Rule, 记录使用到的分片表
}

// NewStmtInfo constructor of StmtInfo
func NewStmtInfo(sql string, context *routing.ShardingContext) *StmtInfo {
	return &StmtInfo{
		sql:        sql,
		context:    context,
		tableRules: make(map[string]*core.ShardingTable),
	}
}

// RecordShardTable 将表信息记录到StmtInfo中, 并返回表信息对应的路由规则
func (s *StmtInfo) AddTable(n *ast.TableName) (*core.ShardingTable, error) {
	db, table := getTableInfoFromTableName(n)
	if err := s.validateDatabase(db); err != nil {
		return nil, err
	}
	shardingTable := s.context.GetShardingTable(table)

	s.tableRules[table] = shardingTable
	return shardingTable, nil
}

func (s *StmtInfo) validateDatabase(db string) error {
	if db != "" && db != s.context.Settings.Server.Schema {
		return fmt.Errorf("cross database is not supported")
	}
	return nil
}
