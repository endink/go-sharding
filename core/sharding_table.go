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

//配置参考：https://shardingsphere.apache.org/document/legacy/4.x/document/cn/manual/sharding-jdbc/configuration/config-yaml/

package core

import (
	"github.com/scylladb/go-set/strset"
)

type ShardingTable struct {
	Name             string
	TableStrategy    ShardingStrategy
	DatabaseStrategy ShardingStrategy
	tables           []string
	databases        []string
}

var NilShardingTable = &ShardingTable{
	TableStrategy:    NoneShardingStrategy,
	DatabaseStrategy: NoneShardingStrategy,
}

func (t *ShardingTable) SetResources(databases []string, tables []string) {
	dbSet := strset.New()
	dbSet.Add(databases...)

	tableSet := strset.New()
	tableSet.Add(tables...)

	t.databases = dbSet.List()
	t.tables = tableSet.List()
}

//get all of the configured tables
func (t *ShardingTable) GetDatabases() []string {
	return t.databases
}

//get all of the configured tables
func (t *ShardingTable) GetTables() []string {
	return t.tables
}

func (t *ShardingTable) HasDbShardingColumn(column string) bool {
	return t.IsDbSharding() && t.containsColumn(t.DatabaseStrategy.GetShardingColumns(), column)
}

func (t *ShardingTable) HasTableShardingColumn(column string) bool {
	return t.IsTableSharding() && t.containsColumn(t.TableStrategy.GetShardingColumns(), column)
}

func (t *ShardingTable) containsColumn(columns []string, column string) bool {
	c := TrimAndLower(column)
	for _, s := range columns {
		if s == c {
			return true
		}
	}
	return false
}

func (t *ShardingTable) IsDbSharding() bool {
	return t.DatabaseStrategy != NoneShardingStrategy
}

func (t *ShardingTable) IsTableSharding() bool {
	return t.DatabaseStrategy != NoneShardingStrategy
}

func (t *ShardingTable) IsSharding() bool {
	return t.IsDbSharding() || t.IsDbSharding()
}

func (t *ShardingTable) IsNil() bool {
	return t == NilShardingTable
}
