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

import "github.com/scylladb/go-set/strset"

type ShardingTable struct {
	Name             string
	ShardingColumns  []string
	TableStrategy    ShardingStrategy
	DatabaseStrategy ShardingStrategy
	resources        map[string][]string //key: database, value: tables
	allTables        []string
}

func NoShardingTable() *ShardingTable {
	return &ShardingTable{
		TableStrategy: NoneShardingStrategy,
	}
}

func (t *ShardingTable) SetResources(resources map[string][]string) {
	r := resources
	if r == nil {
		r = make(map[string][]string, 0)
	}

	set := strset.New()
	for _, tables := range r {
		set.Add(tables...)
	}

	t.allTables = set.List()
	t.resources = r
}

//get physical database and tables
func (t *ShardingTable) GetResources() map[string][]string {
	return t.resources
}

//get all of the configured tables
func (t *ShardingTable) GetAllTables() []string {
	return t.allTables
}

func (t *ShardingTable) HasColumn(column string) bool {
	for _, column := range t.ShardingColumns {
		if TrimAndLower(column) == column {

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
