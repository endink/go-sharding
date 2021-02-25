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

import "github.com/XiaoMi/Gaea/core"

type simpleShardingProvider struct {
	shardingTables map[string]*core.ShardingTable
}

func NewShardingTableProvider(shardingTables map[string]*core.ShardingTable) ShardingTableProvider {
	return &simpleShardingProvider{shardingTables: shardingTables}
}

func MockShardingTableProvider(tables ...*ShardingTableMocked) ShardingTableProvider {
	mocked := &simpleShardingProvider{
		shardingTables: make(map[string]*core.ShardingTable),
	}
	for _, table := range tables {
		name := core.TrimAndLower(table.name)
		columns := core.TrimAndLowerArray(table.columns)

		if name != "" && len(columns) > 0 {
			sdt := core.MockShardingTableSimple(name, columns...)
			mocked.shardingTables[name] = sdt
		}
	}
	return mocked
}

func (m *simpleShardingProvider) GetShardingTable(table string) (*core.ShardingTable, bool) {
	name := core.TrimAndLower(table)
	sdt, ok := m.shardingTables[name]
	return sdt, ok
}

type ShardingTableMocked struct {
	name    string
	columns []string
}
