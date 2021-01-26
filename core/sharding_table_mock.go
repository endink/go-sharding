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

package core

var _ ShardingStrategy = &mockedShardingStrategy{}

func MockShardingTable(
	name string,
	databases []string,
	physicalTables []string,
	shardingColumns []string) *ShardingTable {

	strategy := mockShardingStrategy(shardingColumns, true, true)

	return &ShardingTable{
		Name:             name,
		TableStrategy:    strategy,
		DatabaseStrategy: strategy,
		tables:           physicalTables,
		databases:        databases,
	}
}

func mockShardingStrategy(columns []string, supportScalar bool, supportRange bool) *mockedShardingStrategy {
	return &mockedShardingStrategy{
		columns:       columns,
		supportScalar: supportScalar,
		supportRange:  supportRange,
	}
}

type mockedShardingStrategy struct {
	columns       []string
	supportScalar bool
	supportRange  bool
}

func (f *mockedShardingStrategy) GetShardingColumns() []string {
	return f.columns
}

func (f *mockedShardingStrategy) IsScalarValueSupported() bool {
	return f.supportScalar
}

func (f *mockedShardingStrategy) IsRangeValueSupported() bool {
	return f.supportRange
}

func (f *mockedShardingStrategy) Shard(sources []string, values *ShardingValues) ([]string, error) {
	return sources, nil
}
