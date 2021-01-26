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

type ShardingStrategy interface {
	GetShardingColumns() []string

	IsScalarValueSupported() bool
	IsRangeValueSupported() bool

	Shard(sources []string, values *ShardingValues) ([]string, error)
}

func RequireAllShard(s ShardingStrategy, values *ShardingValues) bool {
	//TODO: 是否考虑优化交集结果为空的情况，改用 values.HasScalar, values.HasRange 判断
	columns := s.GetShardingColumns()
	colLength := len(columns)
	for _, column := range columns {
		if (s.IsRangeValueSupported() || !values.HasEffectiveRange(column)) &&
			(s.IsScalarValueSupported() || !values.HasEffectiveScalar(column)) &&
			(values.Logic(column) == LogicAnd || colLength == 1) {
			continue
		} else {
			return true
		}
	}
	return false
}
