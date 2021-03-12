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

type ShardType int

const (
	ShardScatter ShardType = iota
	ShardAll
	ShardImpossible
)

type ShardingStrategy interface {
	GetShardingColumns() []string

	IsScalarValueSupported() bool
	IsRangeValueSupported() bool

	Shard(sources []string, values *ShardingValues) ([]string, error)
}

//decide execute sql to every shard
func DetectShardType(s ShardingStrategy, values *ShardingValues, hasFullShardColumn bool) ShardType {
	columns := s.GetShardingColumns()

	isImpossible := func(col string) bool {
		if hasFullShardColumn {
			return false
		}

		return (s.IsRangeValueSupported() && !values.HasEffectiveRange(col) && !values.HasRange(col)) || (s.IsScalarValueSupported() && !values.HasEffectiveScalar(col) && values.HasScalar(col))
	}

	hasEffective := func(col string) bool {
		return (s.IsRangeValueSupported() && values.HasEffectiveRange(col)) || (s.IsScalarValueSupported() && values.HasEffectiveScalar(col))
	}

	for _, column := range columns {
		if isImpossible(column) && (values.Logic(column) == LogicAnd) {
			return ShardImpossible
		}
		if hasEffective(column) && (values.Logic(column) == LogicAnd || !hasFullShardColumn) {
			return ShardScatter
		}
	}
	return ShardAll
}
