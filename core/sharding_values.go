/*
 *
 *  * Copyright 2021. Go-Sharding Author All Rights Reserved.
 *  *
 *  *  Licensed under the Apache License, Version 2.0 (the "License");
 *  *  you may not use this file except in compliance with the License.
 *  *  You may obtain a copy of the License at
 *  *
 *  *      http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  *  Unless required by applicable law or agreed to in writing, software
 *  *  distributed under the License is distributed on an "AS IS" BASIS,
 *  *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  *  See the License for the specific language governing permissions and
 *  *  limitations under the License.
 *  *
 *  *  File author: Anders Xiao
 *
 */

package core

type BinaryLogic int

const (
	LogicAnd BinaryLogic = iota
	LogicOr
)

func (l BinaryLogic) String() string {
	switch l {
	case LogicAnd:
		return "and"
	case LogicOr:
		return "or"
	}
	return ""
}

type ShardingValues struct {
	TableName        string
	ScalarValues     map[string][]interface{} //key: column, value: values
	RangeValues      map[string][]Range
	rangeCount       map[string]int
	scalarCount      map[string]int
	totalRangeCount  int
	totalScalarCount int
	columnLogic      map[string]BinaryLogic
}

func ShardingValuesForSingleScalar(tableName string, column string) *ShardingValues {
	values := make([]interface{}, 1)
	scalar := map[string][]interface{}{column: values}

	return &ShardingValues{
		TableName:    tableName,
		ScalarValues: scalar,
	}
}

func (values *ShardingValues) IsEmpty() bool {
	return values.totalRangeCount == 0 && values.totalScalarCount == 0
}

func (values *ShardingValues) HasEffectiveScalar(column string) bool {
	if v, ok := values.ScalarValues[column]; ok {
		return len(v) > 0
	}
	return false
}

func (values *ShardingValues) HasEffectiveRange(column string) bool {
	if v, ok := values.RangeValues[column]; ok {
		return len(v) > 0
	}
	return false
}

func (values *ShardingValues) EffectiveScalarCount(column string) int {
	c := 0
	if v, ok := values.ScalarValues[column]; ok {
		c = len(v)
	}
	return c
}

func (values *ShardingValues) EffectiveRangeCount(column string) int {
	c := 0
	if v, ok := values.RangeValues[column]; ok {
		c = len(v)
	}
	return c
}

func (values *ShardingValues) HasScalar(column string) bool {
	if v, ok := values.scalarCount[column]; ok {
		return v > 0
	}
	return false
}

func (values *ShardingValues) HasRange(column string) bool {
	if v, ok := values.rangeCount[column]; ok {
		return v > 0
	}
	return false
}

func (values *ShardingValues) Logic(column string) BinaryLogic {
	if lg, ok := values.columnLogic[column]; ok {
		return lg
	}
	return LogicAnd
}

func (values *ShardingValues) ScalarCount(column string) int {
	if values.scalarCount == nil {
		return 0
	}
	if v, ok := values.scalarCount[column]; ok {
		return v
	}
	return 0
}

func (values *ShardingValues) RangeCount(column string) int {
	if values.rangeCount == nil {
		return 0
	}
	if v, ok := values.rangeCount[column]; ok {
		return v
	}
	return 0
}

func (values *ShardingValues) TotalScalarCount() int {
	return values.totalScalarCount
}

func (values *ShardingValues) TotalRangeCount() int {
	return values.totalRangeCount
}

func (values *ShardingValues) HasScalarColumn(column string) bool {
	if values.scalarCount == nil {
		return false
	}
	_, ok := values.scalarCount[column]
	return ok
}

func (values *ShardingValues) HasRangeColumn(column string) bool {
	if values.rangeCount == nil {
		return false
	}
	_, ok := values.rangeCount[column]
	return ok
}
