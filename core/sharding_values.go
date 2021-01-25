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

type ShardingValues struct {
	TableName    string
	ScalarValues map[string][]interface{} //key: column, value: values
	RangeValues  map[string][]Range
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
	return values.IsEmptyScalars() && values.IsEmptyRanges()
}

func (values *ShardingValues) IsEmptyScalars() bool {
	return len(values.ScalarValues) == 0
}

func (values *ShardingValues) IsEmptyRanges() bool {
	return len(values.RangeValues) == 0
}

func (values *ShardingValues) HasScalarColumn(column string) bool {
	if values.ScalarValues == nil {
		return false
	}
	_, ok := values.ScalarValues[column]
	return ok
}

func (values *ShardingValues) HasRangeColumn(column string) bool {
	if values.RangeValues == nil {
		return false
	}
	_, ok := values.RangeValues[column]
	return ok
}
