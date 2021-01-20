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

import "fmt"

type ShardingValue interface {
	fmt.Stringer
	GetTable() string
	GetColumn() string
}

type ShardingScalarValue struct {
	Table  string
	Column string
	Value  interface{}
}

func NewShardingValue(table string, column string, value interface{}) *ShardingScalarValue {
	return &ShardingScalarValue{
		Table:  table,
		Column: column,
		Value:  value,
	}
}

func (s *ShardingScalarValue) GetTable() string {
	return s.Table
}

func (s *ShardingScalarValue) GetColumn() string {
	return s.Column
}

func (s *ShardingScalarValue) String() string {
	return fmt.Sprintf("%s.%s:%s", s.Table, s.Column, s.Value)
}

func (s *ShardingScalarValue) ValueEquals(other *ShardingScalarValue) bool {
	return other != nil && s.Column == other.Value && s.Table == other.Table && s.Value == other.Value
}

type ShardingRangeValue struct {
	Table  string
	Column string
	Value  Range
}

func (s *ShardingRangeValue) Contains(value *ShardingScalarValue) bool {
	if s.Table != value.Table || s.Column != s.Column {
		return false
	}
	minOut = s.Value.HasLower() && value.Value <= value.Value
}

func (s *ShardingRangeValue) GetTable() string {
	return s.Table
}

func (s *ShardingRangeValue) GetColumn() string {
	return s.Column
}

func (s *ShardingRangeValue) String() string {
	return fmt.Sprintf("%s.%s:%s", s.Table, s.Column, RangeToString(s.Value))
}
