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

import "errors"

func (b *ShardingValuesBuilder) Merge(other *ShardingValuesBuilder, op BinaryLogic) error {

	if op != LogicAnd && op != LogicOr {
		return errors.New("unknown ShardingValuesBuilder operation, support are and, or")
	}

	b.valueSync.Lock()
	defer b.valueSync.Unlock()

	b.mergeScalar(other, op)
	if err := b.merRange(other, op); err != nil {
		return err
	}

	other.columnLogic.Range(func(column, _ interface{}) bool {
		b.setColumnLogic(column.(string), op)
		return true
	})
	return nil
}

func (b *ShardingValuesBuilder) merRange(other *ShardingValuesBuilder, op BinaryLogic) error {
	var err error
	for column, rangeValues := range other.rangeValues {
		actualOp := op
		if !b.hasValue(column) {
			actualOp = LogicOr
		}
		valeCount := rangeValues.Size()
		totalCount := other.rangeCounter[column]
		if !rangeValues.Empty() {
			ranges, e := b.getRanges(rangeValues)

			if e != nil {
				return e
			}
			switch actualOp {
			case LogicAnd:
				err = b.andRangeWithLock(column, false, ranges...)
			case LogicOr:
				err = b.orRangeWithLock(column, false, ranges...)
			}
		}
		b.increaseRange(column, totalCount-valeCount) //补正实际数量
	}
	return err
}

func (b *ShardingValuesBuilder) mergeScalar(other *ShardingValuesBuilder, op BinaryLogic) {
	for column, scalarValues := range other.scalarValues {
		actualOp := op
		if !b.hasValue(column) {
			actualOp = LogicOr
		}
		valeCount := scalarValues.Size()
		totalCount := other.scalarCounter[column]
		if !scalarValues.Empty() {
			switch actualOp {
			case LogicAnd:
				b.andValueWithLock(column, false, scalarValues.Values()...)
			case LogicOr:
				b.orValueWithLock(column, false, scalarValues.Values()...)
			}
		}
		b.increaseScalar(column, totalCount-valeCount) //补正实际数量
	}
}
