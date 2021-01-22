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

import (
	"github.com/XiaoMi/Gaea/testkit"
	"github.com/stretchr/testify/assert"
	"testing"
)

const testColumn = "ok"
const testTable = "Table"

func TestBuild(t *testing.T) {
	builder := NewShardingValuesBuilder(testTable)
	var err error

	builder.AndValue(testColumn, 2, 3)
	builder.OrValue(testColumn, 4, 5, 6)

	r := createRange(7, 10)
	err = builder.OrRange(testColumn, r)
	assert.Nil(t, err)

	values := builder.Build()

	assert.Equal(t, testTable, values.TableName)
	testkit.AssertArrayEquals(t, []interface{}{2, 3, 4, 5, 6}, values.ScalarValues[testColumn])
	assertRangesEquals(t, []Range{r}, values.RangeValues[testColumn])
}

func TestAndScalarToScalar(t *testing.T) {
	builder := NewShardingValuesBuilder(testTable)

	builder.OrValue(testColumn, 3, 4, 5, 6)
	builder.AndValue(testColumn, 5)
	values := builder.Build()
	testkit.AssertArrayEquals(t, []interface{}{5}, values.ScalarValues[testColumn])

	builder.Reset()

	builder.OrValue(testColumn, 4, 5, 6)
	builder.AndValue(testColumn, 1, 2, 3)
	values = builder.Build()
	testkit.AssertEmptyArray(t, values.ScalarValues[testColumn])

	builder.Reset()

	builder.OrValue(testColumn, 4, 5, 6)
	builder.AndValue(testColumn, 1, 2, 3, 4, 5)
	values = builder.Build()
	testkit.AssertArrayEquals(t, []interface{}{4, 5}, values.ScalarValues[testColumn])

	builder.Reset()

	builder.OrValue(testColumn, 4, 5, 6)
	builder.AndValue(testColumn, 5, 6, 7, 8, 9)
	values = builder.Build()
	testkit.AssertArrayEquals(t, []interface{}{5, 6}, values.ScalarValues[testColumn])

	builder.Reset()

	builder.OrValue(testColumn, 4, 5, 6)
	builder.AndValue(testColumn, 4, 5, 6)
	values = builder.Build()
	testkit.AssertArrayEquals(t, []interface{}{4, 5, 6}, values.ScalarValues[testColumn])

}

func TestAndScalarToRange1(t *testing.T) {
	builder := NewShardingValuesBuilder(testTable)

	/*
		a = 3 or a = 4 or a = 5 or (a >= 10 and a <= 100)
		and
		a = 13

		result: a = 13
	*/

	builder.AndValue(testColumn, 3)
	builder.OrValue(testColumn, 4, 5)
	err := builder.OrRange(testColumn, createRange(10, 100))
	assert.Nil(t, err)

	builder.AndValue(testColumn, 13)

	values := builder.Build()

	testkit.AssertArrayEquals(t, []interface{}{13}, values.ScalarValues[testColumn])
	assertEmptyRanges(t, values.RangeValues[testColumn])
}

func TestAndScalarToRange2(t *testing.T) {
	builder := NewShardingValuesBuilder(testTable)

	/*
		a = 3 or a = 4 or a = 5 or (a >= 10 and a <= 100)
		and
		a = 5 or a = 6 or a = 13

		result: a = 13 or a = 14
	*/

	builder.AndValue(testColumn, 3)
	builder.OrValue(testColumn, 4, 5)
	err := builder.OrRange(testColumn, createRange(10, 100))
	assert.Nil(t, err)

	builder.AndValue(testColumn, 13, 14)

	values := builder.Build()

	testkit.AssertArrayEquals(t, []interface{}{13, 14}, values.ScalarValues[testColumn])
	assertEmptyRanges(t, values.RangeValues[testColumn])
}

func TestAndScalarToRange3(t *testing.T) {
	builder := NewShardingValuesBuilder(testTable)

	/*
		a = 3 or a = 4 or a = 5 or (a >= 10 and a <= 100)
		and
		a = 5 or a = 6 or a = 13 or a= 14 or a = 15

		result: a= 5 or a = 13 or a = 14 or a = 15
	*/

	builder.AndValue(testColumn, 3)
	builder.OrValue(testColumn, 4, 5)
	err := builder.OrRange(testColumn, createRange(10, 100))
	assert.Nil(t, err)

	builder.AndValue(testColumn, 2, 3, 5, 6, 13, 14, 15)

	values := builder.Build()

	testkit.AssertArrayEquals(t, []interface{}{3, 5, 13, 14, 15}, values.ScalarValues[testColumn])
	assertEmptyRanges(t, values.RangeValues[testColumn])
}

func TestAndScalarToRange4(t *testing.T) {
	builder := NewShardingValuesBuilder(testTable)

	/*
		a = 3 or a = 4 or a = 5 or (a >= 10 and a <= 100) or (a >= 200 and a <= 300)
		and
		a in (2, 3, 5, 6, 10, 13, 14, 15, 99, 108, 200, 201, 300)

		result: a in (3, 5, 10, 13, 14, 15, 99, 200, 201, 300)
	*/

	builder.AndValue(testColumn, 3)
	builder.OrValue(testColumn, 4, 5)
	err := builder.OrRange(testColumn, createRange(10, 100))
	assert.Nil(t, err)
	err = builder.OrRange(testColumn, createRange(200, 300))
	assert.Nil(t, err)

	builder.AndValue(testColumn, 2, 3, 5, 6, 10, 13, 14, 15, 99, 108, 200, 201, 300)

	values := builder.Build()

	testkit.AssertArrayEquals(t, []interface{}{3, 5, 10, 13, 14, 15, 99, 200, 201, 300}, values.ScalarValues[testColumn])
	assertEmptyRanges(t, values.RangeValues[testColumn])
}

func TestAndScalarToRange5(t *testing.T) {
	builder := NewShardingValuesBuilder(testTable)

	/*
		a = 3 or a = 4 or a = 5 or (a >= 10 and a <= 100) or (a >= 200 and a <= 300)
		and
		a = 400 or a = 500

		result: a = nobody
	*/

	builder.AndValue(testColumn, 3)
	builder.OrValue(testColumn, 4, 5)
	err := builder.OrRange(testColumn, createRange(10, 100))
	assert.Nil(t, err)
	err = builder.OrRange(testColumn, createRange(200, 300))
	assert.Nil(t, err)

	builder.AndValue(testColumn, 400, 500)

	values := builder.Build()

	testkit.AssertEmptyArray(t, values.ScalarValues[testColumn])
	assertEmptyRanges(t, values.RangeValues[testColumn])
}

func TestAndScalarToRange6(t *testing.T) {
	builder := NewShardingValuesBuilder(testTable)

	/*
		a = 3 or a = 4 or a = 5 or (a >= 10 and a <= 100) or (a >= 200 and a <= 300)
		and
		a = 3 or a = 4 or a = 5

		result: a in (3, 4, 5)
	*/

	builder.AndValue(testColumn, 3)
	builder.OrValue(testColumn, 4, 5)
	err := builder.OrRange(testColumn, createRange(10, 100))
	assert.Nil(t, err)
	err = builder.OrRange(testColumn, createRange(200, 300))
	assert.Nil(t, err)

	builder.AndValue(testColumn, 3, 4, 5)

	values := builder.Build()

	testkit.AssertArrayEquals(t, []interface{}{3, 4, 5}, values.ScalarValues[testColumn])
	assertEmptyRanges(t, values.RangeValues[testColumn])
}

func TestAndRangeToScalar1(t *testing.T) {
	/*
		a in (1 ~ 10)
		and
		a >= 2 and a <= 5

		result: a in (2, 3, 4, 5)
	*/
	builder := NewShardingValuesBuilder(testTable)
	var err error
	builder.AndValue(testColumn, intArray(1, 10)...)

	err = builder.AndRange(testColumn, createRange(2, 5))
	assert.Nil(t, err)

	values := builder.Build()
	testkit.AssertArrayEquals(t, []interface{}{2, 3, 4, 5}, values.ScalarValues[testColumn])
	assertEmptyRanges(t, values.RangeValues[testColumn])
}

func TestAndRangeToScalar2(t *testing.T) {
	/*
		a in (1 ~ 10)
		and
		a >= 11 and a <= 20

		result: a = nobody
	*/
	builder := NewShardingValuesBuilder(testTable)
	var err error
	builder.AndValue(testColumn, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10)

	err = builder.AndRange(testColumn, createRange(11, 20))
	assert.Nil(t, err)

	values := builder.Build()
	testkit.AssertEmptyArray(t, values.ScalarValues[testColumn])
	assertEmptyRanges(t, values.RangeValues[testColumn])
}

func TestAndRangeToScalar3(t *testing.T) {
	/*
		a in (1 ~ 10)
		and
		a >= -2 and a <= 8

		result: a in ( 1~8 )
	*/
	builder := NewShardingValuesBuilder(testTable)
	var err error
	builder.AndValue(testColumn, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10)

	err = builder.AndRange(testColumn, createRange(-2, 8))
	assert.Nil(t, err)

	values := builder.Build()
	testkit.AssertArrayEquals(t, intArray(1, 8), values.ScalarValues[testColumn])
	assertEmptyRanges(t, values.RangeValues[testColumn])
}

func TestAndRangeToScalar4(t *testing.T) {
	/*
		a in (1 ~ 10)
		and
		a >= 8 and a <= 12

		result: a in ( 8, 9, 10 )
	*/
	builder := NewShardingValuesBuilder(testTable)
	var err error
	builder.AndValue(testColumn, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10)

	err = builder.AndRange(testColumn, createRange(8, 12))
	assert.Nil(t, err)

	values := builder.Build()
	testkit.AssertArrayEquals(t, intArray(8, 10), values.ScalarValues[testColumn])
	assertEmptyRanges(t, values.RangeValues[testColumn])
}

func TestAndRangeToScalar5(t *testing.T) {
	/*
		a in (1 ~ 10)
		and
		a >= 1 and a <= 10

		result: a in ( 1~10 )
	*/
	builder := NewShardingValuesBuilder(testTable)
	var err error
	builder.AndValue(testColumn, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10)

	err = builder.AndRange(testColumn, createRange(1, 10))
	assert.Nil(t, err)

	values := builder.Build()
	testkit.AssertArrayEquals(t, intArray(1, 10), values.ScalarValues[testColumn])
	assertEmptyRanges(t, values.RangeValues[testColumn])
}

func TestAndRangeToRange1(t *testing.T) {
	/*
		a in (2, 4, 6, 8, 10) or a in (20 ~ 30) or a in (50 ~ 60)
		and
		a in (5~55)

		result: a in ( 2, 4, 6, 8, 10 ) or a in (20 ~ 30) or a in (50~55)
	*/
	builder := NewShardingValuesBuilder(testTable)
	var err error
	builder.OrValue(testColumn, 2, 4, 6, 8, 10)

	err = builder.OrRange(testColumn, createRange(20, 30))
	assert.Nil(t, err)
	err = builder.OrRange(testColumn, createRange(50, 60))
	assert.Nil(t, err)

	err = builder.AndRange(testColumn, createRange(5, 55))
	assert.Nil(t, err)

	values := builder.Build()
	testkit.AssertArrayEquals(t, []interface{}{6, 8, 10}, values.ScalarValues[testColumn])
	assertRangesEquals(t, []Range{createRange(20, 30), createRange(50, 55)}, values.RangeValues[testColumn])
}

func TestAndRangeToRange2(t *testing.T) {
	/*
		a in (2, 4, 6, 8, 10) or a in (20 ~ 30) or a in (50 ~ 60)
		and
		a in (1~100)

		result: a in (2, 4, 6, 8, 10) or a in (20 ~ 30) or a in (50 ~ 60)
	*/
	builder := NewShardingValuesBuilder(testTable)
	var err error
	builder.OrValue(testColumn, 2, 4, 6, 8, 10)

	err = builder.OrRange(testColumn, createRange(20, 30))
	assert.Nil(t, err)
	err = builder.OrRange(testColumn, createRange(50, 60))
	assert.Nil(t, err)

	err = builder.AndRange(testColumn, createRange(1, 100))
	assert.Nil(t, err)

	values := builder.Build()
	testkit.AssertArrayEquals(t, []interface{}{2, 4, 6, 8, 10}, values.ScalarValues[testColumn])
	assertRangesEquals(t, []Range{createRange(20, 30), createRange(50, 60)}, values.RangeValues[testColumn])
}

func TestAndRangeToRange3(t *testing.T) {
	/*
		a in (2, 4, 6, 8, 10) or a in (20 ~ 30) or a in (50 ~ 60)
		and
		a in (25~55)

		result: a in a in (25 ~ 30) or a in (50 ~ 55)
	*/
	var err error
	builder := NewShardingValuesBuilder(testTable)
	builder.OrValue(testColumn, 2, 4, 6, 8, 10)

	err = builder.OrRange(testColumn, createRange(20, 30))
	assert.Nil(t, err)
	err = builder.OrRange(testColumn, createRange(50, 60))
	assert.Nil(t, err)

	err = builder.AndRange(testColumn, createRange(25, 55))
	assert.Nil(t, err)

	values := builder.Build()
	testkit.AssertEmptyArray(t, values.ScalarValues[testColumn])
	assertRangesEquals(t, []Range{createRange(25, 30), createRange(50, 55)}, values.RangeValues[testColumn])
}

func TestUnionOptimization1(t *testing.T) {
	/*************** 并集优化，移除多余的明确值 **************/
	/*
		a in (2, 4, 6, 8, 10)
		or
		a in (1~10)

		result: a in (1~10)
	*/

	var err error
	builder := NewShardingValuesBuilder(testTable)
	builder.OrValue(testColumn, 2, 4, 6, 8, 10)

	err = builder.OrRange(testColumn, createRange(1, 10))
	assert.Nil(t, err)

	values := builder.Build()

	testkit.AssertEmptyArray(t, values.ScalarValues[testColumn])
	assertRangesEquals(t, []Range{createRange(1, 10)}, values.RangeValues[testColumn])
}

func TestUnionOptimization2(t *testing.T) {
	/*************** 并集优化，移除多余的明确值 **************/
	/*
		a in (2, 4, 6, 8, 10)
		or
		a in (-8~5)

		result: a in (-8~5) or a in (6, 8, 10)
	*/

	var err error
	builder := NewShardingValuesBuilder(testTable)
	builder.OrValue(testColumn, 2, 4, 6, 8, 10)

	err = builder.OrRange(testColumn, createRange(-8, 5))
	assert.Nil(t, err)

	values := builder.Build()

	testkit.AssertArrayEquals(t, []interface{}{6, 8, 10}, values.ScalarValues[testColumn])
	assertRangesEquals(t, []Range{createRange(-8, 5)}, values.RangeValues[testColumn])
}

func TestUnionOptimization3(t *testing.T) {
	/*************** 并集优化，移除多余的明确值 **************/
	/*
		a in (2, 4, 6, 8, 10)
		or
		a in (5~20)

		result: a in (5~20) or a in (2, 4)
	*/

	var err error
	builder := NewShardingValuesBuilder(testTable)
	builder.OrValue(testColumn, 2, 4, 6, 8, 10)

	err = builder.OrRange(testColumn, createRange(5, 20))
	assert.Nil(t, err)

	values := builder.Build()

	testkit.AssertArrayEquals(t, []interface{}{2, 4}, values.ScalarValues[testColumn])
	assertRangesEquals(t, []Range{createRange(5, 20)}, values.RangeValues[testColumn])
}

func TestUnionOptimization4(t *testing.T) {
	/*************** 并集优化，移除多余的明确值 **************/
	/*
		a in (2, 4, 6, 8, 10)
		or
		a in (1~100)

		result: a in (1~100)
	*/

	var err error
	builder := NewShardingValuesBuilder(testTable)
	builder.OrValue(testColumn, 2, 4, 6, 8, 10)

	err = builder.OrRange(testColumn, createRange(1, 100))
	assert.Nil(t, err)

	values := builder.Build()

	testkit.AssertEmptyArray(t, values.ScalarValues[testColumn])
	assertRangesEquals(t, []Range{createRange(1, 100)}, values.RangeValues[testColumn])
}

func TestUnionOptimization5(t *testing.T) {
	/*************** 并集优化，移除多余的明确值 **************/
	/*
		a in (2, 4, 6, 8, 10)
		or
		a in (5~8)

		result: a in (5~8) or a = 2 or a= 4 or a = 10
	*/

	var err error
	builder := NewShardingValuesBuilder(testTable)
	builder.OrValue(testColumn, 2, 4, 6, 8, 10)

	err = builder.OrRange(testColumn, createRange(5, 8))
	assert.Nil(t, err)

	values := builder.Build()

	testkit.AssertArrayEquals(t, []interface{}{2, 4, 10}, values.ScalarValues[testColumn])
	assertRangesEquals(t, []Range{createRange(5, 8)}, values.RangeValues[testColumn])
}

func createRange(min interface{}, max interface{}) Range {
	r, err := NewRange(min, max)
	if err != nil {
		panic(err)
	}
	return r
}

func assertEmptyRanges(t *testing.T, actual []Range, msg ...interface{}) bool {
	return assertRangesEquals(t, nil, actual, msg...)
}

func assertRangesEquals(t *testing.T, excepted []Range, actual []Range, msg ...interface{}) bool {
	return testkit.AssertArrayEquals(t, convertArray(excepted), convertArray(actual), msg...)
}

func convertArray(ranges []Range) []interface{} {
	if ranges == nil {
		return nil
	}
	array := make([]interface{}, 0, len(ranges))
	for _, r := range ranges {
		array = append(array, r)
	}
	return array
}

func intArray(from int, to int) []interface{} {
	array := make([]interface{}, 0, to-from)
	for i := from; i <= to; i++ {
		array = append(array, i)
	}
	return array
}
