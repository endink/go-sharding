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

package script

import (
	"fmt"
	"github.com/XiaoMi/Gaea/core"
	"github.com/XiaoMi/Gaea/testkit"
	"github.com/emirpasic/gods/utils"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestFlatNoScript(t *testing.T) {
	expr := "ds_1,ds_2, ds_3"
	list := FlatInlineExpression(expr, t)
	assert.Equal(t, 3, len(list))

	assert.True(t, core.StringSliceEqual(list, []string{"ds_1", "ds_2", "ds_3"}))
}

func TestFlatOneDepth(t *testing.T) {
	expr := "ds_${range(1,3)}"
	list := FlatInlineExpression(expr, t)
	assert.Equal(t, 3, len(list))

	testkit.AssertStrArrayEquals(t, []string{"ds_1", "ds_2", "ds_3"}, list)
}

func TestFlatTwoDepth(t *testing.T) {
	expr := "ds_${range(1,3)}_t${range(2,3)}"
	list := FlatInlineExpression(expr, t)
	assert.Equal(t, 6, len(list))

}

func TestFlatThirdDepth(t *testing.T) {
	expr := "ds_${range(1,3)}_t${range(2,3)}_b${[5,6,7,8]}"
	list := FlatInlineExpression(expr, t)
	assert.Equal(t, 24, len(list))
}

func TestMultiFlatThirdDepth(t *testing.T) {
	expr := "ds_${range(1,3)}_t${range(2,3)}_b${[5,6,7,8]},es_${range(2,4)}_t${range(2,3)}_b${[5,6,7,8]}, ts_${range(3,5)}_t${range(2,3)}_b${[5,6,7,8]}"
	list := FlatInlineExpression(expr, t)
	assert.Equal(t, 72, len(list))
}

func TestDuplexMultiFlatThirdDepth(t *testing.T) {
	expr := "ds_${range(1,3)}_t${range(2,3)}_b${[5,6,7,8]}, ds_${range(3,4)}_t${range(2,3)}_b${[5,6,7,8]}"
	list := FlatInlineExpression(expr, t)
	assert.Equal(t, 32, len(list))
}

func printSorted(list []string) {
	arrayList := make([]interface{}, len(list))
	for i, s := range list {
		arrayList[i] = s
	}
	utils.Sort(arrayList, utils.StringComparator)

	for _, i2 := range arrayList {
		println(fmt.Sprint(i2))
	}
}

func GetInlineExpression(expression string, t *testing.T) InlineExpression {
	v, err := NewInlineExpression(expression)
	assert.Nil(t, err, "create inline expression fault: %s", expression)
	return v
}

func FlatInlineExpression(expression string, t *testing.T) []string {
	expr := GetInlineExpression(expression, t)
	list, err := expr.Flat()
	assert.Nil(t, err, "flat inline expression fault: %s", expression)
	return list
}
