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
	"fmt"
	"github.com/XiaoMi/Gaea/core/collection"
)

func (b *ShardingValuesBuilder) orValueWithLock(column string, lock bool, values ...interface{}) {
	valueCount := len(values)
	if column != "" && valueCount > 0 {
		b.setColumnLogic(column, LogicOr)
		defer func() { b.increaseScalar(column, valueCount) }()
		if lock {
			b.valueSync.Lock()
			defer b.valueSync.Unlock()
		}
		set := b.getOrCreateScalarValueSet(column)
		set.Add(values...)
	}
}

func (b *ShardingValuesBuilder) andValueWithLock(column string, lock bool, values ...interface{}) {
	valueCount := len(values)
	if column != "" && valueCount > 0 {
		b.setColumnLogic(column, LogicAnd)
		if lock {
			b.valueSync.Lock()
			defer b.valueSync.Unlock()
		}

		//首先找到和集合的交集明确值
		var rValues []interface{}
		//这里无需故关心有没有设置过 range 值，如果没有可能由于 and 过明确值，处理明确值即可
		if set, ok := b.rangeValues[column]; ok && !set.Empty() {
			rValues = b.intersectRangeAndValues(set, values)
			//明确值的交集将导致 range 失效
			if !set.Empty() { //性能考虑，为 0 时不再重新分配内存
				set.Clear()
			}
		}

		//与现有明确值计算交集
		if _, ok := b.scalarCounter[column]; !ok {
			//首次没有值时可能, 标记已经投入过值，先投入一个
			b.orValueWithLock(column, false, values[0])
			b.increaseScalar(column, -1) //移除多计算的数量
			if valueCount > 1 {
				b.intersect(column, values[1:])
			}
		} else {
			b.intersect(column, values)
		}

		//并入集合交集值
		b.orValueWithLock(column, false, rValues...)
		b.increaseScalar(column, -len(rValues)) //移除多计算的数量

		b.increaseScalar(column, valueCount)
	}
}

func (b *ShardingValuesBuilder) andSingleRange(column string, value Range) error {
	defer func() { b.increaseRange(column, 1) }()

	b.setColumnLogic(column, LogicAnd)

	//首次处理值时快速添加，无需复杂优化
	if !b.hasRangeValue(column) && !b.hasScalarValue(column) {
		set := b.getOrCreateRageValueSet(column)
		set.Add(value)
		return nil
	}

	/*
		对于新加入的条件 a < 100 考虑如下逻辑：
		得到如下逻辑： (a = 152 or a = 2 or (a > 140 and a < 150)) and a < 145
		算法：
		剔除新加入的范围中没有交集的所有明确值
		对已存在的范围求交集，如果没有交集进行移除，得以下逻辑：
		a = 2 or (a > 140 and a < 145)
	*/

	//筛选明确的值
	if scalarSet, hasScalar := b.scalarValues[column]; hasScalar && !scalarSet.Empty() {
		values, err := scalarSet.Select(func(item interface{}) (bool, error) {
			return value.ContainsValue(item)
		})
		if err != nil {
			return err
		}
		b.scalarValues[column] = collection.NewHashSet(values...)
	}

	//求范围交集
	set := b.getOrCreateRageValueSet(column)

	var err error
	found := set.Select(func(index int, item interface{}) bool {
		if err != nil { //如果已经有错误，无需继续查找
			return false
		}
		rangItem := item.(Range)
		has, e := rangItem.HasIntersection(value)
		if e != nil {
			if err != nil {
				err = fmt.Errorf("%s intersect %s fault", rangItem, value)
			}
		}
		return e == nil && has
	})

	if err != nil {
		return err
	}

	if found.Empty() {
		set.Clear()
	} else {
		result := make([]Range, 0, found.Size())
		found.All(func(index int, item interface{}) bool {
			rangItem := item.(Range)
			var newRange Range
			newRange, err = rangItem.Intersect(value) //前面判断过，这里一定有交集
			if err != nil {
				return false
			}
			result = append(result, newRange)
			return true
		})

		set.Clear()
		for _, rng := range result {
			set.Add(rng)
		}
	}
	return nil
}

func (b *ShardingValuesBuilder) orSingleRange(column string, value Range) error {
	defer func() { b.increaseRange(column, 1) }()

	b.setColumnLogic(column, LogicOr)

	scalarValues := b.getOrCreateScalarValueSet(column)

	//范围内已经覆盖的值进行移除优化
	del, err := scalarValues.Select(func(item interface{}) (bool, error) {
		return value.ContainsValue(item)
	})

	if err != nil {
		return err
	}

	for _, del := range del {
		scalarValues.Remove(del)
	}

	//能够做并集的范围进行并集优化，否则添加新范围到列表
	set := b.getOrCreateRageValueSet(column)
	if !set.Empty() {
		var err error
		index, found := set.Find(func(index int, item interface{}) bool {
			r := item.(Range)
			var hasInter bool
			hasInter, err = r.HasIntersection(value)
			return err != nil || hasInter
		})
		if err != nil {
			return err
		}
		if index >= 0 {
			newRange, e := found.(Range).Union(value)
			if e != nil {
				return e
			}
			set.Set(index, newRange)
			return nil
		}
	}
	set.Add(value)
	return nil
}
