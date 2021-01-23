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
	"errors"
	"fmt"
	"github.com/XiaoMi/Gaea/core/collection"
	"github.com/emirpasic/gods/lists/arraylist"
	"sync"
)

type ShardingValuesBuilder struct {
	tableName    string
	valueSync    sync.Mutex
	scalarValues map[string]*collection.HashSet //key: column, value: value
	rangeValues  map[string]*arraylist.List     //key: column, value: value range

	scalarCounter map[string]int
	rangeCounter  map[string]int
}

func (b *ShardingValuesBuilder) Reset() {
	b.valueSync.Lock()
	defer b.valueSync.Unlock()
	b.scalarValues = make(map[string]*collection.HashSet)
	b.rangeValues = make(map[string]*arraylist.List)
	b.scalarCounter = make(map[string]int)
	b.rangeCounter = make(map[string]int)
}

func (b *ShardingValuesBuilder) Build() *ShardingValues {
	var smap = make(map[string][]interface{}, len(b.scalarValues))
	var rmap = make(map[string][]Range, len(b.rangeValues))

	for column, values := range b.scalarValues {
		smap[column] = values.Values()
	}

	for column, values := range b.rangeValues {
		array := make([]Range, values.Size())
		values.Each(func(i int, value interface{}) {
			array[i] = value.(Range)
		})
		rmap[column] = array
	}
	return &ShardingValues{
		TableName:    b.tableName,
		ScalarValues: smap,
		RangeValues:  rmap,
	}
}

func NewShardingValuesBuilder(tableName string) *ShardingValuesBuilder {
	return &ShardingValuesBuilder{
		tableName:     tableName,
		scalarValues:  make(map[string]*collection.HashSet),
		rangeValues:   make(map[string]*arraylist.List),
		scalarCounter: make(map[string]int),
		rangeCounter:  make(map[string]int),
	}
}

func (b *ShardingValuesBuilder) hasValue(column string) bool {
	return b.hasScalarValue(column) || b.hasRangeValue(column)
}

func (b *ShardingValuesBuilder) hasRangeValue(column string) bool {
	_, ok := b.rangeCounter[column]
	return ok
}

func (b *ShardingValuesBuilder) hasScalarValue(column string) bool {
	_, ok := b.scalarCounter[column]
	return ok
}

func (b *ShardingValuesBuilder) countRangeValue(column string) {
	c, _ := b.rangeCounter[column]
	b.rangeCounter[column] = c + 1
}

func (b *ShardingValuesBuilder) countScalarValue(column string) {
	b.scalarCounter[column]++
}

func (b *ShardingValuesBuilder) ContainsRange(column string, lower interface{}, upper interface{}) bool {
	c := TrimAndLower(column)
	if set, ok := b.rangeValues[c]; ok {
		return set.Any(func(_ int, value interface{}) bool {
			r := value.(Range)
			return r.LowerBound() == lower && r.UpperBound() == upper
		})
	}
	return false
}

func (b *ShardingValuesBuilder) ContainsValue(column string, value interface{}) bool {
	c := TrimAndLower(column)
	if set, ok := b.scalarValues[c]; ok {
		return set.Contains(value)
	}
	return false
}

func (b *ShardingValuesBuilder) Merge(other *ShardingValuesBuilder, op BinaryLogic) error {
	if op != LogicAnd && op != LogicOr {
		return errors.New("unknown ShardingValuesBuilder operation, support are and, or")
	}

	b.valueSync.Lock()
	defer b.valueSync.Unlock()

	for column, scalarValues := range other.scalarValues {
		actualOp := op
		if !b.hasValue(column) {
			actualOp = LogicOr
		}
		if !scalarValues.Empty() {
			switch actualOp {
			case LogicAnd:
				b.andValueWithLock(column, false, scalarValues.Values()...)
			case LogicOr:
				b.orValueWithLock(column, false, scalarValues.Values()...)
			}
		}
	}

	var err error
	for column, rangeValues := range other.rangeValues {
		actualOp := op
		if !b.hasValue(column) {
			actualOp = LogicOr
		}
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
	}
	return err
}

func (b *ShardingValuesBuilder) getRanges(list *arraylist.List) ([]Range, error) {
	ranges := make([]Range, list.Size())
	var err error
	list.All(func(i int, value interface{}) bool {
		if r, ok := value.(Range); ok {
			ranges[i] = r
		} else {
			err = fmt.Errorf("%v is not Range type", value)
		}
		return err == nil
	})
	if err != nil {
		return nil, err
	}
	return ranges, nil
}

func (b *ShardingValuesBuilder) AndValue(column string, values ...interface{}) {
	c := TrimAndLower(column)
	b.andValueWithLock(c, true, values...)
}

func (b *ShardingValuesBuilder) andValueWithLock(column string, lock bool, values ...interface{}) {
	valueCount := len(values)
	if column != "" && valueCount > 0 {
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
			//首次没有值时可能, 标记已经投入过值
			b.orValueWithLock(column, false, values...)
			return
		} else {
			b.intersect(column, values)
			b.countScalarValue(column)
		}

		//并入集合交集值
		b.orValueWithLock(column, false, rValues...)
	}
}

func (b *ShardingValuesBuilder) intersectRangeAndValues(rangeList *arraylist.List, values []interface{}) []interface{} {
	set := collection.NewHashSet()
	for _, v := range values {
		if rangeList.Any(func(index int, rv interface{}) bool {
			rangeItem := rv.(Range)
			c, _ := rangeItem.ContainsValue(v)
			return c
		}) {
			set.Add(v)
		}
	}
	return set.Values()
}

func (b *ShardingValuesBuilder) intersect(column string, slice2 []interface{}) {
	set, ok := b.scalarValues[column]
	if !ok {
		b.scalarValues[column] = collection.NewHashSet(slice2...)
	}

	nn := make([]interface{}, 0)

	for _, v := range slice2 {
		existed := set.Contains(v)
		if existed {
			nn = append(nn, v)
		}
	}
	b.scalarValues[column] = collection.NewHashSet(nn...)
}

func (b *ShardingValuesBuilder) OrValue(column string, values ...interface{}) {
	c := TrimAndLower(column)
	b.orValueWithLock(c, true, values...)
}

func (b *ShardingValuesBuilder) orValueWithLock(column string, lock bool, values ...interface{}) {
	valueCount := len(values)
	if column != "" && valueCount > 0 {
		defer func() { b.countScalarValue(column) }()
		if lock {
			b.valueSync.Lock()
			defer b.valueSync.Unlock()
		}
		set := b.getOrCreateScalarValueSet(column)
		set.Add(values...)
	}
}

func (b *ShardingValuesBuilder) OrRange(column string, values ...Range) error {
	c := TrimAndLower(column)
	return b.orRangeWithLock(c, true, values...)
}

func (b *ShardingValuesBuilder) orRangeWithLock(column string, lock bool, values ...Range) error {
	valueCount := len(values)
	if column != "" && valueCount > 0 {
		if lock {
			b.valueSync.Lock()
			defer b.valueSync.Unlock()
		}
		for _, item := range values {
			if err := b.orSingleRange(column, item); err != nil {
				return err
			}
		}
	}
	return nil
}

func (b *ShardingValuesBuilder) AndRange(column string, values ...Range) error {
	c := TrimAndLower(column)
	return b.andRangeWithLock(c, true, values...)
}

func (b *ShardingValuesBuilder) andRangeWithLock(column string, lock bool, values ...Range) error {
	valueCount := len(values)

	if column != "" && valueCount > 0 {
		if lock {
			b.valueSync.Lock()
			defer b.valueSync.Unlock()
		}
		for _, item := range values {
			if err := b.andSingleRange(column, item); err != nil {
				return err
			}
		}
	}
	return nil
}

func (b *ShardingValuesBuilder) andSingleRange(column string, value Range) error {
	defer func() { b.countRangeValue(column) }()

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
	defer func() { b.countRangeValue(column) }()

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

func (b *ShardingValuesBuilder) getOrCreateScalarValueSet(column string) *collection.HashSet {
	if set, ok := b.scalarValues[column]; ok {
		return set
	} else {
		set = collection.NewHashSet()
		b.scalarValues[column] = set
		return set
	}
}

func (b *ShardingValuesBuilder) getOrCreateRageValueSet(column string) *arraylist.List {
	if set, ok := b.rangeValues[column]; ok {
		return set
	} else {
		set = arraylist.New()
		b.rangeValues[column] = set
		return set
	}
}
