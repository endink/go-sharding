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

import (
	"fmt"
	"github.com/XiaoMi/Gaea/core/collection"
	"github.com/emirpasic/gods/lists/arraylist"
	"sync"
)

type ShardingValues struct {
	TableName    string
	ScalarValues map[string][]interface{} //key: column, value: value
	RangeValues  map[string][]Range
}

type ShardingValuesBuilder struct {
	tableName    string
	valueSync    sync.Mutex
	scalarValues map[string]*collection.HashSet //key: column, value: value
	rangeValues  map[string]*arraylist.List     //key: column, value: value range
}

func (b *ShardingValuesBuilder) Build() *ShardingValues {
	var smap = make(map[string][]interface{}, len(b.scalarValues))
	var rmap = make(map[string][]Range, len(b.rangeValues))

	for column, values := range b.scalarValues {
		smap[column] = values.Values()
	}

	for column, values := range b.rangeValues {
		array := make([]Range, 0, values.Size())
		values.Each(func(_ int, value interface{}) {
			array = append(array, value.(Range))
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
		tableName:    tableName,
		scalarValues: make(map[string]*collection.HashSet),
		rangeValues:  make(map[string]*arraylist.List),
	}
}

func (b *ShardingValuesBuilder) AndValue(column string, values ...interface{}) {
	c := TrimAndLower(column)
	valueCount := len(values)
	if c != "" && valueCount > 0 {
		b.valueSync.Lock()
		defer b.valueSync.Unlock()
		sValues := values
		if set, ok := b.rangeValues[c]; ok {
			sValues = b.intersectRangeAndValues(set, sValues)
			//明确值的交集将导致 range 失效
			set.Clear()
		}
		b.intersect(c, sValues)
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
	valueCount := len(values)
	if c != "" && valueCount > 0 {
		b.valueSync.Lock()
		defer b.valueSync.Unlock()
		set := b.getOrCreateScalarValueSet(c)
		set.Add(values)
	}
}

func (b *ShardingValuesBuilder) orRange(column string, values ...Range) error {
	c := TrimAndLower(column)
	valueCount := len(values)
	if c != "" && valueCount > 0 {
		b.valueSync.Lock()
		defer b.valueSync.Unlock()
		for _, item := range values {
			if err := b.orSingleRange(c, item); err != nil {
				return err
			}
		}
	}
	return nil
}

func (b *ShardingValuesBuilder) AndRange(column string, values ...Range) error {
	c := TrimAndLower(column)
	valueCount := len(values)

	if c != "" && valueCount > 0 {
		b.valueSync.Lock()
		defer b.valueSync.Unlock()
		for _, item := range values {
			if err := b.andSingleRange(c, item); err != nil {
				return err
			}
		}
	}
	return nil
}

func (b *ShardingValuesBuilder) andSingleRange(column string, value Range) error {
	/*
		对于新加入的条件 a < 100 产生如下逻辑：
		得到如下逻辑： (a = 152 or a = 2 or (a > 140 and a < 150)) and a < 145
		处理步骤：
		剔除新加入的范围中没有交集的所有明确值
		对已存在的范围求交集，如果没有交集进行移除，得以下逻辑：
		a = 2 or (a > 140 and a < 145)
	*/

	//筛选明确的值
	if set, hasScalar := b.scalarValues[column]; hasScalar && !set.Empty() {
		values, err := set.Select(func(item interface{}) (bool, error) {
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
		b, e := rangItem.HasIntersection(value)
		if e != nil {
			if err != nil {
				err = fmt.Errorf("%s intersect %s fault", rangItem, value)
			}
		}
		return e == nil && b
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
			newRange, err = rangItem.Intersect(value)
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
	set := b.getOrCreateRageValueSet(column)
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
	} else {
		set.Add(value)
	}
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
