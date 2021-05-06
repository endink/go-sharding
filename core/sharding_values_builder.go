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
	"github.com/emirpasic/gods/lists/arraylist"
	"github.com/endink/go-sharding/core/collection"
	"sync"
)

type ShardingValuesBuilder struct {
	tableName    string
	valueSync    sync.Mutex
	scalarValues map[string]*collection.HashSet //key: column, value: value
	rangeValues  map[string]*arraylist.List     //key: column, value: value range

	scalarCounter map[string]int
	rangeCounter  map[string]int

	columnLogic sync.Map //该列和其他列的逻辑关系比如 a = 1 or b = 2, a 为 and, b 为 or
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
	var scmap = make(map[string]int, len(b.scalarCounter))
	var rcmap = make(map[string]int, len(b.scalarCounter))
	var lgmap = make(map[string]BinaryLogic)

	scalarTotal := copyCount(scmap, b.scalarCounter)
	rangeTotal := copyCount(rcmap, b.rangeCounter)

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

	b.columnLogic.Range(func(key, value interface{}) bool {
		lgmap[key.(string)] = value.(BinaryLogic)
		return true
	})

	return &ShardingValues{
		TableName:        b.tableName,
		ScalarValues:     smap,
		RangeValues:      rmap,
		scalarCount:      scmap,
		rangeCount:       rcmap,
		totalRangeCount:  rangeTotal,
		totalScalarCount: scalarTotal,
		columnLogic:      lgmap,
	}
}

func copyCount(dest map[string]int, source map[string]int) (totalCount int) {
	tt := 0
	for k, v := range source {
		dest[k] = v
		tt += v
	}
	return tt
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

func (b *ShardingValuesBuilder) increaseRange(column string, count int) {
	b.rangeCounter[column] += count
}

func (b *ShardingValuesBuilder) increaseScalar(column string, count int) {
	b.scalarCounter[column] += count
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

func (b *ShardingValuesBuilder) setColumnLogic(column string, logic BinaryLogic) {
	b.columnLogic.LoadOrStore(column, logic)
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
