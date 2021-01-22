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
	"github.com/emirpasic/gods/lists/arraylist"
	"github.com/emirpasic/gods/sets"
	"github.com/emirpasic/gods/sets/hashset"
	"sync"
)

type ShardingValues struct {
	valueSync    sync.Mutex
	scalarValues map[string]sets.Set //key: table.column, value
	rangeValues  map[string]*arraylist.List
}

func NewShardingValues() *ShardingValues {
	return &ShardingValues{
		scalarValues: make(map[string]sets.Set),
		rangeValues:  make(map[string]*arraylist.List),
	}
}

func (r *ShardingValues) AndValue(column string, values ...interface{}) {
	c := TrimAndLower(column)
	valueCount := len(values)
	if c != "" && valueCount > 0 {
		r.valueSync.Lock()
		defer r.valueSync.Unlock()
		r.intersect(column, values)
	}
}

func (r *ShardingValues) intersect(column string, slice2 []interface{}) {
	set, ok := r.scalarValues[column]
	if !ok {
		r.scalarValues[column] = hashset.New(slice2...)
	}

	m := make(map[interface{}]struct{})
	nn := make([]interface{}, 0)
	slice1 := set.Values()
	for _, v := range slice1 {
		m[v] = struct{}{}
	}

	for _, v := range slice2 {
		_, ok := m[v]
		if ok {
			nn = append(nn, v)
		}
	}
	r.scalarValues[column] = hashset.New(nn...)
}

func (r *ShardingValues) OrValue(column string, values ...interface{}) {
	c := TrimAndLower(column)
	valueCount := len(values)
	if c != "" && valueCount > 0 {
		r.valueSync.Lock()
		defer r.valueSync.Unlock()
		set := r.getOrCreateScalarValueSet(c)
		set.Add(values)
	}
}

func (r *ShardingValues) orRange(column string, values ...Range) error {
	c := TrimAndLower(column)
	valueCount := len(values)
	if c != "" && valueCount > 0 {
		r.valueSync.Lock()
		defer r.valueSync.Unlock()
		for _, item := range values {
			if err := r.orSingleRange(c, item); err != nil {
				return err
			}
		}
	}
	return nil
}

func (r *ShardingValues) AndRange(column string, values ...Range) error {
	c := TrimAndLower(column)
	valueCount := len(values)
	if c != "" && valueCount > 0 {
		r.valueSync.Lock()
		defer r.valueSync.Unlock()
		for _, item := range values {
			if err := r.andSingleRange(c, item); err != nil {
				return err
			}
		}
	}
	return nil
}

func (r *ShardingValues) andSingleRange(column string, value Range) error {
	set := r.getOrCreateRageValueSet(column)
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
	}

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
	return nil
}

func (r *ShardingValues) orSingleRange(column string, value Range) error {
	set := r.getOrCreateRageValueSet(column)
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

func (r *ShardingValues) getOrCreateScalarValueSet(column string) sets.Set {
	if set, ok := r.scalarValues[column]; ok {
		return set
	} else {
		set = hashset.New()
		r.scalarValues[column] = set
		return set
	}
}

func (r *ShardingValues) getOrCreateRageValueSet(column string) *arraylist.List {
	if set, ok := r.rangeValues[column]; ok {
		return set
	} else {
		set = arraylist.New()
		r.rangeValues[column] = set
		return set
	}
}
