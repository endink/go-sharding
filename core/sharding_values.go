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
	"github.com/scylladb/go-set/strset"
	"sync"
)

type ShardingValues struct {
	valueSync      sync.Mutex
	scalarValues   map[string][]ShardingScalarValue //key: table.column, value
	scalarValueSet map[string]*strset.Set
	rangeValues    map[string][]ShardingRangeValue
}

func (r *ShardingValues) PushScalarValue(column string, values ...ShardingScalarValue) {
	c := TrimAndLower(column)
	valueCount := len(values)
	if c != "" && valueCount > 0 {
		r.valueSync.Lock()
		defer r.valueSync.Unlock()
		changed := false
		values, set := r.getOrCreateScalarValueSet(c, valueCount)
		for _, v := range values {
			vStr := v.String()
			if !set.HasAny(vStr) {
				set.Add(vStr)
				values = append(values, v)
				changed = true
			}
		}
		if changed {
			r.scalarValues[c] = values
		}
	}
}

func (r *ShardingValues) getOrCreateScalarValueSet(column string, initSize int) ([]ShardingScalarValue, *strset.Set) {
	var valueStrSet *strset.Set
	var valueSet []ShardingScalarValue
	if set, ok := r.scalarValueSet[column]; ok {
		valueStrSet = set
		valueSet = r.scalarValues[column]
	} else {
		valueStrSet = strset.NewWithSize(initSize)
		valueSet = make([]ShardingScalarValue, 0, initSize)
	}
	return valueSet, valueStrSet
}
