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
	"github.com/emirpasic/gods/lists/arraylist"
)

type action int
const(
	actionDelete action = iota
	actionAdd
)

type ShardingValueCalc interface {
}

func NewShardingValueCalc() ShardingValueCalc {
	return &shardingValueCalc{
		values: arraylist.New(),
	}
}

type shardingValueCalc struct {
	values *arraylist.List //合并值，交集的含义
}

type actionItem {
	action
}

func (calc *shardingValueCalc) And(value ShardingValue) {
	if value != nil {
		switch v := value.(type) {
		case *ShardingScalarValue:
			if calc.values.Size() == 0 {
				calc.values.Add(value)
			} else {
				actions:= make(map[ShardingValue]action)
				calc.values.Find(func(i int, cv interface{}) {
					switch existValue := cv.(type) {
					case *ShardingScalarValue:
						if existValue != v {
							actions[existValue] = actionDelete
						}
					case *ShardingRangeValue:

					}
				})
			}
		}
	}
}
