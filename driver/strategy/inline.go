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

package strategy

import (
	"github.com/XiaoMi/Gaea/core"
	"github.com/XiaoMi/Gaea/core/script"
)

type Inline struct {
	Columns    []string
	Expression script.InlineExpression
}

func (i *Inline) GetShardingColumns() []string {
	return i.Columns
}

func (i *Inline) IsShardingColumn(column string) bool {
	for _, c := range i.Columns {
		if column == c {
			return true
		}
	}
	return false
}

func (i *Inline) IsScalarValueSupported() bool {
	return true
}

func (i *Inline) IsRangeValueSupported() bool {
	return false
}

func (i *Inline) Shard(sources []string, values *core.ShardingValues) []string {
	panic("implement me")
}
