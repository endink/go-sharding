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

package explain

import (
	"github.com/endink/go-sharding/core"
	"sync"
)

type valueScope struct {
	logic     core.BinaryLogic
	tableLock sync.Mutex
	builders  map[string]*core.ShardingValuesBuilder
}

func newValueScope(logic core.BinaryLogic) *valueScope {
	return &valueScope{
		logic:    logic,
		builders: map[string]*core.ShardingValuesBuilder{},
	}
}

func (s *valueScope) table(tableName string) *core.ShardingValuesBuilder {
	var builder *core.ShardingValuesBuilder
	var ok bool
	if builder, ok = s.builders[tableName]; !ok {
		s.tableLock.Lock()
		defer s.tableLock.Unlock()
		if builder, ok = s.builders[tableName]; !ok {
			builder = core.NewShardingValuesBuilder(tableName)
			s.builders[tableName] = builder
		}
	}
	return builder
}
