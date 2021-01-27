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
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestMockShardingTableProvider(t *testing.T) {
	var tables = []*ShardingTableMocked{
		{
			name:    "t_order",
			columns: []string{"order_id"},
		},
		{
			name:    "t_order_item",
			columns: []string{"order_id", "item_id"},
		},
	}

	mock := MockShardingTableProvider(tables...)

	t.Run("found", func(t *testing.T) {
		assertFindTable(t, mock, "t_order", true)
		assertFindTable(t, mock, "t_order_item", true)
	})

	t.Run("foundCaseName", func(t *testing.T) {
		assertFindTable(t, mock, "t_Order", true)
		assertFindTable(t, mock, "t_order_item", true)
	})

	t.Run("noFound", func(t *testing.T) {
		assertFindTable(t, mock, "A", false)
	})
}

func assertFindTable(t *testing.T, mock ShardingTableProvider, table string, foundAssertion bool) {
	_, ok := mock.GetShardingTable(table)
	if foundAssertion {
		assert.True(t, ok, "table '%s' not found", table)
	} else {
		assert.False(t, ok, "excepted table '%s' not found, but it found", table)
	}
}
