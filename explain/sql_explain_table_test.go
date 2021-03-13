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
	"github.com/XiaoMi/Gaea/core"
	_ "github.com/XiaoMi/Gaea/driver"
	"testing"
)

var MockedShardingTables = []*ShardingTableMocked{
	{
		name:    "t_order",
		columns: []string{"order_id"},
	},
	{
		name:    "t_order_item",
		columns: []string{"order_id", "item_id"},
	},
}

func TestExplainTables(t *testing.T) {
	t.Run("noShardTable", func(t *testing.T) {
		sql := "SELECT B, C, D FROM A WHERE ID = 12345 and name = 'gggg'"
		assertTableLookup(t, sql, MockedShardingTables)
	})

	t.Run("shardSingleColumn", func(t *testing.T) {
		sql := "SELECT a, b FROM `T_Order` WHERE order_id = 12345 and name = 'gggg'"
		assertTableLookup(t, sql, MockedShardingTables, "t_order")
	})

	t.Run("noShardColumn", func(t *testing.T) {
		sql := "SELECT a, b FROM `T_Order`"
		assertTableLookup(t, sql, MockedShardingTables, "t_order")
	})

	t.Run("shardColumnNotEnough", func(t *testing.T) {
		sql := "SELECT a, b FROM `t_order_item` WHERE order_id = 12345 and name = '3333'"
		assertTableLookup(t, sql, MockedShardingTables, "t_order_item")
	})

	t.Run("shardColumnAnd", func(t *testing.T) {
		sql := "SELECT a, b FROM `t_order_item` WHERE (order_id = 12345 and item_id = 3)"
		assertTableLookup(t, sql, MockedShardingTables, "t_order_item")
	})

	t.Run("shardColumnOr", func(t *testing.T) {
		sql := "SELECT a, b FROM `t_order_item` WHERE (order_id = 12345 or item_id = 3)"
		assertTableLookup(t, sql, MockedShardingTables, "t_order_item")
	})

	t.Run("shardColumnOr", func(t *testing.T) {
		sql := "SELECT a, b FROM `t_order_item` WHERE (order_id = 12345 or item_id = 3)"
		assertTableLookup(t, sql, MockedShardingTables, "t_order_item")
	})

	t.Run("alias", func(t *testing.T) {
		sql := "SELECT a, b FROM `t_order_item` as A WHERE (A.order_id = 12345 or A.item_id = 3)"
		assertTableAlias(t, sql, MockedShardingTables, "a")
	})

	t.Run("multiAlias", func(t *testing.T) {
		sql := `
SELECT a.a, a.b 
FROM 
t_order_item as i 
join 
t_order as o 
on 
i.order_id = o.order_id 
and i.name in ('ccc', 'ddd') 
or o.order_id =9999 or (o.order_id = 12345 and o.order_id = 45678)`
		assertTableAlias(t, sql, MockedShardingTables, "o", "i")
	})
}

func TestJoinOn(t *testing.T) {
	var sql string

	sql = `
SELECT i.a, o.b 
FROM 
t_order_item as i 
join 
t_order as o 
on 
i.order_id = o.order_id 
and i.name in ('ccc', 'ddd') 
or o.order_id =9999 or (o.order_id = 12345 and o.order_id = 45678)
`
	av := assertValues{
		valueLogic:           core.LogicAnd,
		scalarCount:          3,
		effectiveScalarCount: 1,
	}
	values := parseExplainTables(t, sql, MockedShardingTables...)

	assertShardingValues(t, values, "t_order", "order_id", av)

}
