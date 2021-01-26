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

package rewriting

import (
	"github.com/XiaoMi/Gaea/core"
	_ "github.com/XiaoMi/Gaea/driver"
	"testing"
)

func TestExplainTables(t *testing.T) {
	var sql string
	sql = "SELECT B, C, D FROM A WHERE ID = 12345 and name = 'gggg'"
	assertTableLookup(t, sql)

	sql = "SELECT a, b FROM `T_Order` WHERE order_id = 12345 and name = 'gggg'"
	assertTableLookup(t, sql, "t_order")

	sql = "SELECT a, b FROM `T_Order`"
	assertTableLookup(t, sql, "t_order")

	sql = "SELECT a, b FROM `t_order_item` WHERE order_id = 12345 and name = 'gggg'"
	assertTableLookup(t, sql, "t_order_item")

	sql = "SELECT a, b FROM `t_order_item` WHERE name in ('cccc', 'ddd') or (order_id = 12345 and id = 3)"
	assertTableLookup(t, sql, "t_order_item")

	sql = `
SELECT a.a, a.b FROM t_order_item as a 
join 
t_order as o 
on 
a.order_id = o.order_id 
and a.name in ('cccc', 'ddd') 
or (o.order_id = 12345 and o.id = 45678)
`
	assertTableLookup(t, sql, "t_order_item", "t_order")
}

func TestJoinOn(t *testing.T) {
	var sql string

	sql = `
SELECT a.a, a.b 
FROM 
t_order_item as a 
join 
t_order as o 
on 
a.order_id = o.order_id 
and a.name in ('ccc', 'ddd') 
or o.order_id =9999 or (o.order_id = 12345 and o.order_id = 45678)
`
	av := assertValues{
		valueLogic:           core.LogicOr,
		scalarCount:          3,
		effectiveScalarCount: 1,
	}
	values := parseExplainTables(t, sql)
	assertShardingValues(t, values, "t_order", "order_id", av)

}
