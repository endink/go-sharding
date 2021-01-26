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
	"github.com/XiaoMi/Gaea/config"
	"github.com/XiaoMi/Gaea/core"
	"github.com/XiaoMi/Gaea/explain"
	"github.com/XiaoMi/Gaea/testkit"
	"github.com/stretchr/testify/assert"
	"testing"
)

const testConfigYaml = `
sources:
  ds0: 
    endpoint: localhost:3306
    schema: test_db
    username: root
    password: 
  ds1:
    endpoint: localhost:3306
    schema: test_db
    username: root
    password: 

default-source: ds0

rule:  
  tables:
    t_order: 
      resources: ds${range(0,1)}.t_order${[0,1]}
      db-strategy:
        inline:
          sharding-columns: user_id
          expression: ds${user_id % 2}
      table-strategy: 
        inline:
          sharding-columns: order_id
          expression: t_order_${order_id % 2}
      keyGenerator:
        type: SNOWFLAKE
        column: order_id
    t_order_item:
      resources: ds${range(0,1)}.t_order_item${range(0,1)}
      db-strategy:
        inline:
          sharding-columns: user_id
          expression: ds${user_id % 2}
      table-strategy:
        inline:
          sharding-columns: order_id, id
          expression: t_order_item_${order_id % 2}_${id *2}
    t_product:
      db-strategy: none
      table-strategy: none

server: 
  port: 13308
  username: root
  password: root2
  schema: test
`

type ExplainTestSession struct {
	ConfigManager       config.Manager
	ShardingTableFinder func(table string) (*core.ShardingTable, bool)
	SqlExplain          *explain.SqlExplain
}

func (s *ExplainTestSession) Context() explain.Context {
	return s.SqlExplain.CurrentContext()
}

func NewExplainTestSession(t *testing.T, yaml string) *ExplainTestSession {
	mgr, err := config.NewManagerFromString(yaml)
	assert.Nil(t, err, "create config manager from yaml fault")
	finder := func(shardingTable string) (*core.ShardingTable, bool) {
		t, ok := mgr.GetSettings().ShardingRule.Tables[shardingTable]
		return t, ok
	}

	exp := explain.NewSqlExplain(finder)

	return &ExplainTestSession{
		ConfigManager:       mgr,
		ShardingTableFinder: finder,
		SqlExplain:          exp,
	}
}

func assertShardingValuesInTable(t *testing.T, table string, values map[string]*core.ShardingValues, hasValue bool) *core.ShardingValues {
	orderValues, ok := values[table]
	if hasValue {
		assert.True(t, ok, "sql should include values of sharding table '%s'", table)
	}
	valueCount := 0
	if ok {
		valueCount = orderValues.TotalScalarCount() + orderValues.TotalRangeCount()
	}
	if hasValue {
		assert.True(t, valueCount > 0, "sql should include values of sharding table '%s'", table)
	}
	return orderValues
}

func getShardingColumnValues(t *testing.T, column string, values *core.ShardingValues) ([]interface{}, []core.Range) {
	return assertShardingColumnValuesAndCounter(t, column, values, -1, -1)
}

func assertShardingColumnValuesAndCounter(t *testing.T, column string, values *core.ShardingValues, scalarCount int, rangeCount int) ([]interface{}, []core.Range) {
	scalarValues, _ := values.ScalarValues[column]
	rangeValues, _ := values.RangeValues[column]
	if scalarCount >= 0 {
		assert.Equal(t, scalarCount, values.ScalarCount(column), "sharding scalar values count is wrong for column '%s', table '%s'", column, values.TableName)
	}
	if rangeCount >= 0 {
		assert.Equal(t, rangeCount, values.RangeCount(column), "sharding range values count is wrong for column '%s', table, '%s'", column, values.TableName)
	}

	return scalarValues, rangeValues
}

func assertTableLookup(t *testing.T, sql string, exceptedTables ...string) {

	//sql = "SELECT A.ID as AID, B.ID AS AID from student A,student B,student C"
	stmt := testkit.ParseSelect(sql, t)
	assert.NotNil(t, stmt)

	session := NewExplainTestSession(t, testConfigYaml)
	rw := NewRewritingEngine(session.Context())

	err := session.SqlExplain.ExplainTables(stmt, rw)
	assert.Nil(t, err)

	shardingTables := session.Context().TableLookup().ShardingTables()
	assert.Equal(t, len(exceptedTables), len(shardingTables))

	testkit.AssertStrArrayEquals(t, exceptedTables, shardingTables)
}

type assertValues struct {
	valueLogic           core.BinaryLogic
	scalarCount          int
	rangeCount           int
	effectiveScalarCount int
	effectiveRangeCount  int
}

func assertShardingValues(t *testing.T, values map[string]*core.ShardingValues, table, column string, av assertValues) {
	orderValues := assertShardingValuesInTable(t, table, values, true)
	//itemValues := testkit.assertShardingValuesInTable(t, "t_order_item", values, false)
	//
	//testkit.assertShardingColumnValuesAndCounter(t, "name", itemValues, 2, 0)
	scalar, ranges := assertShardingColumnValuesAndCounter(t, column, orderValues, av.scalarCount, av.rangeCount)
	assert.Equal(t, av.effectiveScalarCount, len(scalar)) //有效的值由于条件冲突被优化处理
	assert.Equal(t, av.effectiveRangeCount, len(ranges))

	assert.Equal(t, av.valueLogic, orderValues.Logic(column), "%s should has logic: %s", column, av.valueLogic.String())
}
