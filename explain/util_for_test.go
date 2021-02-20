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
	"github.com/XiaoMi/Gaea/testkit"
	"github.com/stretchr/testify/assert"
	"testing"
)

func parseExplainTables(t *testing.T, sql string, shardingTables ...*ShardingTableMocked) map[string]*core.ShardingValues {

	//sql = "SELECT A.ID as AID, B.ID AS AID from student A,student B,student C"
	stmt := testkit.ParseSelect(sql, t)
	assert.NotNil(t, stmt)

	explain := MockSqlExplain(shardingTables...)
	err := explain.ExplainTables(stmt, NoneRewriter)
	assert.Nil(t, err)

	values, err := explain.valueRedoLogs.Redo(newValueRedoContext(), nil)
	assert.Nil(t, err)
	return values
}

func parseExplainWhere(t *testing.T, sql string, shardingTables ...*ShardingTableMocked) map[string]*core.ShardingValues {
	//sql = "SELECT A.ID as AID, B.ID AS AID from student A,student B,student C"
	stmt := testkit.ParseSelect(sql, t)
	assert.NotNil(t, stmt)

	explain := MockSqlExplain(shardingTables...)
	if assert.NotNil(t, stmt.Where, "where statement requried") {
		err := explain.ExplainWhere(stmt, NoneRewriter)
		assert.Nil(t, err)
		values, e := explain.valueRedoLogs.Redo(newValueRedoContext(), nil)
		assert.Nil(t, t, e)
		return values
	}
	return nil
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
	noNil := assert.NotNil(t, values, "column '%s' values can not be nil", column)
	if noNil {
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
	return make([]interface{}, 0), make([]core.Range, 0)
}

func assertTableAlias(t *testing.T, sql string, mockedShardingTables []*ShardingTableMocked, exceptedAlias ...string) {

	//sql = "SELECT A.ID as AID, B.ID AS AID from student A,student B,student C"
	stmt := testkit.ParseSelect(sql, t)
	assert.NotNil(t, stmt)

	explain := MockSqlExplain(mockedShardingTables...)

	err := explain.ExplainTables(stmt, NoneRewriter)
	assert.Nil(t, err)

	lookup := explain.currentContext().TableLookup()
	if len(exceptedAlias) > 0 {
		for _, alias := range exceptedAlias {
			assert.True(t, lookup.HasAlias(core.TrimAndLower(alias)), "should has alias: '%s'", alias)
		}
	}
}

func assertTableLookup(t *testing.T, sql string, mockedShardingTables []*ShardingTableMocked, exceptedTables ...string) {

	//sql = "SELECT A.ID as AID, B.ID AS AID from student A,student B,student C"
	stmt := testkit.ParseSelect(sql, t)
	assert.NotNil(t, stmt)

	explain := MockSqlExplain(mockedShardingTables...)

	err := explain.ExplainTables(stmt, NoneRewriter)
	assert.Nil(t, err)

	shardingTables := explain.currentContext().TableLookup().ShardingTables()

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
