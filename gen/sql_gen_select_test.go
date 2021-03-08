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

package gen

import (
	"github.com/XiaoMi/Gaea/core"
	"github.com/XiaoMi/Gaea/core/script"
	"github.com/XiaoMi/Gaea/driver/strategy"
	"github.com/XiaoMi/Gaea/explain"
	"github.com/XiaoMi/Gaea/rewriting"
	"github.com/XiaoMi/Gaea/testkit"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestSelectShard(t *testing.T) {

	tests := []struct {
		tbInline string
		sql      string
		sqls     *SqlGenResult
	}{
		{
			tbInline: "test_${id%4}",
			sql:      "select * from test where id = 0",
			sqls: &SqlGenResult{
				Commands: []*ScatterCommand{
					{
						DataSource: "db1",
						SqlCommand: "SELECT * FROM `test_0` WHERE `id`=0",
					},
				},
				Usage: UsageShard,
			},
		},
		{
			tbInline: "test_${id%4}",
			sql:      "select id from test where id = 0",
			sqls: &SqlGenResult{
				Commands: []*ScatterCommand{
					{
						DataSource: "db1",
						SqlCommand: "SELECT `id` FROM `test_0` WHERE `id`=0",
					},
				},
			},
		},

		{
			tbInline: "test_${id%4}",
			sql:      "select * from test where id in (0,1,2,3,4,6)",
			sqls: &SqlGenResult{
				Commands: []*ScatterCommand{
					{
						DataSource: "db1",
						SqlCommand: "SELECT * FROM `test_0` WHERE `id` IN (0,4)",
					},
					{
						DataSource: "db1",
						SqlCommand: "SELECT `id` FROM `test_1` WHERE `id`=1",
					},
					{
						DataSource: "db1",
						SqlCommand: "SELECT `id` FROM `test_2` WHERE IN (2,6)",
					},
					{
						DataSource: "db1",
						SqlCommand: "SELECT `id` FROM `test_2` WHERE `id`=2",
					},
					{
						DataSource: "db1",
						SqlCommand: "SELECT `id` FROM `test_3` WHERE `id`=3",
					},
				},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.sql, func(tt *testing.T) {
			privder := useShardingTables(tt, test.tbInline)
			stmt := testkit.ParseSelect(test.sql, tt)

			expl := explain.NewSqlExplain(privder)
			err := expl.ExplainSelect(stmt, rewriting.DefaultRewriter)
			assert.Nil(tt, err)

			r, e := GenerateSql("db1", expl, nil)
			assert.Nil(tt, e)
			AssertResultEquals(tt, test.sqls, r)
		})
	}
}

func AssertResultEquals(t testing.TB, r1 *SqlGenResult, r2 *SqlGenResult) {
	if (r1 == nil && r2 != nil) || (r1 != nil && r2 == nil) {
		assert.Fail(t, testkit.ErrorDifferentInfo(r1, r2))
		return
	}
	if r1 == nil && r2 == nil {
		return
	}
	if r1 != nil && r2 != nil {
		if r1.Usage != r2.Usage || len(r1.Commands) != len(r2.Commands) {
			assert.Fail(t, testkit.ErrorDifferentInfo(r1, r2))
		} else {
			for i := range r1.Commands {
				if !AssertScatterCommandEqual(t, r1.Commands[i], r2.Commands[i]) {
					return
				}
			}
		}

	} else {
		assert.Equal(t, r1, r2)
	}
}

func AssertScatterCommandEqual(t testing.TB, cmd1 *ScatterCommand, cmd2 *ScatterCommand) bool {
	r1 := assert.Equal(t, cmd1.DataSource, cmd2.DataSource)
	r2 := testkit.AssertEqualSql(t, cmd1.SqlCommand, cmd2.SqlCommand)
	return r2 && r1
}

func useShardingTables(tb testing.TB, tableExpression string) explain.ShardingTableProvider {
	inlineExpr, err := script.NewInlineExpression(tableExpression, &script.Variable{
		Name: "id",
	})

	assert.Nil(tb, err)

	st1 := &core.ShardingTable{
		Name:             "test",
		DatabaseStrategy: core.NoneShardingStrategy,
		TableStrategy: &strategy.Inline{
			Columns:    []string{"id"},
			Expression: inlineExpr,
		},
	}

	st1.SetResources([]string{"db1"}, []string{"test_0", "test_1", "test_2", "test_3"})

	return explain.NewShardingTableProvider("sharding-db", map[string]*core.ShardingTable{
		"test": st1,
	})
}
