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
	"github.com/XiaoMi/Gaea/mysql/types"
	"github.com/XiaoMi/Gaea/rewriting"
	"github.com/XiaoMi/Gaea/testkit"
	"github.com/emirpasic/gods/utils"
	"github.com/stretchr/testify/assert"
	"strings"
	"testing"
)

type shardCommandTestCase struct {
	tbInline   string
	sql        string
	sqls       *SqlGenResult
	vars       []*types.BindVariable
	explainErr string
	genErr     string
}

func TestSelectWhere(t *testing.T) {

	tests := []shardCommandTestCase{

		{
			tbInline: "test_${id%4}",
			sql:      "select * from no_shard where id = 2",
			sqls: &SqlGenResult{
				Commands: []*ScatterCommand{
					{
						DataSource: "db1",
						SqlCommand: "SELECT * FROM `no_shard` WHERE `id`=2",
					},
				},
			},
		},

		{
			tbInline: "test_${id%4}",
			sql:      "select * from test where id = 2 or name = 'x'",
			sqls: &SqlGenResult{
				Commands: []*ScatterCommand{
					{
						DataSource: "db1",
						SqlCommand: "select * from test_0 where id = 2 or name = 'x'",
					},
					{
						DataSource: "db1",
						SqlCommand: "select * from test_1 where id = 2 or name = 'x'",
					},
					{
						DataSource: "db1",
						SqlCommand: "select * from test_2 where id = 2 or name = 'x'",
					},
					{
						DataSource: "db1",
						SqlCommand: "select * from test_3 where id = 2 or name = 'x'",
					},
				},
			},
		},

		{
			tbInline: "test_${id%4}",
			sql:      "select * from test where a = 'b' or id = 2 and name = 'x'",
			sqls: &SqlGenResult{
				Commands: []*ScatterCommand{
					{
						DataSource: "db1",
						SqlCommand: "select * from test_0 where a = 'b' or id = 2 and name = 'x'",
					},
					{
						DataSource: "db1",
						SqlCommand: "select * from test_1 where a = 'b' or id = 2 and name = 'x'",
					},
					{
						DataSource: "db1",
						SqlCommand: "select * from test_2 where a = 'b' or id = 2 and name = 'x'",
					},
					{
						DataSource: "db1",
						SqlCommand: "select * from test_3 where a = 'b' or id = 2 and name = 'x'",
					},
				},
			},
		},

		{
			tbInline: "test_${id%4}",
			sql:      "select * from test where name = 'x' or id = 2",
			sqls: &SqlGenResult{
				Commands: []*ScatterCommand{
					{
						DataSource: "db1",
						SqlCommand: "select * from test_0 where name = 'x' or id = 2",
					},
					{
						DataSource: "db1",
						SqlCommand: "select * from test_1 where name = 'x' or id = 2",
					},
					{
						DataSource: "db1",
						SqlCommand: "select * from test_2 where name = 'x' or id = 2",
					},
					{
						DataSource: "db1",
						SqlCommand: "select * from test_3 where name = 'x' or id = 2",
					},
				},
			},
		},

		{
			tbInline: "test_${id%4}",
			sql:      "select * from test where id = 2 and id = 3",
			sqls: &SqlGenResult{
				Usage: UsageImpossible,
			},
		},

		{
			tbInline: "test_${id%4}",
			sql:      "select * from test limit 100",
			sqls: &SqlGenResult{
				Commands: []*ScatterCommand{
					{
						DataSource: "db1",
						SqlCommand: "SELECT * FROM `test_0` limit 100",
					},
					{
						DataSource: "db1",
						SqlCommand: "SELECT * FROM `test_1` limit 100",
					},
					{
						DataSource: "db1",
						SqlCommand: "SELECT * FROM `test_2` limit 100",
					},
					{
						DataSource: "db1",
						SqlCommand: "SELECT * FROM `test_3` limit 100",
					},
				},
			},
		},
		{
			tbInline: "test_${id%4}",
			sql:      "select id from test where id =? or id=?",
			vars:     makeIntVars(0, 3),
			sqls: &SqlGenResult{
				Commands: []*ScatterCommand{
					{
						DataSource: "db1",
						SqlCommand: "SELECT id FROM `test_0` WHERE id =? or id=?",
						Vars:       makeIntVars(0, 3),
					},
					{
						DataSource: "db1",
						SqlCommand: "SELECT id FROM `test_3` WHERE id =? or id=?",
						Vars:       makeIntVars(0, 3),
					},
				},
			},
		},
		{
			tbInline: "test_${id%4}",
			sql:      "select id from test where id =?",
			vars:     makeIntVars(2),
			sqls: &SqlGenResult{
				Commands: []*ScatterCommand{
					{
						DataSource: "db1",
						SqlCommand: "SELECT `id` FROM `test_2` WHERE `id`=?",
						Vars:       makeIntVars(2),
					},
				},
			},
		},

		{
			tbInline: "test_${id%4}",
			sql:      "select * from test where id = 1 and (a = 'a' or id = 2)",
			sqls: &SqlGenResult{
				Usage: UsageShard,
				Commands: []*ScatterCommand{
					{
						DataSource: "db1",
						SqlCommand: "select * from test_0 where id = 1 and (a = 'a' or id = 2)",
					},
					{
						DataSource: "db1",
						SqlCommand: "select * from test_1 where id = 1 and (a = 'a' or id = 2)",
					},
					{
						DataSource: "db1",
						SqlCommand: "select * from test_2 where id = 1 and (a = 'a' or id = 2)",
					},
					{
						DataSource: "db1",
						SqlCommand: "select * from test_3 where id = 1 and (a = 'a' or id = 2)",
					},
				},
			},
		},

		{
			tbInline: "test_${id%4}",
			sql:      "select * from test where id = ? and (a = 'a' or id = ?)",
			vars:     makeIntVars(1, 2),
			sqls: &SqlGenResult{
				Usage: UsageShard,
				Commands: []*ScatterCommand{
					{
						DataSource: "db1",
						SqlCommand: "select * from test_0 where id = ? and (a = 'a' or id = ?)",
						Vars:       makeIntVars(1, 2),
					},
					{
						DataSource: "db1",
						SqlCommand: "select * from test_1 where id = ? and (a = 'a' or id = ?)",
						Vars:       makeIntVars(1, 2),
					},
					{
						DataSource: "db1",
						SqlCommand: "select * from test_2 where id = ? and (a = 'a' or id = ?)",
						Vars:       makeIntVars(1, 2),
					},
					{
						DataSource: "db1",
						SqlCommand: "select * from test_3 where id = ? and (a = 'a' or id = ?)",
						Vars:       makeIntVars(1, 2),
					},
				},
			},
		},

		{
			tbInline: "test_${id%4}",
			sql:      "select * from test where (id = ? or a = 'b') and (a = 'a' or id = ?)",
			vars:     makeIntVars(1, 2),
			sqls: &SqlGenResult{
				Usage: UsageShard,
				Commands: []*ScatterCommand{
					{
						DataSource: "db1",
						SqlCommand: "select * from test_0 where (id = ? or a = 'b') and (a = 'a' or id = ?)",
						Vars:       makeIntVars(1, 2),
					},
					{
						DataSource: "db1",
						SqlCommand: "select * from test_1 where (id = ? or a = 'b') and (a = 'a' or id = ?)",
						Vars:       makeIntVars(1, 2),
					},
					{
						DataSource: "db1",
						SqlCommand: "select * from test_2 where (id = ? or a = 'b') and (a = 'a' or id = ?)",
						Vars:       makeIntVars(1, 2),
					},
					{
						DataSource: "db1",
						SqlCommand: "select * from test_3 where (id = ? or a = 'b') and (a = 'a' or id = ?)",
						Vars:       makeIntVars(1, 2),
					},
				},
			},
		},

		{
			tbInline: "test_${id%4}",
			sql:      "select * from test where id = 1 or a = 'a' and id = 3 and b = 'b'",
			sqls: &SqlGenResult{
				Usage: UsageShard,
				Commands: []*ScatterCommand{
					{
						DataSource: "db1",
						SqlCommand: "select * from test_1 where id = 1 or a = 'a' and id = 3 and b = 'b'",
					},
					{
						DataSource: "db1",
						SqlCommand: "select * from test_3 where id = 1 or a = 'a' and id = 3 and b = 'b'",
					},
				},
			},
		},

		{
			tbInline: "test_${id%4}",
			sql:      "select * from test where a = 'a' or id = 3 and id = 4",
			sqls: &SqlGenResult{
				Commands: []*ScatterCommand{
					{
						DataSource: "db1",
						SqlCommand: "select * from test_0 where a = 'a' or id = 3 and id = 4",
					},
					{
						DataSource: "db1",
						SqlCommand: "select * from test_1 where a = 'a' or id = 3 and id = 4",
					},
					{
						DataSource: "db1",
						SqlCommand: "select * from test_2 where a = 'a' or id = 3 and id = 4",
					},
					{
						DataSource: "db1",
						SqlCommand: "select * from test_3 where a = 'a' or id = 3 and id = 4",
					},
				},
			},
		},

		{
			tbInline: "test_${id%4}",
			sql:      "select * from test where id = 1 or id = 3 and id = 4",
			sqls: &SqlGenResult{
				Commands: []*ScatterCommand{
					{
						DataSource: "db1",
						SqlCommand: "select * from test_1 where id = 1 or id = 3 and id = 4",
					},
				},
			},
		},

		{
			tbInline: "test_${id%4}",
			sql:      "select * from test where id = 3 or (id = 3 and id = 4)",
			sqls: &SqlGenResult{
				Commands: []*ScatterCommand{
					{
						DataSource: "db1",
						SqlCommand: "select * from test_3 where id = 3 or (id = 3 and id = 4)",
					},
				},
			},
		},

		{
			tbInline: "test_${id%4}",
			sql:      "select * from test where id = 3 or (id = 3 and id = 4)",
			sqls: &SqlGenResult{
				Commands: []*ScatterCommand{
					{
						DataSource: "db1",
						SqlCommand: "select * from test_3 where id = 3 or (id = 3 and id = 4)",
					},
				},
			},
		},

		{
			tbInline: "test_${id%4}",
			sql:      "select * from test where (id = 3 and id = 1) or (id = 3 and id = 4)",
			sqls: &SqlGenResult{
				Usage: UsageImpossible,
			},
		},

		{
			tbInline: "test_${id%4}",
			sql:      "select * from test where id = 0 or id = 1 or id = 2 or id = 3 or id = 4",
			sqls: &SqlGenResult{
				Commands: []*ScatterCommand{
					{
						DataSource: "db1",
						SqlCommand: "select * from test_0 where id = 0 or id = 1 or id = 2 or id = 3 or id = 4",
					},
					{
						DataSource: "db1",
						SqlCommand: "select * from test_1 where id = 0 or id = 1 or id = 2 or id = 3 or id = 4",
					},
					{
						DataSource: "db1",
						SqlCommand: "select * from test_2 where id = 0 or id = 1 or id = 2 or id = 3 or id = 4",
					},
					{
						DataSource: "db1",
						SqlCommand: "select * from test_3 where id = 0 or id = 1 or id = 2 or id = 3 or id = 4",
					},
				},
			},
		},
	}

	runTestCases(t, tests)
}

func TestSelectWhereIn(t *testing.T) {
	tests := []shardCommandTestCase{
		{
			tbInline: "test_${id%4}",
			sql:      "select * from test where id in (0,1,2,3,4,6)",
			sqls: &SqlGenResult{
				Commands: []*ScatterCommand{
					{
						DataSource: "db1",
						SqlCommand: "SELECT * FROM `test_0` WHERE id IN (0,4)",
					},
					{
						DataSource: "db1",
						SqlCommand: "SELECT * FROM `test_1` WHERE id = 1",
					},
					{
						DataSource: "db1",
						SqlCommand: "SELECT * FROM `test_2` WHERE id IN (2,6)",
					},
					{
						DataSource: "db1",
						SqlCommand: "SELECT * FROM `test_3` WHERE id=3",
					},
				},
			},
		},
		{
			tbInline: "test_${id%4}",
			sql:      "select id from test where id in (?,?,?,?,?,?)",
			vars:     makeIntVars(0, 1, 2, 3, 4, 6),
			sqls: &SqlGenResult{
				Commands: []*ScatterCommand{
					{
						DataSource: "db1",
						SqlCommand: "SELECT id FROM `test_0` WHERE id IN (?,?)",
						Vars:       makeIntVars(0, 4),
					},
					{
						DataSource: "db1",
						SqlCommand: "SELECT id FROM `test_1` WHERE id=?",
						Vars:       makeIntVars(1),
					},
					{
						DataSource: "db1",
						SqlCommand: "SELECT id FROM `test_2` WHERE id IN (?,?)",
						Vars:       makeIntVars(2, 6),
					},
					{
						DataSource: "db1",
						SqlCommand: "SELECT id FROM `test_3` WHERE id=?",
						Vars:       makeIntVars(3),
					},
				},
			},
		},

		{
			tbInline: "test_${id%4}",
			sql:      "select * from test where id = 3 and id in (3, 4)",
			sqls: &SqlGenResult{
				Commands: []*ScatterCommand{
					{
						DataSource: "db1",
						SqlCommand: "select * from test_3 where id = 3 and id = 3",
					},
				},
			},
		},

		{
			tbInline: "test_${id%4}",
			sql:      "select * from test where id = 0 or id in (2, 3, 4, 8)",
			sqls: &SqlGenResult{
				Commands: []*ScatterCommand{
					{
						DataSource: "db1",
						SqlCommand: "select * from test_0 where id = 0 or id in (4, 8)",
					},
					{
						DataSource: "db1",
						SqlCommand: "select * from test_2 where id = 0 or id = 2",
					},
					{
						DataSource: "db1",
						SqlCommand: "select * from test_3 where id = 0 or id = 3",
					},
				},
			},
		},

		{
			tbInline: "test_${id%4}",
			sql:      "select * from test where id =? or id in (?,?,?,?)",
			vars:     makeIntVars(0, 2, 3, 4, 8),
			sqls: &SqlGenResult{
				Commands: []*ScatterCommand{
					{
						DataSource: "db1",
						SqlCommand: "select * from test_0 where id =? or id in (?, ?)",
						Vars:       makeIntVars(0, 4, 8),
					},
					{
						DataSource: "db1",
						SqlCommand: "select * from test_2 where id =? or id =?",
						Vars:       makeIntVars(0, 2),
					},
					{
						DataSource: "db1",
						SqlCommand: "select * from test_3 where id =? or id =?",
						Vars:       makeIntVars(0, 3),
					},
				},
			},
		},

		{
			tbInline: "test_${id%4}",
			sql:      "select * from test where id in (0, 1, 2) and id in (2, 3, 4, 8)",
			sqls: &SqlGenResult{
				Commands: []*ScatterCommand{
					{
						DataSource: "db1",
						SqlCommand: "select * from test_2 where id = 2 and id = 2",
					},
				},
			},
		},

		{
			tbInline: "test_${id%4}",
			sql:      "select * from test where id in (0, 1, 2, 3) and id in (2, 3, 4, 8)",
			sqls: &SqlGenResult{
				Commands: []*ScatterCommand{
					{
						DataSource: "db1",
						SqlCommand: "select * from test_2 where id = 2 and id = 2",
					},
					{
						DataSource: "db1",
						SqlCommand: "select * from test_3 where id = 3 and id = 3",
					},
				},
			},
		},
	}
	runTestCases(t, tests)
}

func TestSelectOrderBy(t *testing.T) {
	tests := []shardCommandTestCase{
		{
			tbInline: "test_${id%4}",
			sql:      "select * from test order by id",
			sqls: &SqlGenResult{
				Commands: []*ScatterCommand{
					{
						DataSource: "db1",
						SqlCommand: "SELECT *, id FROM `test_0` order by id",
					},
					{
						DataSource: "db1",
						SqlCommand: "SELECT *, id FROM `test_1` order by id",
					},
					{
						DataSource: "db1",
						SqlCommand: "SELECT *, id FROM `test_2` order by id",
					},
					{
						DataSource: "db1",
						SqlCommand: "SELECT *, id FROM `test_3` order by id",
					},
				},
			},
		},
		{
			tbInline: "test_${id%4}",
			sql:      "select * from test where id = 2 order by id",
			sqls: &SqlGenResult{
				Commands: []*ScatterCommand{
					{
						DataSource: "db1",
						SqlCommand: "SELECT *, id FROM `test_2` WHERE `id`=2 order by id",
					},
				},
			},
		},
		{
			tbInline: "test_${id%4}",
			sql:      "select a,b,id,d from test where id = 2 order by id",
			sqls: &SqlGenResult{
				Commands: []*ScatterCommand{
					{
						DataSource: "db1",
						SqlCommand: "SELECT a,b,id,d FROM `test_2` WHERE `id`=2 order by id",
					},
				},
			},
		},
		{
			tbInline: "test_${id%4}",
			sql:      "select id from test where id =? order by name",
			vars:     makeIntVars(0),
			sqls: &SqlGenResult{
				Commands: []*ScatterCommand{
					{
						DataSource: "db1",
						SqlCommand: "SELECT id, name FROM `test_0` WHERE `id`=? order by name",
						Vars:       makeIntVars(0),
					},
				},
			},
		},
		{
			tbInline: "test_${id%4}",
			sql:      "select name from no_shard where id = 2 order by id",
			sqls: &SqlGenResult{
				Commands: []*ScatterCommand{
					{
						DataSource: "db1",
						SqlCommand: "SELECT name, id FROM `no_shard` WHERE `id`=2 order by id", //补列
					},
				},
			},
		},
	}
	runTestCases(t, tests)
}

func TestSelectGroupBy(t *testing.T) {
	tests := []shardCommandTestCase{
		{
			tbInline: "test_${id%4}",
			sql:      "select count(id) from test where id = 2 group by id",
			sqls: &SqlGenResult{
				Commands: []*ScatterCommand{
					{
						DataSource: "db1",
						SqlCommand: "SELECT count(id), id FROM test_2 WHERE id=2 group by id",
					},
				},
			},
		},
		{
			tbInline:   "test_${id%4}",
			sql:        "select avg(a) from test where id = 2 group by id",
			explainErr: "aggregate function type is not support",
		},
		{
			tbInline: "test_${id%4}",
			sql:      "select max(a),id,count(*) from test where id = 2 group by id",
			sqls: &SqlGenResult{
				Commands: []*ScatterCommand{
					{
						DataSource: "db1",
						SqlCommand: "SELECT max(a),id,count(*) FROM `test_2` WHERE `id`=2 group by id",
					},
				},
			},
		},
		{
			tbInline: "test_${id%4}",
			sql:      "select max(id) from test where id =? group by name",
			vars:     makeIntVars(0),
			sqls: &SqlGenResult{
				Commands: []*ScatterCommand{
					{
						DataSource: "db1",
						SqlCommand: "SELECT max(id), name FROM `test_0` WHERE `id`=? group by name",
						Vars:       makeIntVars(0),
					},
				},
			},
		},
		{
			tbInline: "test_${id%4}",
			sql:      "select min(id) as m from no_shard where id = 2 group by id",
			sqls: &SqlGenResult{
				Commands: []*ScatterCommand{
					{
						DataSource: "db1",
						SqlCommand: "SELECT min(id) as m, id FROM `no_shard` WHERE `id`=2 group by id", //补列
					},
				},
			},
		},
	}
	runTestCases(t, tests)
}

func runTestCases(t *testing.T, tests []shardCommandTestCase) {
	for _, test := range tests {
		t.Run(test.sql, func(tt *testing.T) {
			privder := useShardingTables(tt, test.tbInline)
			stmt := testkit.ParseSelect(test.sql, tt)

			expl := explain.NewSqlExplain(privder)
			err := expl.ExplainSelect(stmt, rewriting.NewRewriter())
			if test.explainErr != "" {
				assert.Error(tt, err)
				assert.True(tt, strings.Contains(err.Error(), test.explainErr))
				return
			}

			if !assert.Nil(tt, err) {
				return
			}

			r, e := GenerateSql("db1", expl, test.vars)
			if test.genErr != "" {
				assert.Error(tt, e)
				assert.True(tt, strings.Contains(e.Error(), test.genErr))
				return
			}

			if assert.Nil(tt, e) {
				AssertResultEquals(tt, test.sqls, r)
			}
		})
	}
}

func makeIntVars(values ...int32) []*types.BindVariable {
	vars := make([]*types.BindVariable, len(values))
	for i, v := range values {
		vars[i] = types.Int32BindVariable(v)
	}
	return vars
}

func AssertResultEquals(t testing.TB, excepted *SqlGenResult, actual *SqlGenResult) {
	if (excepted == nil && actual != nil) || (excepted != nil && actual == nil) {
		assert.Fail(t, testkit.ErrorDifferentInfo(excepted, actual))
		return
	}
	if excepted == nil && actual == nil {
		return
	}
	if excepted != nil && actual != nil {

		if excepted.Usage != actual.Usage {
			assert.Fail(t, testkit.ErrorDifferentInfo(excepted, actual, "Usage is not same"))
		} else if len(excepted.Commands) != len(actual.Commands) {
			assert.Fail(t, testkit.ErrorDifferentInfo(excepted, actual, "Commands length is not same"))
		} else {
			cmd1 := make([]interface{}, len(excepted.Commands))
			for i, command := range excepted.Commands {
				command.SqlCommand = testkit.NormalizeSql(t, command.SqlCommand)
				cmd1[i] = command
			}

			cmd2 := make([]interface{}, len(actual.Commands))
			for i, command := range actual.Commands {
				command.SqlCommand = testkit.NormalizeSql(t, command.SqlCommand)
				cmd2[i] = command
			}
			utils.Sort(cmd1, compareScatterCommand)
			utils.Sort(cmd2, compareScatterCommand)
			testkit.AssertArrayStrictlyEquals(t, cmd1, cmd2, "command list are not same")
		}

	} else {
		assert.Equal(t, excepted, actual)
	}
}

func compareScatterCommand(a, b interface{}) int {
	cmd1 := a.(*ScatterCommand)
	cmd2 := b.(*ScatterCommand)
	return utils.StringComparator(cmd1.String(), cmd2.String())
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
