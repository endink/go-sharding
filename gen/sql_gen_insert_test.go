package gen

import (
	"github.com/endink/go-sharding/explain"
	"github.com/endink/go-sharding/rewriting"
	"github.com/endink/go-sharding/testkit"
	"github.com/stretchr/testify/assert"
	"strings"
	"testing"
)

func TestBatchInsert(t *testing.T){
	tests := []shardCommandTestCase{
		{
			tbInline: "test_${id%4}",
			sql:      "insert into test (id, name, f2) values (1, 'cccc1', 'fff1'), (2, 'cccc2','fff2'), (3, 'cccc3', 'fff3'), (4, 'cccc4', 'fff4'), (5, 'cccc5', 'fff5')",
			sqls: &SqlGenResult{
				Commands: []*ScatterCommand{
					{
						DataSource: "db1",
						SqlCommand: "insert into test_0 (id, name, f2) values (4, 'cccc4', 'fff4')", //补列
					},
					{
						DataSource: "db1",
						SqlCommand: "insert into test_1 (id, name, f2) values (1, 'cccc1', 'fff1'), (5, 'cccc5', 'fff5')",
					},
					{
						DataSource: "db1",
						SqlCommand: "insert into test_2 (id, name, f2) values (2, 'cccc2', 'fff2')",
					},
					{
						DataSource: "db1",
						SqlCommand: "insert into test_3 (id, name, f2) values (3, 'cccc3', 'fff3')",
					},
				},
			},
		},
		{
			tbInline: "test_${id%4}",
			sql:      "insert into test set id=1, name=2, value='333'",
			sqls: &SqlGenResult{
				Commands: []*ScatterCommand{
					{
						DataSource: "db1",
						SqlCommand: "insert into test_1 set id=1, name=2, value='333'", //补列
					},
				},
			},
		},
		{
			tbInline: "test_${id%4}",
			sql:      "insert into test set id=8, name=2, value='333'",
			sqls: &SqlGenResult{
				Commands: []*ScatterCommand{
					{
						DataSource: "db1",
						SqlCommand: "insert into test_0 set id=1, name=2, value='333'", //补列
					},
				},
			},
		},
	}
	runInsertTestCases(t, tests)
}

func runInsertTestCases(t *testing.T, tests []shardCommandTestCase) {
	for _, test := range tests {
		t.Run(test.sql, func(tt *testing.T) {
			privder := useShardingTables(tt, test.tbInline)
			stmt := testkit.ParseInsert(test.sql, tt)

			expl := explain.NewSqlExplain(privder)
			err := expl.ExplainInsert(stmt, rewriting.NewRewriter())
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


