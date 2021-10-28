package gen

import (
	"github.com/endink/go-sharding/explain"
	"github.com/endink/go-sharding/rewriting"
	"github.com/endink/go-sharding/testkit"
	"github.com/stretchr/testify/assert"
	"strings"
	"testing"
)

func TestDelete(t *testing.T){
	tests := []shardCommandTestCase{
		{
			tbInline: "test_${id%4}",
			sql:      "delete from test where id=1",
			sqls: &SqlGenResult{
				Commands: []*ScatterCommand{
					{
						DataSource: "db1",
						SqlCommand: "delete from test_1 where id=1", //补列
					},
				},
			},
		},
		{
			tbInline: "test_${id%4}",
			sql:      "delete from test where id=8",
			sqls: &SqlGenResult{
				Commands: []*ScatterCommand{
					{
						DataSource: "db1",
						SqlCommand: "delete from test_0 where id=8", //补列
					},
				},
			},
		},
		{
			tbInline: "test_${id%4}",
			sql:      "delete from test where name='3'",
			sqls: &SqlGenResult{
				Commands: []*ScatterCommand{
					{
						DataSource: "db1",
						SqlCommand: "delete from test_0 where name='3'", //补列
					},
					{
						DataSource: "db1",
						SqlCommand: "delete from test_1 where name='3'", //补列
					},
					{
						DataSource: "db1",
						SqlCommand: "delete from test_2 where name='3'", //补列
					},
					{
						DataSource: "db1",
						SqlCommand: "delete from test_3 where name='3'", //补列
					},
				},
			},
		},
	}
	runDeleteTestCases(t, tests)
}

func runDeleteTestCases(t *testing.T, tests []shardCommandTestCase) {
	for _, test := range tests {
		t.Run(test.sql, func(tt *testing.T) {
			provider := useShardingTables(tt, test.tbInline)
			stmt := testkit.ParseDelete(test.sql, tt)

			expl := explain.NewSqlExplain(provider)
			err := expl.ExplainDelete(stmt, rewriting.NewRewriter())
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
