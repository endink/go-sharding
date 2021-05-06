/*
Copyright 2019 The Vitess Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package parser

import (
	"github.com/endink/go-sharding/mysql/types"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGenerateQuery(t *testing.T) {
	tcases := []struct {
		desc     string
		query    string
		bindVars map[string]*types.BindVariable
		extras   map[string]Encodable
		output   string
	}{
		{
			desc:  "no substitutions",
			query: "select * from a where id = 2",
			bindVars: map[string]*types.BindVariable{
				"id": types.Int64BindVariable(1),
			},
			output: "select * from a where id = 2",
		}, {
			desc:  "missing bind var",
			query: "select * from a where id1 = :id1 and id2 = :id2",
			bindVars: map[string]*types.BindVariable{
				"id1": types.Int64BindVariable(1),
			},
			output: "missing bind var id2",
		}, {
			desc:  "simple bindvar substitution",
			query: "select * from a where id1 = :id1 and id2 = :id2",
			bindVars: map[string]*types.BindVariable{
				"id1": types.Int64BindVariable(1),
				"id2": types.NullBindVariable,
			},
			output: "select * from a where id1 = 1 and id2 = null",
		}, {
			desc:  "tuple *types.BindVariable",
			query: "select * from a where id in ::vals",
			bindVars: map[string]*types.BindVariable{
				"vals": types.TestBindVariable([]interface{}{1, "aa"}),
			},
			output: "select * from a where id in (1, 'aa')",
		}, {
			desc:  "list bind vars 0 arguments",
			query: "select * from a where id in ::vals",
			bindVars: map[string]*types.BindVariable{
				"vals": types.TestBindVariable([]interface{}{}),
			},
			output: "empty list supplied for vals",
		}, {
			desc:  "non-list bind var supplied",
			query: "select * from a where id in ::vals",
			bindVars: map[string]*types.BindVariable{
				"vals": types.Int64BindVariable(1),
			},
			output: "unexpected list arg type (INT64) for key vals",
		}, {
			desc:  "list bind var for non-list",
			query: "select * from a where id = :vals",
			bindVars: map[string]*types.BindVariable{
				"vals": types.TestBindVariable([]interface{}{1}),
			},
			output: "unexpected arg type (TUPLE) for non-list key vals",
		}, {
			desc:  "single column tuple equality",
			query: "select * from a where b = :equality",
			extras: map[string]Encodable{
				"equality": &TupleEqualityList{
					Columns: []string{"pk"},
					Rows: [][]types.Value{
						{types.NewInt64(1)},
						{types.NewVarBinary("aa")},
					},
				},
			},
			output: "select * from a where b = pk in (1, 'aa')",
		}, {
			desc:  "multi column tuple equality",
			query: "select * from a where b = :equality",
			extras: map[string]Encodable{
				"equality": &TupleEqualityList{
					Columns: []string{"pk1", "pk2"},
					Rows: [][]types.Value{
						{
							types.NewInt64(1),
							types.NewVarBinary("aa"),
						},
						{
							types.NewInt64(2),
							types.NewVarBinary("bb"),
						},
					},
				},
			},
			output: "select * from a where b = (pk1 = 1 and pk2 = 'aa') or (pk1 = 2 and pk2 = 'bb')",
		},
	}

	for _, tcase := range tcases {
		//buf := BuildParsedQuery(tcase.query)
		pq := BuildParsedQuery(tcase.query)
		bytes, err := pq.GenerateQuery(tcase.bindVars, tcase.extras)
		if err != nil {
			assert.Equal(t, tcase.output, err.Error())
		} else {
			assert.Equal(t, tcase.output, string(bytes))
		}
	}
}

func TestParseAndBind(t *testing.T) {
	testcases := []struct {
		in    string
		binds []*types.BindVariable
		out   string
	}{
		{
			in:  "select * from tbl",
			out: "select * from tbl",
		}, {
			in:  "select * from tbl where b=4 or a=3",
			out: "select * from tbl where b=4 or a=3",
		}, {
			in:  "select * from tbl where b = 4 or a = 3",
			out: "select * from tbl where b = 4 or a = 3",
		}, {
			in:    "select * from tbl where name=%a",
			binds: []*types.BindVariable{types.StringBindVariable("xyz")},
			out:   "select * from tbl where name='xyz'",
		}, {
			in:    "select * from tbl where c=%a",
			binds: []*types.BindVariable{types.Int64BindVariable(17)},
			out:   "select * from tbl where c=17",
		}, {
			in:    "select * from tbl where name=%a and c=%a",
			binds: []*types.BindVariable{types.StringBindVariable("xyz"), types.Int64BindVariable(17)},
			out:   "select * from tbl where name='xyz' and c=17",
		}, {
			in:    "select * from tbl where name=%a",
			binds: []*types.BindVariable{types.StringBindVariable("it's")},
			out:   "select * from tbl where name='it\\'s'",
		}, {
			in:    "where name=%a",
			binds: []*types.BindVariable{types.StringBindVariable("xyz")},
			out:   "where name='xyz'",
		}, {
			in:    "name=%a",
			binds: []*types.BindVariable{types.StringBindVariable("xyz")},
			out:   "name='xyz'",
		}, {
			in:    "name=?",
			binds: []*types.BindVariable{types.StringBindVariable("v0")},
			out:   "name='v0'",
		},
		{
			in:    "name=? and id=?",
			binds: []*types.BindVariable{types.StringBindVariable("v0"), types.StringBindVariable("v1")},
			out:   "name='v0' and id='v1'",
		},
	}

	for _, tc := range testcases {
		t.Run(tc.in, func(t *testing.T) {
			query, err := ParseAndBind(tc.in, tc.binds...)
			assert.NoError(t, err)
			assert.Equal(t, tc.out, query)
		})
	}
}
