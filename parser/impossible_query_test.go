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

package parser

import (
	"github.com/XiaoMi/Gaea/testkit"
	"testing"
)

func TestSelectSimple(t *testing.T) {
	testCases := []struct {
		name string
		sql  string
		r    string
	}{
		{
			name: "simple-select",
			sql:  "select a, b, c, d from e where a = 3 and b = 4",
			r:    "select a, b, c, d from e where 1!=1",
		},
		{
			name: "join",
			sql:  "select e.a, e.b, e.c, e.d from e join f on e.id = f.id and e.b = 4",
			r:    "select e.a, e.b, e.c, e.d from e join f on e.id = f.id and e.b = 4",
		},
	}

	for _, c := range testCases {
		t.Run(c.name, func(tt *testing.T) {
			buf := NewTrackedBuffer()
			stmt := testkit.ParseSelect(c.sql, t)
			FormatImpossibleQuery(buf, stmt)

			rewrote := buf.ParsedQuery().Query

			testkit.AssertEqualSql(tt, c.r, rewrote)
		})
	}
}
