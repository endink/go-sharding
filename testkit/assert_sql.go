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

package testkit

import (
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/format"
	"github.com/stretchr/testify/assert"
	"strings"
	"testing"
)

func NormalizeSql(t testing.TB, sql string) string {
	s := ParseForTest(sql, t)
	return writeNode(t, s)
}

func AssertEqualSql(t testing.TB, sql1 string, sql2 string) bool {
	n1 := ParseForTest(sql1, t)
	n2 := ParseForTest(sql2, t)

	s1 := writeNode(t, n1)
	s2 := writeNode(t, n2)
	return assert.Equal(t, s1, s2)
}

func writeNode(t testing.TB, node ast.Node) string {
	var sb = new(strings.Builder)
	ctx := format.NewRestoreCtx(format.DefaultRestoreFlags|format.RestoreSpacesAroundBinaryOperation, sb)
	err := node.Restore(ctx)
	assert.Nil(t, err)
	return sb.String()
}
