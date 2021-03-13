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

package testkit

import (
	"fmt"
	"github.com/pingcap/parser"
	"github.com/pingcap/parser/ast"
	_ "github.com/pingcap/tidb/types/parser_driver"
	"github.com/stretchr/testify/assert"
	"sync"
	"testing"
)

var TestParser = parser.New()
var testParserMutex sync.Mutex

func ParseSelect(sql string, t testing.TB) *ast.SelectStmt {
	testParserMutex.Lock()
	defer testParserMutex.Unlock()
	node, err := TestParser.ParseOneStmt(sql, "", "")
	if err != nil {
		t.Fatalf(err.Error())
	}
	sel, ok := node.(*ast.SelectStmt)
	assert.True(t, ok, fmt.Sprint("provided content is not select sql text", "\n", "SQL:", "\n", sql))
	return sel
}

func ParseForTest(sql string, t testing.TB) ast.StmtNode {
	testParserMutex.Lock()
	defer testParserMutex.Unlock()
	node, err := TestParser.ParseOneStmt(sql, "", "")
	if err != nil {
		t.Fatalf("%s\nsql err:%v", sql, err.Error())
	}
	return node
}
