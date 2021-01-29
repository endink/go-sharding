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

package parser

import (
	tidb "github.com/pingcap/parser"
	"github.com/pingcap/parser/ast"
	_ "github.com/pingcap/tidb/types/parser_driver"
	"sync"
)

var parserPool = sync.Pool{}

func ParseSQL(sql string) (ast.StmtNode, error) {
	var parser *tidb.Parser
	i := parserPool.Get()
	if i != nil {
		parser = i.(*tidb.Parser)
	} else {
		parser = tidb.New()
	}

	defer func() {
		parserPool.Put(parser)
	}()
	return parser.ParseOneStmt(sql, "", "")
}
