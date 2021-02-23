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

import "github.com/pingcap/parser/ast"

// FormatImpossibleQuery creates an impossible query in a TrackedBuffer.
// An impossible query is a modified version of a query where all selects have where clauses that are
// impossible for mysql to resolve. This is used in the vtgate and vttablet:
//
// - In the vtgate it's used for joins: if the first query returns no result, then vtgate uses the impossible
// query just to fetch field info from vttablet
// - In the vttablet, it's just an optimization: the field info is fetched once form MySQL, cached and reused
// for subsequent queries
func FormatImpossibleQuery(buf *TrackedBuffer, stmt ast.StmtNode) {
	switch node := stmt.(type) {
	case *ast.SelectStmt:
		buf.astPrintf("select %v from %v where 1 != 1", node.Fields, node.From)
		if node.GroupBy != nil {
			buf.astPrintf("%v", node.GroupBy)
		}
	case *ast.UnionStmt:
		buf.astPrintf("%v", node.SelectList.Selects[0])
		for _, us := range node.SelectList.Selects {
			buf.astPrintf("%v", us)
		}
	default:
		buf.astPrintf("%v", node)
	}
}
