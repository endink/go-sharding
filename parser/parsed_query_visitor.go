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
	"github.com/pingcap/parser/ast"
	driver "github.com/pingcap/tidb/types/parser_driver"
)

type varVisitor struct {
	limitCount int64
	defers     []func()
	literal    map[string]interface{}
}

func (v *varVisitor) Enter(n ast.Node) (node ast.Node, skipChildren bool) {
	switch sel := node.(type) {
	case *ast.SelectStmt:
		limit := sel.Limit
		if limit == nil {
			sel.Limit = NewLimit(v.limitCount)
			v.defers = append(v.defers, func() {
				sel.Limit = nil
			})
		}
	case *ast.UnionStmt:
		// Code is identical to *Select, but this one is a *Union.
		limit := sel.Limit
		if limit == nil {
			sel.Limit = NewLimit(v.limitCount)
			v.defers = append(v.defers, func() {
				sel.Limit = nil
			})
		}
	case *driver.ValueExpr:
	case *driver.ParamMarkerExpr:

	}
	return n, false
}

func (v *varVisitor) Leave(n ast.Node) (ast.Node, bool) {
	return n, true
}
