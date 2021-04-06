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

package explain

import (
	"fmt"
	"github.com/pingcap/parser/ast"
)

func (s *SqlExplain) explainOrderBy(sn ast.StmtNode, rewriter Rewriter) error {

	switch stmt := sn.(type) {
	case *ast.SelectStmt:
		if stmt.OrderBy == nil {
			return nil
		}

		orderByLookup := s.currentContext().OrderByLookup()
		return s.attachByItems(stmt, stmt.OrderBy.Items, orderByLookup, rewriter)
	case *ast.UpdateStmt:
		if stmt.Where != nil {
			property := NewNodeProperty(stmt.Where, func(n ast.ExprNode) {
				stmt.Where = n
			})
			return s.rewriteCondition(property, rewriter)
		}
	default:
		return fmt.Errorf("explain where is not supported, statement type: '%T'", sel)
	}
	return nil

}
