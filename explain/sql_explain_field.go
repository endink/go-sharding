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
	"github.com/pingcap/parser/ast"
)

func (s *SqlExplain) ExplainFields(stmt *ast.SelectStmt, rewriter Rewriter) error {
	fields := stmt.Fields
	if fields == nil {
		return nil
	}

	if e := s.rewriteColumn(fields, rewriter, "explain fields fault"); e != nil {
		return e
	}

	ctx := s.CurrentContext()
	for i, field := range stmt.Fields.Fields {
		err := ctx.AggLookup().visit(i, field)
		if err == nil {
			err = ctx.FieldLookup().addField(i, field)
		}
		if err != nil {
			return err
		}
	}
	return nil
}