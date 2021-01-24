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

type fieldVisitor struct {
	re      Rewriter
	context Context
}

func NewFieldVisitor(rewriter Rewriter, context Context) *fieldVisitor {
	return &fieldVisitor{
		re:      rewriter,
		context: context,
	}
}

// Enter implement ast.Visitor
func (s *fieldVisitor) Enter(n ast.Node) (node ast.Node, skipChildren bool) {
	return n, false
}

// Leave implement ast.Visitor
func (s *fieldVisitor) Leave(n ast.Node) (node ast.Node, ok bool) {
	field, ok := n.(*ast.ColumnNameExpr)
	if !ok {
		return n, true
	}

	result, err := s.re.RewriteField(field, s.context)
	if err != nil {
		panic(fmt.Errorf("check rewrite column name for ColumnNameExpr error: %v", err))
	}
	if result.IsRewrote() {
		return result.GetNewNode(), true
	}

	return n, true
}
