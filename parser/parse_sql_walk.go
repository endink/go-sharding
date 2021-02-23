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

import "github.com/pingcap/parser/ast"

var _ ast.Visitor = &walkVisitor{}

// Visit defines the signature of a function that
// can be used to visit all nodes of a parse tree.
type Visit func(node ast.Node) (kontinue bool, err error)

// Walk calls visit on every node.
// If visit returns true, the underlying nodes
// are also visited. If it returns an error, walking
// is interrupted, and the error is returned.
func Walk(visit Visit, nodes ...ast.Node) error {
	v := &walkVisitor{
		visit: visit,
	}
	for _, node := range nodes {
		if node == nil {
			continue
		}
		v.reset()
		node.Accept(v)
		if v.err != nil {
			return v.err
		}
	}
	return nil
}

type walkVisitor struct {
	err   error
	visit Visit
	skip  bool
}

func (w *walkVisitor) reset() {
	w.skip = false
	w.err = nil
}

func (w *walkVisitor) Enter(n ast.Node) (node ast.Node, skipChildren bool) {
	c, er := w.visit(n)
	if er != nil {
		w.err = er
		return n, true // we have to return true here so that post gets called
	}
	w.skip = !c
	return n, w.skip
}

func (w *walkVisitor) Leave(n ast.Node) (node ast.Node, ok bool) {
	return n, w.err == nil && !w.skip
}
