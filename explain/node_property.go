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

import "github.com/pingcap/parser/ast"

type NodeProperty interface {
	Get() ast.ExprNode
	Set(newNode ast.ExprNode)
	IsReadonly() bool
}

func NewNodePropertyReadonly(raw ast.ExprNode) NodeProperty {
	return NewNodeProperty(raw, nil)
}

func NewNodeProperty(raw ast.ExprNode, setFunc func(n ast.ExprNode)) NodeProperty {
	return &nodeSetter{
		val:     raw,
		setFunc: setFunc,
	}
}

type nodeSetter struct {
	val     ast.ExprNode
	setFunc func(node ast.ExprNode)
}

func (n *nodeSetter) IsReadonly() bool {
	return n.setFunc == nil
}

func (n *nodeSetter) Get() ast.ExprNode {
	return n.val
}

func (n *nodeSetter) Set(newNode ast.ExprNode) {
	if n.setFunc != nil {
		n.setFunc(newNode)
		n.val = newNode
	}
}
