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

package explain

import (
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/types"
)

type StatementFormatter interface {
	Format(ctx StatementContext) error
	Text() string
	GetType() *types.FieldType
	GetFlag() uint64
}

type nodeFormatter struct {
	node ast.Node
}

func (a *nodeFormatter) Format(ctx StatementContext) error {
	return a.node.Restore(ctx.CreateRestoreCtx())
}

func (a *nodeFormatter) Text() string {
	return a.Text()
}

func (a *nodeFormatter) GetType() *types.FieldType {
	return a.GetType()
}

func (a *nodeFormatter) GetFlag() uint64 {
	if expr, ok := a.node.(ast.ExprNode); ok {
		return expr.GetFlag()
	}
	return 0
}
