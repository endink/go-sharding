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

package rewriting

import (
	"fmt"
	"github.com/XiaoMi/Gaea/explain"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/format"
)

var _ ast.Node = &TableNameWriter{}

type TableNameWriter struct {
	origin        *ast.TableName
	runtime       explain.Runtime
	shardingTable string
}

func NewTableNameWriter(n *ast.TableName, context explain.Context) (*TableNameWriter, error) {
	if len(n.PartitionNames) != 0 {
		return nil, fmt.Errorf("TableName does not support PartitionNames in sharding")
	}

	ret := &TableNameWriter{
		origin:        n,
		runtime:       context.Runtime(),
		shardingTable: n.Name.L,
	}

	return ret, nil
}

// Restore implement ast.Node
func (t *TableNameWriter) Restore(ctx *format.RestoreCtx) error {
	table, err := t.runtime.GetCurrentTable(t.shardingTable)
	if err != nil {
		return err
	}

	//if t.origin.Schema.String() != "" {
	//	ctx.WriteName(db)
	//	ctx.WritePlain(".")
	//}

	ctx.WriteName(table)
	return nil
}

func (t *TableNameWriter) Accept(v ast.Visitor) (ast.Node, bool) {
	return t, true
}

func (t *TableNameWriter) Text() string {
	return t.origin.Text()
}

func (t *TableNameWriter) SetText(text string) {
	t.origin.SetText(text)
}
