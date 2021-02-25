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
	"github.com/XiaoMi/Gaea/core"
	"github.com/XiaoMi/Gaea/explain"
	"github.com/XiaoMi/Gaea/proxy/router"
	"github.com/pingcap/parser/ast"
)

var _ explain.StatementFormatter = &BetweenWriter{}

// BetweenExprDecorator decorate BetweenExpr
// Between只需要改写表名并计算路由, 不需要改写边界值.
type BetweenWriter struct {
	*ast.BetweenExpr // origin
	column           *ColumnNameWriter

	tables []string

	rule    router.Rule
	runtime explain.Runtime
}

// NewBetweenWriter create BetweenExprDecorator
func NewBetweenWriter(n *ast.BetweenExpr, shardingTable *core.ShardingTable) (*BetweenWriter, error) {
	columnNameExpr := n.Expr.(*ast.ColumnNameExpr)
	columnNameExprDecorator, err := NewColumnNameWriter(columnNameExpr, shardingTable.Name)
	if err != nil {
		return nil, err
	}

	ret := &BetweenWriter{
		BetweenExpr: n,
		column:      columnNameExprDecorator,
	}
	return ret, nil
}

func (b *BetweenWriter) Format(ctx explain.StatementContext) error {
	rsCtx := ctx.GetRestoreCtx()
	if err := b.column.Restore(rsCtx); err != nil {
		return fmt.Errorf("an error occurred while restore BetweenExpr.Expr: %v", err)
	}
	if b.Not {
		ctx.WriteKeyWord(" NOT BETWEEN ")
	} else {
		ctx.WriteKeyWord(" BETWEEN ")
	}
	if err := b.Left.Restore(rsCtx); err != nil {
		return fmt.Errorf("an error occurred while restore BetweenExpr.Left: %v", err)
	}
	ctx.WriteKeyWord(" AND ")
	if err := b.Right.Restore(rsCtx); err != nil {
		return fmt.Errorf("an error occurred while restore BetweenExpr.Right: %v", err)
	}
	return nil
}
