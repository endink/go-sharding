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

package planner

import (
	"github.com/XiaoMi/Gaea/core"
	"github.com/XiaoMi/Gaea/explain"
	"github.com/XiaoMi/Gaea/parser"
	"github.com/pingcap/parser/ast"
)

type tableFinder struct {
	tables map[string]*core.ShardingTable
}

func planSelect(sel ast.StmtNode, tables explain.ShardingTableProvider) (*Plan, error) {
	query, err := parser.GenerateLimitQuery(sel, 1000)
	if err != nil {
		return nil, err
	}
	fieldQuery, err := parser.GenerateFieldQuery(sel)
	if err != nil {
		return nil, err
	}
	plan := &Plan{
		PlanID:     PlanSelect,
		Query:      query,
		FieldQuery: fieldQuery,
	}

	switch stmt := sel.(type) {
	case *ast.SelectStmt:
		if stmt.LockTp == ast.SelectLockForUpdate || stmt.LockTp == ast.SelectLockForUpdateNoWait {
			plan.PlanID = PlanSelectLock
		}
		if stmt.Where != nil {
			comp, ok := stmt.Where.(*ast.BinaryOperationExpr)
			if ok && parser.IsImpossibleExpr(comp) {
				plan.PlanID = PlanSelectImpossible
				return plan, nil
			}
		}
		exp := explain.NewSqlExplain(tables)
		exp.ExplainSelect()
	}

	return plan, nil
}
