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

package planner

import (
	"fmt"
	"github.com/XiaoMi/Gaea/explain"
	"github.com/XiaoMi/Gaea/mysql/types"
	"github.com/XiaoMi/Gaea/parser"
	"github.com/pingcap/parser/ast"
)

//CanNormalize takes Statement and returns if the statement can be normalized.
func CanNormalize(stmt ast.StmtNode) bool {
	switch stmt.(type) {
	case *ast.SelectStmt, *ast.UnionStmt, *ast.InsertStmt, *ast.UpdateStmt, *ast.DeleteStmt, *ast.SetStmt: // TODO: we could merge this logic into ASTrewriter
		return true
	}
	return false
}

func getPlan(sql string, comments parser.MarginComments, bindVars map[string]*types.BindVariable) (*Plan, error) {
	stmt, err := parser.ParseSQL(sql)
	if err != nil {
		return nil, err
	}

	query := sql
	statement := stmt
}

// Build builds a plan based on the schema.
func buildPlan(statement ast.StmtNode, tables explain.ShardingTableProvider, isReservedConn bool, dbName string) (plan *Plan, err error) {
	if !isReservedConn {
		err = parser.CheckForPoolingUnsafeConstructs(statement)
		if err != nil {
			return nil, err
		}
	}

	//TODO: UNION 不支持
	switch stmt := statement.(type) {
	case *ast.SelectStmt:
		plan, err = planSelect(stmt, tables)
	case *ast.InsertStmt:
		plan, err = analyzeInsert(stmt, tables)
	case *ast.UpdateStmt:
		plan, err = analyzeUpdate(stmt, tables)
	case *ast.DeleteStmt:
		plan, err = analyzeDelete(stmt, tables)
	case *ast.SetStmt:
		plan, err = analyzeSet(stmt), nil
	//case *ast.DDLNode.DDLStatement:
	//	// DDLs and some other statements below don't get fully parsed.
	//	// We have to use the original query at the time of execution.
	//	// We are in the process of changing this
	//	var fullQuery *sqlparser.ParsedQuery
	//	// If the query is fully parsed, then use the ast and store the fullQuery
	//	if stmt.IsFullyParsed() {
	//		fullQuery = GenerateFullQuery(stmt)
	//	}
	//	plan = &Plan{PlanID: PlanDDL, FullQuery: fullQuery}
	case *ast.ShowStmt:
		plan, err = analyzeShow(stmt, dbName)
	case *ast.ExplainStmt:
		plan, err = &Plan{PlanID: PlanOtherRead}, nil
	case *ast.AdminStmt:
		plan, err = &Plan{PlanID: PlanOtherAdmin}, nil
	//case *ast.Savepoint:
	//	plan, err = &Plan{PlanID: PlanSavepoint}, nil
	//case *sqlparser.Release:
	//	plan, err = &Plan{PlanID: PlanRelease}, nil
	//case *ast.SRollback: //Save Point Rollback
	//	plan, err = &Plan{PlanID: PlanSRollback}, nil
	case *ast.LoadDataStmt:
		plan, err = &Plan{PlanID: PlanLoad}, nil
	case *ast.FlushStmt:
		plan, err = &Plan{PlanID: PlanFlush, FullQuery: GenerateFullQuery(stmt)}, nil
	default:
		return nil, fmt.Errorf("invalid SQL")
	}
	if err != nil {
		return nil, err
	}
	plan.Permissions = BuildPermissions(statement)
	return plan, nil
}
