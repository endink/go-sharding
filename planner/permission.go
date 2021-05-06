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
	"github.com/endink/go-sharding/core"
	"github.com/pingcap/parser/ast"
)

// BuildPermissions builds the list of required permissions for all the
// tables referenced in a query.
func BuildPermissions(stmt ast.StmtNode) []core.Permission {
	var permissions []core.Permission
	// All Statement types myst be covered here.
	switch node := stmt.(type) {
	case *ast.UnionStmt:
		for _, selectStmt := range node.SelectList.Selects {
			permissions = buildSubqueryPermissions(selectStmt, core.RoleReader, permissions)
		}
	case *ast.SelectStmt:
		permissions = buildSubqueryPermissions(node, core.RoleReader, permissions)
	case *ast.InsertStmt:
		permissions = buildTableExprPermissions(node.Table, core.RoleWriter, permissions)
		permissions = buildTableExprPermissions(node.Table, core.RoleReader, permissions)
	case *ast.UpdateStmt:
		permissions = buildTableExprPermissions(node.TableRefs, core.RoleWriter, permissions)
		permissions = buildTableExprPermissions(node.TableRefs, core.RoleReader, permissions)
	case *ast.DeleteStmt:
		permissions = buildTableExprsPermissions(node.Tables.Tables, core.RoleWriter, permissions)
		permissions = buildTableExprsPermissions(node.Tables.Tables, core.RoleReader, permissions)
	case *ast.FlushStmt:
		for _, t := range node.Tables {
			permissions = buildTableNamePermissions(t, core.RoleAdmin, permissions)
		}
	case *ast.AdminStmt, *ast.BeginStmt, *ast.CommitStmt, *ast.RollbackStmt,
		*ast.LoadDataStmt, *ast.SetStmt, *ast.ShowStmt,
		*ast.ExplainStmt:
		// no op
	default:
		//if dn, isDDL:=stmt.(ast.DDLNode);isDDL {
		//	for _, t := range dn {
		//		permissions = buildTableNamePermissions(t, core.RoleAdmin, permissions)
		//	}
		//}
		panic(fmt.Errorf("BUG: unexpected statement type: %T", node))
	}
	return permissions
}

func buildSubqueryPermissions(stmt *ast.SelectStmt, role core.Role, permissions []core.Permission) []core.Permission {
	return buildTableExprPermissions(stmt.From.TableRefs, role, permissions)
}

func buildTableExprsPermissions(tables []*ast.TableName, role core.Role, permissions []core.Permission) []core.Permission {
	for _, node := range tables {
		permissions = buildTableExprPermissions(node, role, permissions)
	}
	return permissions
}

func buildTableExprPermissions(node ast.ResultSetNode, role core.Role, permissions []core.Permission) []core.Permission {
	if node == nil {
		return permissions
	}
	switch n := node.(type) {
	case *ast.TableName:
		permissions = buildTableNamePermissions(n, role, permissions)
	case *ast.SelectStmt:
		permissions = buildSubqueryPermissions(n, role, permissions)
	case *ast.Join:
		permissions = buildTableExprPermissions(n.Left, role, permissions)
		permissions = buildTableExprPermissions(n.Right, role, permissions)
	case *ast.TableRefsClause:
		permissions = buildTableExprPermissions(n.TableRefs, role, permissions)
	}
	return permissions
}

func buildTableNamePermissions(node *ast.TableName, role core.Role, permissions []core.Permission) []core.Permission {
	permissions = append(permissions, core.Permission{
		TableName: node.Name.L,
		Role:      role,
	})
	return permissions
}
