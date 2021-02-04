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

import (
	"fmt"
	"github.com/pingcap/parser/ast"
	driver "github.com/pingcap/tidb/types/parser_driver"
	"math"
)

type paramVisitor struct {
	//markers []ast.ParamMarkerExpr
	paramCount int
}

func (v *paramVisitor) Enter(in ast.Node) (ast.Node, bool) {
	return in, false
}

func (v *paramVisitor) Leave(in ast.Node) (ast.Node, bool) {
	if _, ok := in.(*driver.ParamMarkerExpr); ok {
		//v.markers = append(v.markers, x)
		v.paramCount++
	}
	return in, true
}

func ParseSqlParamCount(sql string) (uint16, error) {
	stmt, err := ParseSQL(sql)
	if err != nil {
		return 0, err
	}
	var paramVt paramVisitor
	stmt.Accept(&paramVt)

	// DDL Statements can not accept parameters
	if _, ok := stmt.(ast.DDLNode); ok && paramVt.paramCount > 0 {
		return 0, fmt.Errorf("parameter in ddl statement is not supported")
	}

	switch stmt.(type) {
	case *ast.LoadDataStmt, *ast.PrepareStmt, *ast.ExecuteStmt, *ast.DeallocateStmt:
		return 0, fmt.Errorf("prepare statement LoadDataStmt, PrepareStmt, ExecuteStmt, DeallocateStmt is not supported")
	}

	// Prepare parameters should NOT over 2 bytes(MaxUint16)
	// https://dev.mysql.com/doc/internals/en/com-stmt-prepare-response.html#packet-COM_STMT_PREPARE_OK.
	if paramVt.paramCount > math.MaxUint16 {
		return 0, fmt.Errorf("sql parameter count out of limit ( allow max: %d )", math.MaxUint16)
	}

	return uint16(paramVt.paramCount), nil
}
