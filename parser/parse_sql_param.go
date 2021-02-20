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
	"github.com/pingcap/parser/ast"
	driver "github.com/pingcap/tidb/types/parser_driver"
)

func ParseSqlParamCount(sql string) (uint16, error) {
	stmt, err := ParseSQL(sql)
	if err != nil {
		return 0, err
	}

	return parseNodeParam(stmt)
}

func parseNodeParam(stmt ast.Node) (uint16, error) {
	var paramCount uint16

	err := Walk(func(node ast.Node) (bool, error) {
		if p, ok := node.(*driver.ParamMarkerExpr); ok {
			p.SetOrder(int(paramCount))
			paramCount++
		}
		return true, nil
	}, stmt)

	if err != nil {
		return 0, err
	}

	return paramCount, nil
}
