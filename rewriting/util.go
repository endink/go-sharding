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
	"github.com/pingcap/parser/ast"
)

func GetColumnTableName(c *ast.ColumnName, context explain.Context) (string, error) {
	db := context.Runtime().GetServerSchema()
	return getTableNameFromColumn(c, db)
}

func FindShardingTable(n *ast.TableName, context explain.Context) (*core.ShardingTable, bool, error) {
	name, err := getTableName(n, context.Runtime().GetServerSchema())
	if err != nil {
		return nil, false, err
	}
	shardingTable, ok := context.TableLookup().FindShardingTable(name)
	return shardingTable, ok, nil
}

func getTableName(t *ast.TableName, allowedDbName string) (string, error) {
	db := t.Schema.O
	if db != "" && db != allowedDbName {
		return "", fmt.Errorf("cross database is not supported")
	}
	return t.Name.L, nil
}

func getTableNameFromColumn(c *ast.ColumnName, allowedDbName string) (string, error) {
	db := c.Schema.O
	if db != "" && db != allowedDbName {
		return "", fmt.Errorf("cross database is not supported")
	}

	return c.Table.L, nil
}

func FindShardingTableByColumn(columnName *ast.ColumnNameExpr, explainContext explain.Context, explicit bool) (*core.ShardingTable, bool, error) {
	var sd *core.ShardingTable
	var err error
	if columnName.Name.Table.O != "" {
		sd, _ = explainContext.TableLookup().FindShardingTable(columnName.Name.Table.L)
	} else if explicit {
		sd, err = explainContext.TableLookup().ExplicitShardingTableByColumn(columnName.Name.Name.L)
	}
	return sd, sd != nil, err
}
