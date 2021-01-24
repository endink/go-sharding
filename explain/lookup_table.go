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

import (
	"fmt"
	"github.com/XiaoMi/Gaea/core"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/model"
)

type TableLookup interface {
	addTable(table *ast.TableSource, provider ShardingProvider) error
	FindShardingTable(tableOrAlias string) (*core.ShardingTable, bool)
	ExplicitShardingTableByColumn(column string) (*core.ShardingTable, bool)
	HasAlias(tableName string) bool
	FindNameByAlias(tableName string) (model.CIStr, bool)
	GetTables() []string
}

type tableLookup struct {
	aliasToTableName map[string]model.CIStr
	tables           map[string]model.CIStr
	shardingTables   map[string]*core.ShardingTable
	tableNames       []string
}

func newTableLookup() *tableLookup {
	return &tableLookup{
		aliasToTableName: map[string]model.CIStr{},
		tables:           map[string]model.CIStr{},
		shardingTables:   map[string]*core.ShardingTable{},
	}
}

func (lookup *tableLookup) GetTables() []string {
	return lookup.tableNames
}

func (lookup *tableLookup) FindNameByAlias(tableName string) (model.CIStr, bool) {
	name, found := lookup.aliasToTableName[tableName]
	return name, found
}

func (lookup *tableLookup) HasAlias(tableName string) bool {
	_, found := lookup.aliasToTableName[tableName]
	return found
}

func (lookup *tableLookup) FindShardingTable(tableOrAlias string) (*core.ShardingTable, bool) {
	sd, found := lookup.shardingTables[tableOrAlias]
	return sd, found
}

//该方法仅可用于查询一个表时根据列明查找分片表，多个表时应该明确使用表明查找
//当找到多余一个的分片表时不会返回，通过 bool 值判断是否能够明确分片表
func (lookup *tableLookup) ExplicitShardingTableByColumn(column string) (*core.ShardingTable, bool) {
	var sd *core.ShardingTable
	var found bool
	for _, name := range lookup.tableNames {
		hasFound := found
		sd, found = lookup.FindShardingTable(name)
		if hasFound && found { //找到多余一个时
			return nil, false
		}
	}
	return sd, found
}

func (lookup *tableLookup) addTable(table *ast.TableSource, provider ShardingProvider) error {
	tableName, isTableName := table.Source.(*ast.TableName)
	if !isTableName {
		return fmt.Errorf("table source is not type of TableName, type: %T", table.Source)
	}
	alias := table.AsName.L
	if alias != "" {
		if n, ok := lookup.aliasToTableName[alias]; ok && n.L != tableName.Name.L {
			return fmt.Errorf("duplex table alias in sql, alias: %s, tables: %s, %s", alias, n.O, tableName.Name.O)
		} else {
			lookup.aliasToTableName[alias] = tableName.Name
		}
	}
	if _, ok := lookup.tables[tableName.Name.L]; !ok {
		lookup.tables[tableName.Name.L] = tableName.Name
		lookup.tableNames = append(lookup.tableNames, tableName.Name.L)
	}
	lookup.addShardingTable(tableName.Name.L, provider, alias)
	return nil
}

func (lookup *tableLookup) addShardingTable(table string, provider ShardingProvider, alias string) {
	if table == "" {
		return
	}
	sd, existed := lookup.shardingTables[table]
	if !existed {
		if provider == nil {
			Logger.Warn("because ShardingProvider is null, table sharding is skipped")
		} else {
			if shardingTable, found := provider(table); found {
				lookup.shardingTables[table] = shardingTable
			}
		}
	}
	if alias != "" {
		lookup.shardingTables[alias] = sd
	}
}
