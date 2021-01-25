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

package gen

import (
	"fmt"
	"github.com/XiaoMi/Gaea/core"
	"github.com/XiaoMi/Gaea/explain"
	"github.com/XiaoMi/Gaea/util"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/format"
	"github.com/scylladb/go-set/strset"
	"strings"
)

func GenerateSql(defaultDatabase string, stmt ast.StmtNode, explain *explain.SqlExplain) (map[string][]string, error) {
	values := explain.GetShardingValues()

	context := explain.CurrentContext()
	runtime, err := builderRuntime(defaultDatabase, context, values)

	if err != nil {
		return nil, err
	}

	return genSqlWithRuntime(stmt, runtime)
}

func genSqlWithRuntime(stmt ast.StmtNode, runtime *genRuntime) (map[string][]string, error) {
	result := make(map[string][]string, len(runtime.databases))
	for {
		var firstDb string
		if runtime.Next() {
			currentDb, e := runtime.GetCurrentDatabase()
			if e != nil {
				return nil, e
			}
			if firstDb == "" {
				firstDb = currentDb
			}
			if firstDb == currentDb {
				sb := &strings.Builder{}
				ctx := format.NewRestoreCtx(util.EscapeRestoreFlags, sb)
				if restErr := stmt.Restore(ctx); restErr != nil {
					return nil, restErr
				}

				var sql = sb.String()
				var sqls []string
				var ok bool
				if sqls, ok = result[currentDb]; !ok {
					sqls = make([]string, 0, runtime.GetShardLength())
					result[currentDb] = sqls
				}
				sqls = append(sqls, sql)
			} else { //其他数据库简单的使用之前的生成结果， 预留后期如果改写 DB 在这里处理代码块
				return result, nil
			}
		} else {
			break
		}
	}
	return result, nil
}

func builderRuntime(defaultDatabase string, context explain.Context, values map[string]*core.ShardingValues) (*genRuntime, error) {
	shardingTables := context.TableLookup().GetTables()
	if len(shardingTables) > 0 {
		allTables := make([][]string, 0, len(shardingTables))
		allDatabases := strset.New()

		for _, table := range shardingTables {
			shardingTable, hasTable := context.TableLookup().FindShardingTable(table)
			if !hasTable {
				return nil, fmt.Errorf("sharding table '%s' not existed", shardingTable)
			}
			shardingValues, _ := values[table]
			databases, dbErr := shardDatabase(shardingValues, shardingTable, defaultDatabase)
			if dbErr == nil {
				return nil, dbErr
			}
			allDatabases.Add(databases...)

			physicalTables, tbErr := shardTables(shardingValues, shardingTable)
			if tbErr == nil {
				return nil, tbErr
			}
			allTables = append(allTables, physicalTables)
		}

		dbs := allDatabases.List()
		resources := make([][]string, len(shardingTables))
		resources[0] = dbs
		resources = append(resources, allTables...)

		return &genRuntime{
			resources:       core.PermuteString(resources),
			shardingTables:  shardingTables,
			defaultDatabase: defaultDatabase,
			databases:       dbs,
			currentIndex:    -1,
			currentTableMap: make(map[string]string, len(shardingTables)),
		}, nil
	}
	return nil, fmt.Errorf("have no any sharding table used in sql")
}

func shardDatabase(shardingValues *core.ShardingValues, shardingTable *core.ShardingTable, defaultDb string) ([]string, error) {
	if shardingValues == nil || shardingValues.IsEmpty() {
		return shardingTable.GetDatabases(), nil
	} else if !shardingTable.IsDbSharding() {
		return []string{defaultDb}, nil
	} else {
		allDatabases := shardingTable.GetDatabases()
		physicalDbs, shardErr := shardingTable.DatabaseStrategy.Shard(allDatabases, shardingValues)
		if shardErr != nil {
			return nil, shardErr
		}
		return physicalDbs, nil
	}
}

func shardTables(shardingValues *core.ShardingValues, shardingTable *core.ShardingTable) ([]string, error) {
	if shardingValues == nil || shardingValues.IsEmpty() {
		return shardingTable.GetTables(), nil
	} else if !shardingTable.IsTableSharding() {
		return []string{shardingTable.Name}, nil
	} else {

		allTables := shardingTable.GetTables()
		physicalTables, shardErr := shardingTable.TableStrategy.Shard(allTables, shardingValues)
		if shardErr != nil {
			return nil, shardErr
		}
		return physicalTables, nil
	}
}
