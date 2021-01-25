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
	"errors"
	"fmt"
	"github.com/XiaoMi/Gaea/core"
	"github.com/XiaoMi/Gaea/explain"
	"github.com/scylladb/go-set/strset"
)

var _ explain.Runtime = &genRuntime{}

var ErrRuntimeResourceNotFound = errors.New("resource was not found in runtime")

func NewGenerationRuntime(defaultDatabase string, context explain.Context, values map[string]*core.ShardingValues) (*genRuntime, error) {
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

type genRuntime struct {
	resources      [][]string //二维数值表示实际数据表，存在多个分片表时取得笛卡尔积, 其中最后一列表示数据库
	shardingTables []string   //分片表逻辑表名
	databases      []string

	currentTableMap map[string]string // 当前的逻辑表和实际表对应关系， key = 分片表名， value = 实际表名
	currentDb       string

	currentIndex    int // 当前执行的索引，滑动 physicalTables 游标来切换表
	defaultDatabase string
}

func (g *genRuntime) GetShardLength() int {
	if len(g.databases) == 0 {
		return 0
	}
	return len(g.resources) / len(g.databases)
}

func (g *genRuntime) GetCurrent(shardingTable string) (database string, table string, err error) {
	if t, terr := g.GetCurrentTable(shardingTable); terr == nil {
		if db, derr := g.GetCurrentDatabase(); derr == nil {
			return db, t, nil
		}
	}
	return "", "", ErrRuntimeResourceNotFound
}

func (g *genRuntime) GetCurrentTable(shardingTable string) (string, error) {
	if t, ok := g.currentTableMap[shardingTable]; ok {
		return t, nil
	}
	return "", ErrRuntimeResourceNotFound
}

func (g *genRuntime) GetCurrentDatabase() (string, error) {
	if g.currentDb == "" {
		return g.currentDb, ErrRuntimeResourceNotFound
	}
	return g.currentDb, nil
}

func (g *genRuntime) GetServerSchema() string {
	return g.defaultDatabase
}

func (g *genRuntime) Next() bool {
	l := len(g.resources)
	hasNext := l > 0 && g.currentIndex < l
	if hasNext {
		g.currentIndex++
		resource := g.resources[g.currentIndex]
		sourceLen := len(resource)
		for i, s := range resource {
			if i < (sourceLen - 1) {
				shardTable := g.shardingTables[i]
				phyTable := s
				g.currentTableMap[shardTable] = phyTable
			} else { //最后一个元素是数据库名
				g.currentDb = s
			}
		}
	} else {

	}
	return hasNext
}
