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
	"github.com/XiaoMi/Gaea/mysql/types"
	"github.com/XiaoMi/Gaea/parser"
	"github.com/pingcap/parser/format"
	"github.com/scylladb/go-set/strset"
)

var _ explain.Runtime = &genRuntime{}

var ErrRuntimeResourceNotFound = errors.New("resource was not found in runtime")

func NewRuntime(
	defaultDatabase string,
	explain *explain.SqlExplain,
	values map[string]*core.ShardingValues,
	bindVars []*types.BindVariable) (*genRuntime, error) {
	//获取用于循环的所有分片表逻辑表名

	usedShardingTables := make([]*core.ShardingTable, 0, len(values))
	shardingTableNames := strset.New()

	var err error
	tables := explain.Context().TableLookup().ShardingTables()
	for _, table := range tables {
		usedShardingTables, err = appendShardingTables(table, explain, shardingTableNames, usedShardingTables)
		if err != nil {
			return nil, err
		}
	}

	for table, _ := range values {
		usedShardingTables, err = appendShardingTables(table, explain, shardingTableNames, usedShardingTables)
		if err != nil {
			return nil, err
		}
	}

	var (
		dbs      []string
		manifest [][]string
	)
	if len(usedShardingTables) > 0 {

		allTables := make([][]string, 0, len(usedShardingTables))
		allDatabases := strset.New() //数据库有重复项,简单起见，使用 set

		for _, shardingTable := range usedShardingTables {
			shardingValues, _ := values[shardingTable.Name]
			//根据分片列的值计算数据库分片
			databases, dbErr := shardDatabase(shardingValues, shardingTable, defaultDatabase, explain.Context().ContainsFullShardColumn())

			if dbErr != nil {
				return nil, dbErr
			}
			allDatabases.Add(databases...)

			//根据分片表的值计算表分片，约定分片算法返回的物理表不会重复
			physicalTables, tbErr := shardTables(shardingValues, shardingTable, explain.Context().ContainsFullShardColumn())
			if tbErr != nil {
				return nil, tbErr
			}
			allTables = append(allTables, physicalTables)
		}

		//整理一下，将数据库放在首位
		dbs = allDatabases.List()
		resources := make([][]string, 0, len(usedShardingTables)+1)
		resources = append(resources, dbs)
		resources = append(resources, allTables...)
		//合并数据库、表形成资源清单（笛卡尔积），得到迭代器数据，或许这里可以优化，实际可以使用上面的数据来循环了
		manifest = core.PermuteString(resources)

	} else {
		dbs = []string{defaultDatabase}
		manifest = [][]string{{defaultDatabase}}
	}

	return &genRuntime{
		restoreFlags:    parser.EscapeRestoreFlags,
		resources:       manifest,
		shardingTables:  shardingTableNames.List(),
		defaultDatabase: defaultDatabase,
		databases:       dbs,
		currentIndex:    -1,
		currentTableMap: make(map[string]string, len(usedShardingTables)),
		bindVars:        bindVars,
	}, nil
}

func appendShardingTables(table string, shardingTableProvider explain.ShardingTableProvider, shardingTableNames *strset.Set, usedShardingTables []*core.ShardingTable) ([]*core.ShardingTable, error) {
	sd, ok := shardingTableProvider.GetShardingTable(table)
	if !ok {
		return nil, fmt.Errorf("sharding table '%s' is not found", table)
	}

	if !shardingTableNames.Has(sd.Name) {
		tables := append(usedShardingTables, sd)
		shardingTableNames.Add(sd.Name)
		return tables, nil
	}
	return usedShardingTables, nil
}

func shardDatabase(shardingValues *core.ShardingValues, shardingTable *core.ShardingTable, defaultDb string, hasFullShardColumn bool) ([]string, error) {
	if shardingValues == nil || shardingValues.IsEmpty() {
		return shardingTable.GetDatabases(), nil
	} else if !shardingTable.IsDbShardingSupported() {
		return []string{defaultDb}, nil
	} else {
		allDatabases := shardingTable.GetDatabases()

		sty := core.DetectShardType(shardingTable.DatabaseStrategy, shardingValues, hasFullShardColumn)
		switch sty {
		case core.ShardImpossible:
			return make([]string, 0), nil
		case core.ShardScatter:
			physicalDbs, shardErr := shardingTable.DatabaseStrategy.Shard(allDatabases, shardingValues)
			if shardErr != nil {
				return nil, shardErr
			}
			return physicalDbs, nil
		}
		return allDatabases, nil
	}
}

func shardTables(shardingValues *core.ShardingValues, shardingTable *core.ShardingTable, hasNotPartitionedColumn bool) ([]string, error) {
	if shardingValues == nil || shardingValues.IsEmpty() {
		return shardingTable.GetTables(), nil
	} else if !shardingTable.IsTableShardingSupported() {
		return []string{shardingTable.Name}, nil
	} else {
		allTables := shardingTable.GetTables()

		sty := core.DetectShardType(shardingTable.TableStrategy, shardingValues, hasNotPartitionedColumn)

		switch sty {
		case core.ShardImpossible:
			return make([]string, 0), nil
		case core.ShardScatter:
			physicalTables, shardErr := shardingTable.TableStrategy.Shard(allTables, shardingValues)
			if shardErr != nil {
				return nil, shardErr
			}
			return physicalTables, nil
		}
		return allTables, nil
	}
}

type genRuntime struct {
	resources      [][]string //二维数值表示实际数据表，存在多个分片表时取得笛卡尔积, 第一列表示数据库， 后续为物理表
	shardingTables []string   //分片表逻辑表名
	databases      []string

	currentTableMap map[string]string // 当前的逻辑表和实际表对应关系， key = 分片表名， value = 实际表名
	currentDb       string

	currentIndex    int // 当前执行的索引，滑动 physicalTables 游标来切换表
	defaultDatabase string
	restoreFlags    format.RestoreFlags
	bindVars        []*types.BindVariable
	removedVars     []int
}

func (g *genRuntime) GetCurrentBindVariables() []*types.BindVariable {
	vars := make([]*types.BindVariable, 0, len(g.bindVars)-len(g.removedVars))

	for index, v := range g.bindVars {
		if !g.isRemovedParam(index) {
			vars = append(vars, v)
			index++
		}
	}
	return vars
}

func (g *genRuntime) isRemovedParam(index int) bool {
	for _, removedIndex := range g.removedVars {
		if index == removedIndex {
			return true
		}
	}
	return false
}

func (g *genRuntime) RemoveParameter(index int) bool {
	ok := index >= 0 && index < len(g.bindVars)
	if ok {
		g.removedVars = append(g.removedVars, index)
	}
	return ok
}

func (g *genRuntime) GetRestoreFlags() format.RestoreFlags {
	return g.restoreFlags
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
	g.removedVars = nil
	g.currentIndex++
	hasNext := l > 0 && g.currentIndex < l
	if hasNext {
		resource := g.resources[g.currentIndex]
		for i, s := range resource {
			if i == 0 {
				g.currentDb = s
			} else if len(resource) > 0 { //可能没有分片表
				shardTable := g.shardingTables[i-1] //分片表从第二列开始
				phyTable := s
				g.currentTableMap[shardTable] = phyTable
			}
		}
	}
	return hasNext
}
