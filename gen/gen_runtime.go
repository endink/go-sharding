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
	"github.com/XiaoMi/Gaea/explain"
)

var _ explain.Runtime = &genRuntime{}

var ErrRuntimeResourceNotFound = errors.New("resource was not found in runtime")

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
