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
	"github.com/XiaoMi/Gaea/explain"
	"github.com/XiaoMi/Gaea/util"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/format"
	"strings"
)

func GenerateSql(defaultDatabase string, stmt ast.StmtNode, explain *explain.SqlExplain) (*SqlGenResult, error) {
	values := explain.GetShardingValues()

	if len(values) == 0 { //没有存在任何分片表数据
		return &SqlGenResult{
			DataSources: []string{defaultDatabase},
			Usage:       UsageRaw,
		}, nil
	}

	runtime, err := NewRuntime(defaultDatabase, explain, values)

	if err != nil {
		return nil, err
	}

	return gen(stmt, runtime)
}

func gen(stmt ast.StmtNode, runtime *genRuntime) (*SqlGenResult, error) {

	genResult := &SqlGenResult{
		SqlCommands: make([]string, 0, runtime.GetShardLength()),
		DataSources: runtime.databases,
		Usage:       UsageShard,
	}

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
				//迭代执行改写引擎
				ctx := format.NewRestoreCtx(util.EscapeRestoreFlags, sb)
				if restErr := stmt.Restore(ctx); restErr != nil {
					return nil, restErr
				}

				var sql = sb.String()
				genResult.SqlCommands = append(genResult.SqlCommands, sql)
			} else { //其他数据库简单的使用之前的生成结果， 预留后期如果改写 DB 在这里处理代码块
				return genResult, nil
			}
		} else {
			//其他数据库循环重复生成目前没有意义，留作将来扩展
			break
		}
	}
	return genResult, nil
}
