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

func GenerateSql(defaultDatabase string, stmt ast.StmtNode, explain *explain.SqlExplain) (map[string][]string, error) {
	values := explain.GetShardingValues()

	context := explain.CurrentContext()
	runtime, err := NewGenerationRuntime(defaultDatabase, context, values)

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
