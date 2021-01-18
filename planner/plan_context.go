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

package planner

import (
	"github.com/XiaoMi/Gaea/core"
	"github.com/pingcap/parser/ast"
	"strings"
)

type PlanContext struct {
	Settings *core.Settings
	schemaL  string
	rawSql   string
	stmt     ast.StmtNode
}

func NewPlanContext(sql string, stmt ast.StmtNode, settings *core.Settings) *PlanContext {
	r := &PlanContext{
		Settings: settings,
		schemaL:  strings.ToLower(settings.Server.Schema),
		rawSql:   sql,
		stmt:     stmt,
	}
	return r
}

func (ctx *PlanContext) GetRawSql() string {
	return ctx.rawSql
}

func (ctx *PlanContext) GetSchema() string {
	return ctx.Settings.Server.Schema
}

func (ctx *PlanContext) IsShardingTable(tableName string) bool {
	_, ok := ctx.Settings.ShardingRule.Tables[tableName]
	return ok
}

func (ctx *PlanContext) GetShardingTable(tableName string) (*core.ShardingTable, bool) {
	if r, ok := ctx.Settings.ShardingRule.Tables[tableName]; ok {
		return r, ok
	} else {
		return nil, false
	}
}
