/*
 *
 *  * Copyright 2021. Go-Sharding Author All Rights Reserved.
 *  *
 *  *  Licensed under the Apache License, Version 2.0 (the "License");
 *  *  you may not use this file except in compliance with the License.
 *  *  You may obtain a copy of the License at
 *  *
 *  *      http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  *  Unless required by applicable law or agreed to in writing, software
 *  *  distributed under the License is distributed on an "AS IS" BASIS,
 *  *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  *  See the License for the specific language governing permissions and
 *  *  limitations under the License.
 *  *
 *  *  File author: Anders Xiao
 *
 */

package planner

import (
	config2 "github.com/XiaoMi/Gaea/config"
	"github.com/XiaoMi/Gaea/parser"
	"github.com/pingcap/parser/ast"
	"github.com/stretchr/testify/assert"
	"go.uber.org/config"
	"strings"
)

const TestYAML = `
sources:
  ds0: 
    endpoint: localhost:3306
    schema: test_db
    username: root
    password: 
  ds1:
    endpoint: localhost:3306
    schema: test_db
    username: root
    password: 

default-source: ds0

rule:  
  tables:
    t_order: 
      resources: ds${range(0,1)}.t_order${[0,1]}
      db-strategy:
        inline:
          sharding-columns: user_id
          expression: ds${user_id % 2}
      table-strategy: 
        inline:
          sharding-columns: order_id
          expression: t_order${order_id % 2}
      keyGenerator:
        type: SNOWFLAKE
        column: order_id
    t_order_item:
      resources: ds${range(0,1)}.t_order_item${range(0,1)}
      db-strategy:
        inline:
          sharding-columns: user_id
          expression: ds${user_id % 2}
      table-strategy:
        inline:
          sharding-columns: order_id
          expression: t_order_item${order_id % 2}  
    t_product:
      db-strategy: none
      table-strategy: none

server: 
  port: 13308
  username: root
  password: root2
  schema: test
`

func newTestManager(yamlContent string, t assert.TestingT) config2.Manager {
	r := strings.NewReader(yamlContent)
	opt := config.Source(r)
	permissive := config.Permissive()
	yml, err := config.NewYAML(opt, permissive)
	assert.Nil(t, err, "yml bad format")

	m, err := config2.NewManagerFromYAML(yml)
	assert.Nil(t, err, "create config manager fault")
	return m
}

type PlanTester struct {
	Sql         string
	Stmt        ast.StmtNode
	HasError    bool
	Err         error
	PlanContext *PlanContext
}

func NewPlanTester(t assert.TestingT, sql string) *PlanTester {
	stmt, err := parser.ParseSQL(sql)
	assert.Nil(t, err, "bad sql: %s", sql)
	settings := newTestManager(TestYAML, t).GetSettings()

	return &PlanTester{
		sql,
		stmt,
		err != nil,
		err,
		NewPlanContext(sql, stmt, settings),
	}
}
