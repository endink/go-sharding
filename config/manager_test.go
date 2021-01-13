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

package config

import (
	"github.com/stretchr/testify/assert"
	"go.uber.org/config"
	"strings"
	"testing"
)

const TestYAML = `
sources:
  ds0: 
    address: localhost:3306
    username: root
    password: 
  ds1:
    address: localhost:3306
    username: root
    password: 

rule:  
  tables:
    t_order: 
      actualDataNodes: ds${0..1}.t_order${0..1}
      databaseStrategy:
        inline:
          shardingColumn: user_id
          algorithmExpression: ds${user_id % 2}
      tableStrategy: 
        inline:
          shardingColumn: order_id
          algorithmExpression: t_order${order_id % 2}
      keyGenerator:
        type: SNOWFLAKE
        column: order_id
    t_order_item:
      actualDataNodes: ds${0..1}.t_order_item${0..1}
      databaseStrategy:
        inline:
          shardingColumn: user_id
          algorithmExpression: ds${user_id % 2}
      tableStrategy:
        inline:
          shardingColumn: order_id
          algorithmExpression: t_order_item${order_id % 2}  
`

func TestNewManager(t *testing.T) {
	r := strings.NewReader(TestYAML)
	opt := config.Source(r)

	yml, err := config.NewYAML(opt)
	assert.Nil(t, err, "yml bad format")

	_, err = NewManagerFromYAML(yml)
	assert.Nil(t, err, "create config manager fault")
}
