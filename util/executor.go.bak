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

package util

import (
	"github.com/XiaoMi/Gaea/mysql"
)

// Executor TODO: move to package executor
type Executor interface {

	// 执行分片或非分片单条SQL
	ExecuteSQL(ctx *RequestContext, slice, db, sql string) (*mysql.Result, error)

	// 执行分片SQL
	ExecuteSQLs(*RequestContext, map[string]map[string][]string) ([]*mysql.Result, error)

	// 用于执行INSERT时设置last insert id
	SetLastInsertID(uint64)

	GetLastInsertID() uint64
}
