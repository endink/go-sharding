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

package server

import (
	"context"
	"github.com/XiaoMi/Gaea/database"
	"github.com/XiaoMi/Gaea/mysql/types"
	"github.com/XiaoMi/Gaea/parser"
)

type QueryExecutor struct {
	query          string
	marginComments parser.MarginComments
	bindVars       map[string]*types.BindVariable
	connID         int64
	options        *types.ExecuteOptions
	ctx            context.Context
	executor       *Executor
	target         *database.Target
}

func (qre *QueryExecutor) Execute() (*types.Result, error) {
	return nil, nil
}
