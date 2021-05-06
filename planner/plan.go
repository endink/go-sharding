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
	"github.com/endink/go-sharding/core"
	"github.com/endink/go-sharding/explain"
	"github.com/endink/go-sharding/parser"
	"sync"
	"time"
)

type Plan struct {
	PlanID      PlanType
	Original    string // Original is the original query.
	Permissions []core.Permission
	explain     *explain.SqlExplain

	FullQuery    *parser.ParsedQuery
	FieldQuery   *parser.ParsedQuery
	Query        *parser.ParsedQuery
	mu           sync.Mutex    // Mutex to protect the fields below
	ExecCount    uint64        // Count of times this plan was executed
	ExecTime     time.Duration // Total execution time
	ShardQueries uint64        // Total number of shard queries
	Rows         uint64        // Total number of rows
	Errors       uint64        // Total number of errors
}
