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

package txserializer

import (
	"context"
	"fmt"
	"github.com/endink/go-sharding/explain"
	"github.com/endink/go-sharding/logging"
	"github.com/endink/go-sharding/mysql/types"
	"github.com/endink/go-sharding/parser"
	"github.com/endink/go-sharding/planner"
	"time"
)

var logComputeRowSerializerKey = logging.NewThrottledLogger("ComputeRowSerializerKey", logging.DefaultLogger, 1*time.Minute)

func ComputeTxSerializerKey(ctx context.Context, sql string, stp explain.ShardingTableProvider, bindVariables map[string]*types.BindVariable) (string, string) {
	// Strip trailing comments so we don't pollute the query cache.
	sql, _ = parser.SplitMarginComments(sql)
	plan, err := planner.GetPlan(sql, false /* isReservedConn */, stp)
	if err != nil {
		logComputeRowSerializerKey.Errorf("failed to get plan for query: %v err: %v", sql, err)
		return "", ""
	}

	switch plan.PlanID {
	// Serialize only UPDATE or DELETE queries.
	case planner.PlanUpdate, planner.PlanUpdateLimit,
		planner.PlanDelete, planner.PlanDeleteLimit:
	default:
		return "", ""
	}

	tableName := plan.TableName
	if tableName == "" || plan.Where == nil {
		// Do not serialize any queries without table name or where clause
		return "", ""
	}

	where, err := plan.Where.GenerateQuery(bindVariables, nil)
	if err != nil {
		logComputeRowSerializerKey.Errorf("failed to substitute bind vars in where clause: %v query: %v bind vars: %v", err, sql, bindVariables)
		return "", ""
	}

	// Example: table1 where id = 1 and sub_id = 2
	key := fmt.Sprintf("%s%s", tableName, where)
	return key, tableName
}
