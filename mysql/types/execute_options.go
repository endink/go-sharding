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

package types

type IncludedFields int32

const (
	IncludeFieldsTypeAndName IncludedFields = iota
	IncludeFieldsTypeOnly
	IncludeFieldsAll
)

type Workload int32

const (
	WorkloadUnspecified Workload = 0
	WorkloadOLTP
	WorkloadOLAP
	WorkloadDBA
)

type TransactionIsolation int32

const (
	IsolationDefault TransactionIsolation = iota
	IsolationRepeatableRead
	IsolationReadCommitted
	IsolationReadUncommitted
	IsolationSerializable
)

// ExecuteOptions is passed around for all Execute calls.
type ExecuteOptions struct {
	// Controls what fields are returned in Field message responses from mysql, i.e.
	// field name, table name, etc. This is an optimization for high-QPS queries where
	// the client knows what it's getting
	IncludedFields IncludedFields
	// client_rows_found specifies if rows_affected should return
	// rows found instead of rows affected. Behavior is defined
	// by MySQL's CLIENT_FOUND_ROWS flag.
	ClientFoundRows bool
	// workload specifies the type of workload:
	// OLTP: DMLs allowed, results have row count limit, and
	// query timeouts are shorter.
	// OLAP: DMLS not allowed, no limit on row count, timeouts
	// can be as high as desired.
	// DBA: no limit on rowcount or timeout, all queries allowed
	// but intended for long DMLs and DDLs.
	Workload Workload
	// sql_select_limit sets an implicit limit on all select statements. Since
	// vitess also sets a rowcount limit on queries, the smallest value wins.
	SqlSelectLimit       int64
	TransactionIsolation TransactionIsolation
	// skip_query_plan_cache specifies if the query plan should be cached by vitess.
	// By default all query plans are cached.
	SkipQueryPlanCache bool
	// PlannerVersion specifies which planner to use.
	// If DEFAULT is chosen, whatever vtgate was started with will be used
	PlannerVersion string
}
