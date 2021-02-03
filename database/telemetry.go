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

package database

import (
	"github.com/XiaoMi/Gaea/telemetry"
	"go.opentelemetry.io/otel/metric"
)

type Stats struct {
	DbConnectLatency      telemetry.DurationValueRecorder
	DbConnectErrLatency   telemetry.DurationValueRecorder
	DbExecLatency         telemetry.DurationValueRecorder
	DbExecStreamLatency   telemetry.DurationValueRecorder
	KillQueriesCounter    metric.Int64Counter
	KillCounter           metric.Int64Counter
	InternalErrorCounter  metric.Int64Counter
	ResourceWaitTime      *telemetry.MultiDurationValueRecorder
	InternalErrors        metric.Int64Counter
	ActiveReservedCounter metric.Int64Counter
	ReservedCounter       metric.Int64Counter
	ReservedTimes         telemetry.DurationCounter

	TransactionCounter metric.Int64Counter
	TransactionTimes   telemetry.DurationCounter
}

var DbMeter = telemetry.GetMeter("database")
var DbStats = &Stats{
	DbConnectLatency:     DbMeter.NewDurationValueRecorder("connect_latency", "Database connect succeed time"),
	DbConnectErrLatency:  DbMeter.NewDurationValueRecorder("connect_error_latency", "Database connect error time"),
	DbExecLatency:        DbMeter.NewDurationValueRecorder("exec_latency", "Database execitopm time"),
	DbExecStreamLatency:  DbMeter.NewDurationValueRecorder("exec_stream_latency", "Database execitopm time"),
	KillQueriesCounter:   DbMeter.NewInt64Counter("kill_queries_count", "Database killed queries count"),
	KillCounter:          DbMeter.NewInt64Counter("kill", "Number of connections being killed"),
	InternalErrorCounter: DbMeter.NewInt64Counter("internal_error_count", "Database error count"),
	ResourceWaitTime:     DbMeter.NewMultiDurationValueRecorder("resource_wait_time", "Resource wait time"),
	InternalErrors:       DbMeter.NewInt64Counter("internal_errors", "Internal component errors"),

	ActiveReservedCounter: DbMeter.NewInt64Counter("active_reserved_count", "active reserved connection for each host"),
	ReservedCounter:       DbMeter.NewInt64Counter("reserved_count", "reserved connection received for each host"),
	ReservedTimes:         DbMeter.NewDurationCounter("reserved_times_ms", "Total reserved connection latency for each host"),

	TransactionCounter: DbMeter.NewInt64Counter("UserTransactionCount", "transactions received for each CallerID"),
	TransactionTimes:   DbMeter.NewDurationCounter("user_transaction_times_ms", "Total transaction latency for each host"),
}
