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
	DbConnectLatency     telemetry.DurationValueRecorder
	DbConnectErrLatency  telemetry.DurationValueRecorder
	DbExecLatency        telemetry.DurationValueRecorder
	DbExecStreamLatency  telemetry.DurationValueRecorder
	KillQueriesCounter   metric.Int64Counter
	InternalErrorCounter metric.Int64Counter
	ResourceWaitTime     *telemetry.MultiDurationValueRecorder
}

var DbTracer = telemetry.GetTracer("DbConn")
var DbStats = newStats("DbConn")

func newStats(instrumentationName string) *Stats {
	meter := telemetry.GetMeter(instrumentationName)

	s := &Stats{
		DbConnectLatency:     meter.NewDurationValueRecorder("connect_latency", "Database connect succeed time"),
		DbConnectErrLatency:  meter.NewDurationValueRecorder("connect_error_latency", "Database connect error time"),
		DbExecLatency:        meter.NewDurationValueRecorder("exec_latency", "Database execitopm time"),
		DbExecStreamLatency:  meter.NewDurationValueRecorder("exec_stream_latency", "Database execitopm time"),
		KillQueriesCounter:   meter.NewInt64Counter("kill_queries_count", "Database killed queries count"),
		InternalErrorCounter: meter.NewInt64Counter("internal_error_count", "Database error count"),
		ResourceWaitTime:     meter.NewMultiDurationValueRecorder("resource_wait_time", "Resource wait time"),
	}

	return s
}
