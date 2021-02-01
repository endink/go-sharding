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

package telemetry

import (
	"context"
	"go.opentelemetry.io/otel/label"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/unit"
	"time"
)

type DurationValueRecorder struct {
	valueRecorder metric.Int64ValueRecorder
}

func NewDurationValueRecorder(meter metric.MeterMust, name string, mos ...metric.InstrumentOption) DurationValueRecorder {
	options := append(mos, metric.WithUnit(unit.Milliseconds))
	return DurationValueRecorder{
		valueRecorder: meter.NewInt64ValueRecorder(name, options...),
	}
}

func (d DurationValueRecorder) Record(ctx context.Context, duration time.Duration, labels ...label.KeyValue) {
	d.valueRecorder.Record(ctx, duration.Milliseconds(), labels...)
}

func (d DurationValueRecorder) RecordLatency(ctx context.Context, startTime time.Time, labels ...label.KeyValue) {
	d.valueRecorder.Record(ctx, time.Since(startTime).Milliseconds(), labels...)
}
