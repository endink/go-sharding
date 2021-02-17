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
	"sync"
	"time"
)

type MultiDurationValueRecorder struct {
	name            string
	recorderFactory func(metric.MeterMust, string, ...metric.InstrumentOption) DurationValueRecorder
	options         []metric.InstrumentOption
	recorders       map[string]DurationValueRecorder
	recordersMutex  sync.Mutex
	meter           metric.MeterMust
}

func NewMultiDurationValueRecorder(meter metric.Meter, name string, mos ...metric.InstrumentOption) MultiDurationValueRecorder {
	return MultiDurationValueRecorder{
		recorderFactory: NewDurationValueRecorder,
		meter:           metric.Must(meter),
		name:            name,
		options:         append(mos, metric.WithUnit(unit.Milliseconds)),
		recorders:       make(map[string]DurationValueRecorder),
	}
}

func (d *MultiDurationValueRecorder) getOrPut(name string) DurationValueRecorder {
	v, ok := d.recorders[name]
	if !ok {
		d.recordersMutex.Lock()
		defer d.recordersMutex.Unlock()
		if v, ok = d.recorders[name]; !ok {
			v = d.recorderFactory(d.meter, BuildMetricName(name, d.name), d.options...)
			d.recorders[name] = v
		}
	}
	return v
}

func (d *MultiDurationValueRecorder) RecordMulti(ctx context.Context, names []string, duration time.Duration, labels ...label.KeyValue) {
	for _, name := range names {
		d.Record(ctx, name, duration, labels...)
	}
}

func (d *MultiDurationValueRecorder) RecordMultiLatency(ctx context.Context, names []string, startTime time.Time, labels ...label.KeyValue) {
	for _, name := range names {
		d.RecordLatency(ctx, name, startTime, labels...)
	}
}

func (d *MultiDurationValueRecorder) Record(ctx context.Context, name string, duration time.Duration, labels ...label.KeyValue) {
	r := d.getOrPut(BuildMetricName(name))
	r.Record(ctx, duration, labels...)
}

func (d *MultiDurationValueRecorder) RecordLatency(ctx context.Context, name string, startTime time.Time, labels ...label.KeyValue) {
	r := d.getOrPut(BuildMetricName(name))
	r.RecordLatency(ctx, startTime, labels...)
}
