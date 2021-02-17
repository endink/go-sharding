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
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/unit"
	"sync"
	"time"
)

//https://github.com/open-telemetry/opentelemetry-specification/blob/main/specification/metrics/api.md#interpretation

type NamedMeter struct {
	meter         metric.Meter
	recorderMutex sync.Mutex
	recorders     map[string]interface{}
}

func (m *NamedMeter) getOrPutRecorder(name string, factory func() interface{}) interface{} {
	r, ok := m.recorders[name]
	if !ok {
		m.recorderMutex.Lock()
		defer m.recorderMutex.Unlock()
		if r, ok = m.recorders[name]; !ok {
			r = factory()
			m.recorders[name] = r
		}
	}
	return r
}

func (m *NamedMeter) NewInt64ValueObserver(name, desc string, callback func() int64) {

	observerCallback := func(_ context.Context, result metric.Int64ObserverResult) {
		value := callback()
		result.Observe(value)
	}
	_ = metric.Must(m.meter).NewInt64ValueObserver(name, observerCallback, metric.WithDescription(desc))
}

func (m *NamedMeter) NewInt64SumObserver(name, desc string, callback func() int64) {
	observerCallback := func(_ context.Context, result metric.Int64ObserverResult) {
		value := callback()
		result.Observe(value)
	}
	_ = metric.Must(m.meter).NewInt64UpDownSumObserver(name, observerCallback, metric.WithDescription(desc))
}

func (m *NamedMeter) NewDurationSumObserver(name, desc string, callback func() time.Duration) {
	observerCallback := func(_ context.Context, result metric.Int64ObserverResult) {
		value := callback()
		result.Observe(value.Milliseconds())
	}
	_ = metric.Must(m.meter).NewInt64UpDownSumObserver(name, observerCallback, metric.WithDescription(desc), metric.WithUnit(unit.Milliseconds))
}

func (m *NamedMeter) NewDurationObserver(name, desc string, callback func() time.Duration) {
	observerCallback := func(_ context.Context, result metric.Int64ObserverResult) {
		value := callback()
		result.Observe(value.Milliseconds())
	}

	_ = metric.Must(m.meter).NewInt64ValueObserver(name, observerCallback, metric.WithDescription(desc), metric.WithUnit(unit.Milliseconds))
}

func (m *NamedMeter) NewInt64Counter(name, desc string) metric.Int64Counter {
	fac := func() interface{} {
		return metric.Must(m.meter).NewInt64Counter(name, metric.WithDescription(desc))
	}
	r := m.getOrPutRecorder(name, fac)
	return r.(metric.Int64Counter)
}

func (m *NamedMeter) NewInt64ValueRecorder(name, desc string) metric.Int64ValueRecorder {
	fac := func() interface{} {
		return metric.Must(m.meter).NewInt64ValueRecorder(name, metric.WithDescription(desc))
	}
	r := m.getOrPutRecorder(name, fac)
	return r.(metric.Int64ValueRecorder)
}

func (m *NamedMeter) NewDurationValueRecorder(name, desc string) DurationValueRecorder {
	fac := func() interface{} {
		return NewDurationValueRecorder(metric.Must(m.meter), name, metric.WithDescription(desc))
	}
	r := m.getOrPutRecorder(name, fac)
	return r.(DurationValueRecorder)
}

func (m *NamedMeter) NewDurationCounter(name, desc string) DurationCounter {
	fac := func() interface{} {
		return NewDurationCounter(metric.Must(m.meter), name, metric.WithDescription(desc))
	}
	r := m.getOrPutRecorder(name, fac)
	return r.(DurationCounter)
}

func (m *NamedMeter) NewMultiDurationValueRecorder(name, desc string) *MultiDurationValueRecorder {
	fac := func() interface{} {
		r := NewMultiDurationValueRecorder(m.meter, name, metric.WithDescription(desc))
		return &r
	}
	r := m.getOrPutRecorder(name, fac)
	return r.(*MultiDurationValueRecorder)
}
