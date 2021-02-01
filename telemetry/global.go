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
	"fmt"
	"go.opentelemetry.io/otel/exporters/stdout"
	"go.opentelemetry.io/otel/sdk/export/metric"
	"go.opentelemetry.io/otel/sdk/export/trace"
	controller "go.opentelemetry.io/otel/sdk/metric/controller/basic"
	processor "go.opentelemetry.io/otel/sdk/metric/processor/basic"
	"go.opentelemetry.io/otel/sdk/metric/selector/simple"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"time"
)

var metricExporter metric.Exporter
var traceExporter trace.SpanExporter

var telemetryContext context.Context

var pusher *controller.Controller
var tracer *sdktrace.TracerProvider

func SetDefaultExporter(exporter metric.Exporter) {
	metricExporter = exporter
}

func Start(ctx context.Context) error {
	//https://opentelemetry.io/docs/go/getting-started/
	if metricExporter == nil || traceExporter == nil {
		basicExporter, err := stdout.NewExporter(
			stdout.WithPrettyPrint(),
		)
		if err != nil {
			return fmt.Errorf("failed to initialize stdout export pipeline: %v", err)
		} else {
			if metricExporter == nil {
				metricExporter = basicExporter
			}
			if traceExporter == nil {
				traceExporter = basicExporter
			}
		}

	}

	//metric
	pusher = controller.New(
		processor.New(
			simple.NewWithExactDistribution(),
			metricExporter,
		),
		controller.WithPusher(metricExporter),
		controller.WithCollectPeriod(5*time.Second),
	)

	bsp := sdktrace.NewBatchSpanProcessor(traceExporter)
	tracer = sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(bsp))

	telemetryContext = ctx
	err := pusher.Start(ctx)
	if err != nil {
		return fmt.Errorf("failed to initialize metric controller: %v", err)
	}
	return nil
}

func Shutdown() {
	if pusher != nil {
		_ = pusher.Stop(telemetryContext)
		pusher = nil
	}
	if tracer != nil {
		_ = tracer.Shutdown(telemetryContext)
	}
	telemetryContext = nil
}
