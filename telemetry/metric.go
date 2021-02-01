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
	"bytes"
	"errors"
	"go.opentelemetry.io/otel"
	"strings"
	"sync"
)

var meterMap = make(map[string]*NamedMeter)
var meterMutex sync.Mutex

func GetMeter(instrumentationName string) *NamedMeter {
	if m, ok := meterMap[instrumentationName]; !ok {
		meterMutex.Lock()
		meterMutex.Unlock()
		if m, ok = meterMap[instrumentationName]; ok {
			return m
		} else {
			meter := otel.Meter(instrumentationName)
			nm := &NamedMeter{
				meter:     meter,
				recorders: make(map[string]interface{}),
			}
			meterMap[instrumentationName] = nm
			return nm
		}
	} else {
		return m
	}
}

func BuildMetricName(statement ...string) string {
	if len(statement) == 0 {
		panic(errors.New("name for 'BuildMetricName' can not be nil or empty"))
	}

	sb := &strings.Builder{}
	array := make([]string, 0, len(statement))
	for _, s := range statement {
		sb.Reset()
		var letters = []byte(s)
		var prev byte
		var prevUpper = true
		for i, current := range letters {
			if (prev == '.' && current == '.') || i == 0 || i == len(letters)-1 {
				continue
			}
			content := []byte{current}
			u := 'A' <= current && current <= 'Z'
			if u {
				if !prevUpper {
					sb.WriteByte('_')
				}
				sb.WriteByte(bytes.ToLower(content)[0])
				prevUpper = true
			} else {
				sb.WriteByte(current)
			}
			prev = current
		}
	}
	return strings.Join(array, ".")
}
