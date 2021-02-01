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
	"errors"
	"go.opentelemetry.io/otel"
	"strings"
	"sync"
	"unicode"
)

var meterMap = make(map[string]*NamedMeter)
var meterMutex sync.Mutex

func GetMeter(instrumentationName string) *NamedMeter {
	nm, ok := meterMap[instrumentationName]
	if !ok {
		meterMutex.Lock()
		defer meterMutex.Unlock()
		if nm, ok = meterMap[instrumentationName]; !ok {
			m := otel.Meter(instrumentationName)
			nm = &NamedMeter{
				meter:     m,
				recorders: make(map[string]interface{}),
			}
			meterMap[instrumentationName] = nm
		}
	}
	return nm
}

func BuildMetricName(statement ...string) string {
	if len(statement) == 0 {
		panic(errors.New("name for 'BuildMetricName' can not be nil or empty"))
	}
	const spliter rune = '_'

	nChar := func(c rune) rune {
		switch c {
		case '.', '-', ' ':
			return spliter
		default:
			return c
		}
	}

	sb := &strings.Builder{}
	array := make([]string, 0, len(statement))
	for _, s := range statement {
		var letters = []rune(s)
		var prev rune
		var prevUpper = true
		var hasChar = false
		for i, current := range letters {
			char := nChar(current)
			if char == spliter {
				prev = spliter
				continue
			}
			if hasChar && current != spliter && i != len(letters)-1 && prev == spliter {
				sb.WriteRune(spliter)
			}

			if current != spliter {
				hasChar = true
			}

			u := 'A' <= char && char <= 'Z'
			if u {
				if !prevUpper {
					sb.WriteRune(spliter)
				}
				sb.WriteRune(unicode.ToLower(char))
			} else {
				sb.WriteRune(char)
			}
			prevUpper = u
			prev = char
		}
		array = append(array, sb.String())
		sb.Reset()
	}
	return strings.Join(array, "_")
}
