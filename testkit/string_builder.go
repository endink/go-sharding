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

package testkit

import (
	"fmt"
	"strings"
)

type stringBuilder struct {
	buffer strings.Builder
}

func newStringBuilder(s ...string) *stringBuilder {
	sb := stringBuilder{}
	if s != nil && len(s) > 0 {
		_, _ = sb.buffer.WriteString(fmt.Sprint(s))
	}
	return &sb
}

func (w *stringBuilder) Clear() {
	w.buffer.Reset()
}

func (w *stringBuilder) WriteLine(value ...interface{}) {
	for _, v := range value {
		w.Write(v)
	}
	w.buffer.WriteString("\n")
}

func (w *stringBuilder) Write(value ...interface{}) {
	for _, v := range value {
		if a, isString := v.(string); isString {
			_, _ = w.buffer.WriteString(a)
			return
		}

		if b, isBuilder := v.(fmt.Stringer); isBuilder {
			_, _ = w.buffer.WriteString(b.String())
			return
		}
		_, _ = w.buffer.WriteString(fmt.Sprint(v))
	}
}

func (w *stringBuilder) WriteLineF(format string, args ...interface{}) {
	w.WriteFormat(format, args...)
	w.buffer.WriteString("\n")
}

func (w *stringBuilder) WriteFormat(format string, arg ...interface{}) {
	_, _ = w.buffer.WriteString(fmt.Sprintf(format, arg...))
}

func (w *stringBuilder) String() string {
	return w.buffer.String()
}
