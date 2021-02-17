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

package logging

import "fmt"

type testLogger struct {
	ch chan string
}

func NewLoggerForTest(ch chan string) StandardLogger {
	return testLogger{
		ch: ch,
	}
}

func (t testLogger) Debug(args ...interface{}) {
	t.ch <- "[DEBUG]" + fmt.Sprint(args...)
}

func (t testLogger) Info(args ...interface{}) {
	t.ch <- "[INFO]" + fmt.Sprint(args...)
}

func (t testLogger) Warn(args ...interface{}) {
	t.ch <- "[WARN]" + fmt.Sprint(args...)
}

func (t testLogger) Error(args ...interface{}) {
	t.ch <- "[ERROR]" + fmt.Sprint(args...)
}

func (t testLogger) Panic(args ...interface{}) {
	t.ch <- "[PANIC]" + fmt.Sprint(args...)
}

func (t testLogger) Fatal(args ...interface{}) {
	t.ch <- "[FATAL]" + fmt.Sprint(args...)
}

func (t testLogger) Debugf(template string, args ...interface{}) {
	t.ch <- "[DEBUG]" + fmt.Sprintf(template, args...)
}

func (t testLogger) Infof(template string, args ...interface{}) {
	t.ch <- "[INFO]" + fmt.Sprintf(template, args...)
}

func (t testLogger) Warnf(template string, args ...interface{}) {
	t.ch <- "[WARN]" + fmt.Sprintf(template, args...)
}

func (t testLogger) Errorf(template string, args ...interface{}) {
	t.ch <- "[ERROR]" + fmt.Sprintf(template, args...)
}

func (t testLogger) Panicf(template string, args ...interface{}) {
	t.ch <- "[PANIC]" + fmt.Sprintf(template, args...)
}

func (t testLogger) Fatalf(template string, args ...interface{}) {
	t.ch <- "[FATAL]" + fmt.Sprintf(template, args...)
}
