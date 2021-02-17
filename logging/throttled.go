/*
Copyright 2019 The Vitess Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package logging

import (
	"fmt"
	"sync"
	"time"
)

// ThrottledLogger will allow logging of messages but won't spam the
// logs.
type ThrottledLogger struct {
	// set at construction
	name        string
	maxInterval time.Duration

	// mu protects the following members
	mu           sync.Mutex
	lastlogTime  time.Time
	skippedCount int
	logger       StandardLogger
}

// NewThrottledLogger will create a ThrottledLogger with the given
// name and throttling interval.
func NewThrottledLogger(name string, logger StandardLogger, maxInterval time.Duration) *ThrottledLogger {
	var log = logger
	if logger == nil {
		log = GetLogger("throttled")
	}
	return &ThrottledLogger{
		name:        name,
		maxInterval: maxInterval,
		logger:      log,
	}
}

type logFunc func(args ...interface{})

func (tl *ThrottledLogger) log(logFunc logFunc, format string, v ...interface{}) {
	now := time.Now()

	tl.mu.Lock()
	defer tl.mu.Unlock()
	logWaitTime := tl.maxInterval - (now.Sub(tl.lastlogTime))
	if logWaitTime < 0 {
		tl.lastlogTime = now
		logFunc(fmt.Sprintf(tl.name+": "+format, v...))
		return
	}
	// If this is the first message to be skipped, start a goroutine
	// to log and reset skippedCount
	if tl.skippedCount == 0 {
		go func(d time.Duration) {
			time.Sleep(d)
			tl.mu.Lock()
			defer tl.mu.Unlock()
			// Because of the go func(), we lose the stack trace,
			// so we just use the current line for this.
			logFunc(fmt.Sprintf("%v: skipped %v log messages", tl.name, tl.skippedCount))
			tl.skippedCount = 0
		}(logWaitTime)
	}
	tl.skippedCount++
}

// Infof logs an info if not throttled.
func (tl *ThrottledLogger) Infof(format string, v ...interface{}) {
	tl.log(tl.logger.Info, format, v...)
}

// Warningf logs a warning if not throttled.
func (tl *ThrottledLogger) Warningf(format string, v ...interface{}) {
	tl.log(tl.logger.Warn, format, v...)
}

// Errorf logs an error if not throttled.
func (tl *ThrottledLogger) Errorf(format string, v ...interface{}) {
	tl.log(tl.logger.Error, format, v...)
}
