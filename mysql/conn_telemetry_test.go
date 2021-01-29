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

package mysql

import (
	"github.com/XiaoMi/Gaea/util/sync2"
	"time"
)

var _ ConnTelemetry = &TestTelemetry{}

type TestTelemetry struct {
	ConnCountByTLSVer map[string]int64
	ConnCountPerUser  map[string]int64
	AcceptCount       sync2.AtomicInt64
	ConnCount         sync2.AtomicInt64
	ConnSlowCount     sync2.AtomicInt64
	RefuseCount       sync2.AtomicInt64
}

func NewTestTelemetry() *TestTelemetry {
	return &TestTelemetry{
		ConnCountByTLSVer: map[string]int64{},
		ConnCountPerUser:  map[string]int64{},
	}
}

func (t *TestTelemetry) AddRefuseCount(count int) {
	t.RefuseCount.Add(int64(count))
}

func (t *TestTelemetry) AddConnCountByTLSVer(tlsVersion string, count int) {
	t.ConnCountByTLSVer[tlsVersion] += int64(count)
}

func (t *TestTelemetry) AddAcceptCount(count int) {
	t.AcceptCount.Add(int64(count))
}

func (t *TestTelemetry) AddConnCount(count int) {
	t.ConnCount.Add(int64(count))
}

func (t *TestTelemetry) AddConnCountPerUser(user string, count int) {
	t.ConnCountPerUser[user] += int64(count)
}

func (t *TestTelemetry) AddConnSlow(count int) {
	t.ConnSlowCount.Add(int64(count))
}

func (t *TestTelemetry) RecordQueryTime(time time.Time) {

}

func (t *TestTelemetry) RecordConnectTime(time time.Time) {

}
