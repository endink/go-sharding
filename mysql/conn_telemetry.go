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

import "time"

type ConnTelemetry interface {
	AddConnCountByTLSVer(tlsVersion string, count int)
	AddAcceptCount(count int)
	AddConnCount(count int)
	AddConnCountPerUser(user string, count int)
	AddConnSlow(count int)
	RecordQueryTime(time time.Time)
	RecordConnectTime(time time.Time)
	AddRefuseCount(count int)
}

var NoneConnTelemetry ConnTelemetry = noneConnTelemetry{}

type noneConnTelemetry struct {
}

func (n noneConnTelemetry) AddRefuseCount(count int) {

}

func (n noneConnTelemetry) AddConnCountByTLSVer(tlsVersion string, count int) {

}

func (n noneConnTelemetry) AddAcceptCount(count int) {
}

func (n noneConnTelemetry) AddConnCount(count int) {
}

func (n noneConnTelemetry) AddConnCountPerUser(user string, count int) {
}

func (n noneConnTelemetry) AddConnSlow(count int) {
}

func (n noneConnTelemetry) RecordQueryTime(time time.Time) {
}

func (n noneConnTelemetry) RecordConnectTime(time time.Time) {
}
