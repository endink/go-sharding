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
	"github.com/XiaoMi/Gaea/mysql/types"
	"net"
	"runtime"
	"strings"
	"testing"
)

func isUnix() bool {
	sysType := runtime.GOOS

	return strings.ToLower(sysType) != "windows"

}

var testUserProvider = NewStaticUserProvider("user1", "password1")
var testCounter = NewTestTelemetry()

var selectRowsResult = &types.Result{
	Fields: []*types.Field{
		{
			Name: "id",
			Type: types.Int32,
		},
		{
			Name: "name",
			Type: types.VarChar,
		},
	},
	Rows: [][]types.Value{
		{
			types.MakeTrusted(types.Int32, []byte("10")),
			types.MakeTrusted(types.VarChar, []byte("nice name")),
		},
		{
			types.MakeTrusted(types.Int32, []byte("20")),
			types.MakeTrusted(types.VarChar, []byte("nicer name")),
		},
	},
	RowsAffected: 2,
}

func newTestListenerWithCounter(telemetry ConnTelemetry) (*Listener, error) {
	conf := ListenerConfig{
		Protocol:         "tcp4",
		Address:          ":0",
		Handler:          &testHandler{},
		ConnReadTimeout:  0,
		ConnWriteTimeout: 0,
		Telemetry:        telemetry,
	}

	l, err := NewListenerWithConfig(conf, NewStaticUserProvider("user1", "password1"))
	return l, err
}

func newTestListenerWithHandler(handler Handler) (*Listener, error) {
	conf := ListenerConfig{
		Protocol:         "tcp4",
		Address:          ":0",
		Handler:          handler,
		ConnReadTimeout:  0,
		ConnWriteTimeout: 0,
		Telemetry:        testCounter,
	}

	l, err := NewListenerWithConfig(conf, NewStaticUserProvider("user1", "password1"))
	return l, err
}

func newTestListenerDefault() (*Listener, error) {
	conf := ListenerConfig{
		Protocol:         "tcp4",
		Address:          ":0",
		Handler:          &testHandler{},
		ConnReadTimeout:  0,
		ConnWriteTimeout: 0,
		Telemetry:        testCounter,
	}

	l, err := NewListenerWithConfig(conf, NewStaticUserProvider("user1", "password1"))
	return l, err
}

func newDefaultConnParam(t testing.TB, l *Listener) *ConnParams {
	_, port := getHostPort(t, l.Addr())

	// Setup the right parameters.
	params := &ConnParams{
		Host:  "127.0.0.1",
		Port:  port,
		Uname: "user1",
		Pass:  "password1",
	}
	return params
}

func newDefaultConnParamWithDb(t testing.TB, l *Listener, db string) *ConnParams {
	_, port := getHostPort(t, l.Addr())

	// Setup the right parameters.
	params := &ConnParams{
		Host:   "127.0.0.1",
		Port:   port,
		Uname:  "user1",
		Pass:   "password1",
		DbName: db,
	}
	return params
}

func newTestListener(user string, passwd string) (*Listener, error) {
	conf := ListenerConfig{
		Protocol:         "tcp4",
		Address:          ":0",
		Handler:          &testHandler{},
		ConnReadTimeout:  0,
		ConnWriteTimeout: 0,
		Telemetry:        testCounter,
	}

	l, err := NewListenerWithConfig(conf, NewStaticUserProvider(user, passwd))
	return l, err
}

func getHostPort(t testing.TB, a net.Addr) (string, int) {
	// For the host name, we resolve 'localhost' into an address.
	// This works around a few travis issues where IPv6 is not 100% enabled.
	hosts, err := net.LookupHost("localhost")
	if err != nil {
		t.Fatalf("LookupHost(localhost) failed: %v", err)
	}
	host := hosts[0]
	port := a.(*net.TCPAddr).Port
	t.Logf("listening on address '%v' port %v", host, port)
	return host, port
}
