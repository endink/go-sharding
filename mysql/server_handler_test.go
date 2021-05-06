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
	"github.com/endink/go-sharding/mysql/types"
	"strings"
	"sync"
	"time"
)

type testHandler struct {
	mu       sync.Mutex
	lastConn *Conn
	result   *types.Result
	err      error
	warnings uint16
}

func (th *testHandler) LastConn() *Conn {
	th.mu.Lock()
	defer th.mu.Unlock()
	return th.lastConn
}

func (th *testHandler) Result() *types.Result {
	th.mu.Lock()
	defer th.mu.Unlock()
	return th.result
}

func (th *testHandler) SetErr(err error) {
	th.mu.Lock()
	defer th.mu.Unlock()
	th.err = err
}

func (th *testHandler) Err() error {
	th.mu.Lock()
	defer th.mu.Unlock()
	return th.err
}

func (th *testHandler) SetWarnings(count uint16) {
	th.mu.Lock()
	defer th.mu.Unlock()
	th.warnings = count
}

func (th *testHandler) NewConnection(c *Conn) {
	th.mu.Lock()
	defer th.mu.Unlock()
	th.lastConn = c
}

func (th *testHandler) ConnectionClosed(c *Conn) {
}

func (th *testHandler) ComQuery(c *Conn, query string, callback func(*types.Result) error) error {
	if result := th.Result(); result != nil {
		callback(result)
		return nil
	}

	switch query {
	case "error":
		return th.Err()
	case "panic":
		panic("test panic attack!")
	case "select rows":
		callback(selectRowsResult)
	case "error after send":
		callback(selectRowsResult)
		return th.Err()
	case "insert":
		callback(&types.Result{
			RowsAffected: 123,
			InsertID:     123456789,
		})
	case "schema echo":
		callback(&types.Result{
			Fields: []*types.Field{
				{
					Name: "schema_name",
					Type: types.VarChar,
				},
			},
			Rows: [][]types.Value{
				{
					types.MakeTrusted(types.VarChar, []byte(c.schemaName)),
				},
			},
		})
	case "ssl echo":
		value := "OFF"
		if c.Capabilities&CapabilityClientSSL > 0 {
			value = "ON"
		}
		callback(&types.Result{
			Fields: []*types.Field{
				{
					Name: "ssl_flag",
					Type: types.VarChar,
				},
			},
			Rows: [][]types.Value{
				{
					types.MakeTrusted(types.VarChar, []byte(value)),
				},
			},
		})
	case "userData echo":
		callback(&types.Result{
			Fields: []*types.Field{
				{
					Name: "user",
					Type: types.VarChar,
				},
				{
					Name: "user_data",
					Type: types.VarChar,
				},
			},
			Rows: [][]types.Value{
				{
					types.MakeTrusted(types.VarChar, []byte(c.User)),
				},
			},
		})
	case "50ms delay":
		callback(&types.Result{
			Fields: []*types.Field{{
				Name: "result",
				Type: types.VarChar,
			}},
		})
		time.Sleep(50 * time.Millisecond)
		callback(&types.Result{
			Rows: [][]types.Value{{
				types.MakeTrusted(types.VarChar, []byte("delayed")),
			}},
		})
	default:
		if strings.HasPrefix(query, benchmarkQueryPrefix) {
			callback(&types.Result{
				Fields: []*types.Field{
					{
						Name: "result",
						Type: types.VarChar,
					},
				},
				Rows: [][]types.Value{
					{
						types.MakeTrusted(types.VarChar, []byte(query)),
					},
				},
			})
		}

		callback(&types.Result{})
	}
	return nil
}

func (th *testHandler) ComPrepare(c *Conn, query string, bindVars map[string]*types.BindVariable) ([]*types.Field, error) {
	return nil, nil
}

func (th *testHandler) ComStmtExecute(c *Conn, prepare *PrepareData, callback func(*types.Result) error) error {
	return nil
}

func (th *testHandler) ComResetConnection(c *Conn) {

}

func (th *testHandler) WarningCount(c *Conn) uint16 {
	th.mu.Lock()
	defer th.mu.Unlock()
	return th.warnings
}
