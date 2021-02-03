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

package server

import (
	"github.com/XiaoMi/Gaea/core"
	"github.com/XiaoMi/Gaea/mysql"
	"github.com/XiaoMi/Gaea/mysql/types"
	"github.com/XiaoMi/Gaea/util/sync2"
	"github.com/google/uuid"
	"sync"
	"time"
)

const maxDuration time.Duration = 1<<63 - 1

var busyConnections = sync2.NewAtomicInt32(0)
var lockHeartbeatTime = time.Second * 5

type mysqlHandler struct {
	mutex        sync.Mutex
	connections  map[*mysql.Conn]struct{}
	queryTimeout time.Duration
}

func (m *mysqlHandler) ConnectionCount() int {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	return len(m.connections)
}

func (m *mysqlHandler) newSession(c *mysql.Conn) *Session {
	session, _ := c.ClientData.(*Session)
	if session == nil {
		u, _ := uuid.NewUUID()
		session = &Session{
			Autocommit:  true,
			SessionUUID: u.String(),
		}
		if c.Capabilities&mysql.CapabilityClientFoundRows != 0 {
			session.Options.ClientFoundRows = true
		}
		c.ClientData = session
	}
	return session
}

func (m *mysqlHandler) NewConnection(c *mysql.Conn) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.connections[c] = core.Nothing
}

func (m *mysqlHandler) ConnectionClosed(c *mysql.Conn) {
	defer func() {
		m.mutex.Lock()
		defer m.mutex.Unlock()
		delete(m.connections, c)
	}()

	//var ctx context.Context
	//var cancel context.CancelFunc
	//if m.queryTimeout != maxDuration {
	//	ctx, cancel = context.WithTimeout(context.Background(), m.queryTimeout)
	//	defer cancel()
	//} else {
	//	ctx = context.Background()
	//}
	session := m.newSession(c)
	if session.InTransaction {
		defer busyConnections.Add(-1)
	}
	//_ = vh.vtg.CloseSession(ctx, session)
}

func (m *mysqlHandler) ComQuery(c *mysql.Conn, query string, callback func(*types.Result) error) error {
	panic("implement me")
}

func (m *mysqlHandler) ComPrepare(c *mysql.Conn, query string, bindVars map[string]*types.BindVariable) ([]*types.Field, error) {
	panic("implement me")
}

func (m *mysqlHandler) ComStmtExecute(c *mysql.Conn, prepare *mysql.PrepareData, callback func(*types.Result) error) error {
	panic("implement me")
}

func (m *mysqlHandler) WarningCount(c *mysql.Conn) uint16 {
	panic("implement me")
}

func (m *mysqlHandler) ComResetConnection(c *mysql.Conn) {
	panic("implement me")
}
