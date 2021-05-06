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
	"context"
	"github.com/endink/go-sharding/core"
	"github.com/endink/go-sharding/logging"
	"github.com/endink/go-sharding/mysql"
	"github.com/endink/go-sharding/mysql/types"
	"github.com/endink/go-sharding/util/sync2"
	"github.com/google/uuid"
	"sync"
	"time"
)

const maxDuration time.Duration = 1<<63 - 1

var lockHeartbeatTime = time.Second * 5

type mysqlHandler struct {
	mutex           sync.Mutex
	connections     map[*mysql.Conn]struct{}
	queryTimeout    time.Duration
	busyConnections sync2.AtomicInt32
	TxConn          TxConn
}

func (m *mysqlHandler) ConnectionCount() int {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	return len(m.connections)
}

func (m *mysqlHandler) session(c *mysql.Conn) *Session {
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

// CloseSession closes the session, rolling back any implicit transactions. This has the
// same effect as if a "rollback" statement was executed, but does not affect the query
// statistics.
func (m *mysqlHandler) closeSession(ctx context.Context, session *Session) error {
	return m.TxConn.ReleaseAll(ctx, NewSafeSession(session))
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

	var ctx context.Context
	var cancel context.CancelFunc
	if m.queryTimeout != maxDuration {
		ctx, cancel = context.WithTimeout(context.Background(), m.queryTimeout)
		defer cancel()
	} else {
		ctx = context.Background()
	}
	session := m.session(c)
	if session.InTransaction {
		defer m.busyConnections.Add(-1)
	}
	m.closeSession(ctx, session)
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
	return uint16(len(m.session(c).Warnings))
}

func (m *mysqlHandler) ComResetConnection(c *mysql.Conn) {
	ctx := context.Background()
	session := m.session(c)
	if session.InTransaction {
		defer m.busyConnections.Add(-1)
	}
	err := m.closeSession(ctx, session)
	if err != nil {
		logging.DefaultLogger.Errorf("Error happened in transaction rollback: %v", err)
	}
}
