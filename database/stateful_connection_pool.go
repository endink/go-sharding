/*
Copyright 2020 The Vitess Authors.

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

package database

import (
	"context"
	"github.com/XiaoMi/Gaea/mysql"
	"github.com/XiaoMi/Gaea/mysql/types"
	"github.com/XiaoMi/Gaea/util"
	"github.com/XiaoMi/Gaea/util/sync2"
	"go.opentelemetry.io/otel/label"
	"time"
)

const (
	scpClosed = int64(iota)
	scpOpen
	scpKillingNonTx
	scpKillingAll
)

// StatefulConnectionPool keeps track of currently and future active connections
// it's used whenever the session has some state that requires a dedicated connection
type StatefulConnectionPool struct {
	state sync2.AtomicInt64

	// conns is the 'regular' pool. By default, connections
	// are pulled from here for starting transactions.
	conns *Pool

	// foundRowsPool is the alternate pool that creates
	// connections with CLIENT_FOUND_ROWS flag set. A separate
	// pool is needed because this option can only be set at
	// connection time.
	foundRowsPool *Pool
	active        *util.Numbered
	lastID        sync2.AtomicInt64
}

//NewStatefulConnPool creates an ActivePool
func NewStatefulConnPool(cfg ConnPoolConfig) *StatefulConnectionPool {

	return &StatefulConnectionPool{
		conns:         NewPool("TransactionPool", cfg),
		foundRowsPool: NewPool("FoundRowsPool", cfg),
		active:        util.NewNumbered(),
		lastID:        sync2.NewAtomicInt64(time.Now().UnixNano()),
	}
}

// Open makes the TxPool operational. This also starts the transaction killer
// that will kill long-running transactions.
func (sf *StatefulConnectionPool) Open(param *mysql.ConnParams) {
	log.Infof("Starting transaction id: %d", sf.lastID)
	sf.conns.Open(param)
	sf.foundRowsPool.Open(param)
	sf.state.Set(scpOpen)
}

// Close closes the TxPool. A closed pool can be reopened.
func (sf *StatefulConnectionPool) Close() {
	for _, v := range sf.active.GetOutdated(time.Duration(0), "for closing") {
		conn := v.(*StatefulConnection)
		thing := "connection"
		if conn.IsInTransaction() {
			thing = "transaction"
		}
		log.Warnf("killing %s for shutdown: %s", thing, conn.String())
		DbStats.InternalErrors.Add(context.TODO(), 1, label.String("type", "StrayTransactions"))
		conn.Close()
		conn.Releasef("pool closed")
	}
	sf.conns.Close()
	sf.foundRowsPool.Close()
	sf.state.Set(scpClosed)
}

// ShutdownNonTx enters the state where all non-transactional connections are killed.
// InUse connections will be killed as they are returned.
func (sf *StatefulConnectionPool) ShutdownNonTx() {
	sf.state.Set(scpKillingNonTx)
	conns := mapToTxConn(sf.active.GetByFilter("kill non-tx", func(sc interface{}) bool {
		return !sc.(*StatefulConnection).IsInTransaction()
	}))
	for _, sc := range conns {
		sc.Releasef("kill non-tx")
	}
}

// ShutdownAll enters the state where all connections are to be killed.
// It returns all connections that are not in use. They must be rolled back
// by the caller (TxPool). InUse connections will be killed as they are returned.
func (sf *StatefulConnectionPool) ShutdownAll() []*StatefulConnection {
	sf.state.Set(scpKillingAll)
	return mapToTxConn(sf.active.GetByFilter("kill non-tx", func(sc interface{}) bool {
		return true
	}))
}

// AdjustLastID adjusts the last transaction id to be at least
// as large as the input value. This will ensure that there are
// no dtid collisions with future transactions.
func (sf *StatefulConnectionPool) AdjustLastID(id int64) {
	if current := sf.lastID.Get(); current < id {
		log.Infof("Adjusting transaction id to: %d", id)
		sf.lastID.Set(id)
	}
}

// GetOutdated returns a list of connections that are older than age.
// It does not return any connections that are in use.
// TODO(sougou): deprecate.
func (sf *StatefulConnectionPool) GetOutdated(age time.Duration, purpose string) []*StatefulConnection {
	return mapToTxConn(sf.active.GetOutdated(age, purpose))
}

func mapToTxConn(outdated []interface{}) []*StatefulConnection {
	result := make([]*StatefulConnection, len(outdated))
	for i, el := range outdated {
		result[i] = el.(*StatefulConnection)
	}
	return result
}

// WaitForEmpty returns as soon as the pool becomes empty
func (sf *StatefulConnectionPool) WaitForEmpty() {
	sf.active.WaitForEmpty()
}

// GetAndLock locks the connection for use. It accepts a purpose as a string.
// If it cannot be found, it returns a "not found" error. If in use,
// it returns a "in use: purpose" error.
func (sf *StatefulConnectionPool) GetAndLock(id int64, reason string) (*StatefulConnection, error) {
	conn, err := sf.active.Get(id, reason)
	if err != nil {
		return nil, err
	}
	return conn.(*StatefulConnection), nil
}

// NewConn creates a new StatefulConnection. It will be created from either the normal pool or
// the found_rows pool, depending on the options provided
func (sf *StatefulConnectionPool) NewConn(ctx context.Context, options *types.ExecuteOptions) (*StatefulConnection, error) {

	var conn *DBConn
	var err error

	if options.ClientFoundRows {
		conn, err = sf.foundRowsPool.Get(ctx)
	} else {
		conn, err = sf.conns.Get(ctx)
	}
	if err != nil {
		return nil, err
	}

	connID := sf.lastID.Add(1)
	sfConn := &StatefulConnection{
		dbConn:         conn,
		ConnID:         connID,
		pool:           sf,
		enforceTimeout: true,
	}

	err = sf.active.Register(
		sfConn.ConnID,
		sfConn,
		sfConn.enforceTimeout,
	)
	if err != nil {
		sfConn.Release(ConnInitFail)
		return nil, err
	}

	return sf.GetAndLock(sfConn.ConnID, "new connection")
}

// ForAllTxProperties executes a function an every connection that has a not-nil TxProperties
func (sf *StatefulConnectionPool) ForAllTxProperties(f func(*TxProperties)) {
	for _, connection := range mapToTxConn(sf.active.GetAll()) {
		props := connection.txProps
		if props != nil {
			f(props)
		}
	}
}

// Unregister forgets the specified connection.  If the connection is not present, it's ignored.
func (sf *StatefulConnectionPool) unregister(id int64, reason string) {
	sf.active.Unregister(id, reason)
}

// markAsNotInUse marks the connection as not in use at the moment
func (sf *StatefulConnectionPool) markAsNotInUse(sc *StatefulConnection, updateTime bool) {
	switch sf.state.Get() {
	case scpKillingNonTx:
		if !sc.IsInTransaction() {
			sc.Releasef("kill non-tx")
			return
		}
	case scpKillingAll:
		if sc.IsInTransaction() {
			sc.Close()
		}
		sc.Releasef("kill all")
		return
	}
	sf.active.Put(sc.ConnID, updateTime)
}

// Capacity returns the pool capacity.
func (sf *StatefulConnectionPool) Capacity() int {
	return int(sf.conns.Capacity())
}

// renewConn unregister and registers with new id.
func (sf *StatefulConnectionPool) renewConn(sc *StatefulConnection) error {
	sf.active.Unregister(sc.ConnID, "renew existing connection")
	sc.ConnID = sf.lastID.Add(1)
	return sf.active.Register(sc.ConnID, sc, sc.enforceTimeout)
}
