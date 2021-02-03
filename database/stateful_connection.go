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

package database

import (
	"context"
	"errors"
	"fmt"
	"github.com/XiaoMi/Gaea/mysql"
	"github.com/XiaoMi/Gaea/mysql/types"
	"github.com/XiaoMi/Gaea/util"
	"go.opentelemetry.io/otel/label"
	"time"
)

// StatefulConnection is used in the situations where we need a dedicated connection for a vtgate session.
// This is used for transactions and reserved connections.
// NOTE: After use, if must be returned either by doing a Unlock() or a Release().
type StatefulConnection struct {
	pool           *StatefulConnectionPool
	dbConn         *DBConn
	txProps        *TxProperties
	reservedProps  *ReservedProperties
	tainted        bool
	enforceTimeout bool
	ConnID         int64
}

// ReservedProperties contains meta information about the connection
type ReservedProperties struct {
	userName  string
	host      string
	StartTime time.Time
}

// Close closes the underlying connection. When the connection is Unblocked, it will be Released
func (sc *StatefulConnection) Close() {
	if sc.dbConn != nil {
		sc.dbConn.Close()
	}
}

// IsClosed returns true when the connection is still operational
func (sc *StatefulConnection) IsClosed() bool {
	return sc.dbConn == nil || sc.dbConn.IsClosed()
}

// IsInTransaction returns true when the connection has tx state
func (sc *StatefulConnection) IsInTransaction() bool {
	return sc.txProps != nil
}

// Exec executes the statement in the dedicated connection
func (sc *StatefulConnection) Exec(ctx context.Context, query string, maxrows int, wantfields bool) (*types.Result, error) {
	if sc.IsClosed() {
		if sc.IsInTransaction() {
			return nil, fmt.Errorf("transaction was aborted: %v", sc.txProps.Conclusion)
		}
		return nil, errors.New("connection was aborted")
	}
	r, err := sc.dbConn.ExecOnce(ctx, query, maxrows, wantfields)
	if err != nil {
		if mysql.IsConnErr(err) {
			select {
			case <-ctx.Done():
				// If the context is done, the query was killed.
				// So, don't trigger a mysql check.
			default:
				//sc.env.CheckMySQL()
			}
			return nil, err
		}
		return nil, err
	}
	return r, nil
}

func (sc *StatefulConnection) execWithRetry(ctx context.Context, query string, maxrows int, wantfields bool) error {
	if sc.IsClosed() {
		return errors.New("connection is closed")
	}
	if _, err := sc.dbConn.Exec(ctx, query, maxrows, wantfields); err != nil {
		return err
	}
	return nil
}

// FetchNext returns the next result set.
func (sc *StatefulConnection) FetchNext(ctx context.Context, maxrows int, wantfields bool) (*types.Result, error) {
	if sc.IsClosed() {
		return nil, errors.New("connection is closed")
	}
	return sc.dbConn.FetchNext(ctx, maxrows, wantfields)
}

// Unlock returns the connection to the pool. The connection remains active.
// This method is idempotent and can be called multiple times
func (sc *StatefulConnection) Unlock() {
	// when in a transaction, we count from the time created, so each use of the connection does not update the time
	updateTime := !sc.IsInTransaction()
	sc.unlock(updateTime)
}

// UnlockUpdateTime returns the connection to the pool. The connection remains active.
// This method is idempotent and can be called multiple times
func (sc *StatefulConnection) UnlockUpdateTime() {
	sc.unlock(true)
}

func (sc *StatefulConnection) unlock(updateTime bool) {
	if sc.dbConn == nil {
		return
	}
	if sc.dbConn.IsClosed() {
		sc.Releasef("unlocked closed connection")
	} else {
		sc.pool.markAsNotInUse(sc, updateTime)
	}
}

// Release is used when the connection will not be used ever again.
// The underlying dbConn is removed so that this connection cannot be used by mistake.
func (sc *StatefulConnection) Release(reason ReleaseReason) {
	sc.Releasef(reason.String())
}

// Releasef is used when the connection will not be used ever again.
// The underlying dbConn is removed so that this connection cannot be used by mistake.
func (sc *StatefulConnection) Releasef(reasonFormat string, a ...interface{}) {
	if sc.dbConn == nil {
		return
	}
	sc.pool.unregister(sc.ConnID, fmt.Sprintf(reasonFormat, a...))
	sc.dbConn.Recycle()
	sc.dbConn = nil
	sc.logReservedConn(context.TODO())
}

// Renew the existing connection with new connection id.
func (sc *StatefulConnection) Renew() error {
	err := sc.pool.renewConn(sc)
	if err != nil {
		sc.Close()
		return util.Wrap(err, "connection renew failed")
	}
	return nil
}

// String returns a printable version of the connection info.
func (sc *StatefulConnection) String() string {
	return fmt.Sprintf(
		"cid:%v, %s",
		sc.ConnID,
		sc.txProps.String(),
	)
}

// Current returns the currently executing query
func (sc *StatefulConnection) Current() string {
	return sc.dbConn.Current()
}

// ID returns the mysql connection ID
func (sc *StatefulConnection) ID() int64 {
	return sc.dbConn.ID()
}

// Kill kills the currently executing query and connection
func (sc *StatefulConnection) Kill(reason string, elapsed time.Duration) error {
	return sc.dbConn.Kill(reason, elapsed)
}

// TxProperties returns the transactional properties of the connection
func (sc *StatefulConnection) TxProperties() *TxProperties {
	return sc.txProps
}

// ReservedID returns the identifier for this connection
func (sc *StatefulConnection) ReservedID() int64 {
	return sc.ConnID
}

// UnderlyingDBConn returns the underlying database connection
func (sc *StatefulConnection) UnderlyingDBConn() *DBConn {
	return sc.dbConn
}

// CleanTxState cleans out the current transaction state
func (sc *StatefulConnection) CleanTxState() {
	sc.txProps = nil
}

// Taint taints the existing connection.
func (sc *StatefulConnection) Taint(ctx context.Context) error {
	if sc.dbConn == nil {
		return errors.New("connection is closed")
	}
	if sc.tainted {
		return errors.New("connection is already reserved")
	}

	c := CallerFromContext(ctx)

	sc.tainted = true
	sc.reservedProps = &ReservedProperties{
		userName:  c.User(),
		host:      c.From(),
		StartTime: time.Now(),
	}
	sc.dbConn.Taint()
	DbStats.ActiveReservedCounter.Add(ctx, 1, label.String("host", c.From()))
	return nil
}

// IsTainted tells us whether this connection is tainted
func (sc *StatefulConnection) IsTainted() bool {
	return sc.tainted
}

// LogTransaction logs transaction related stats
func (sc *StatefulConnection) LogTransaction(reason ReleaseReason) {
	if sc.txProps == nil {
		return //Nothing to log as no transaction exists on this connection.
	}
	sc.txProps.Conclusion = reason.Name()
	sc.txProps.EndTime = time.Now()

	hostLb := label.String("host", sc.txProps.remoteHost)
	reasonLb := label.String("conclusion", reason.String())
	duration := sc.txProps.EndTime.Sub(sc.txProps.StartTime)

	DbStats.TransactionCounter.Add(context.TODO(), 1, hostLb, reasonLb)
	DbStats.TransactionTimes.Add(context.TODO(), duration, hostLb, reasonLb)
}

// logReservedConn logs reserved connection related stats.
func (sc *StatefulConnection) logReservedConn(ctx context.Context) {
	c := ctx
	if c == nil {
		c = context.TODO()
	}

	if sc.reservedProps == nil {
		return //Nothing to log as this connection is not reserved.
	}

	lb := label.String("host", sc.reservedProps.host)

	duration := time.Since(sc.reservedProps.StartTime)
	DbStats.ActiveReservedCounter.Add(c, -1, lb)
	DbStats.ReservedCounter.Add(c, 1, lb)
	DbStats.ReservedTimes.Add(c, duration, lb)
}
