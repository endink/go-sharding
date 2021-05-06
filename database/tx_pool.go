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

package database

import (
	"context"
	"errors"
	"fmt"
	"github.com/endink/go-sharding/mysql"
	"github.com/endink/go-sharding/mysql/types"
	"github.com/endink/go-sharding/telemetry"
	"github.com/endink/go-sharding/util"
	"github.com/endink/go-sharding/util/sync2"
	"github.com/endink/go-sharding/util/timer"
	"go.opentelemetry.io/otel/label"
	"sync"
	"time"
)

const txLogInterval = 1 * time.Minute

const (
	MetricTxOperation = "operation"
)

var txIsolations = map[types.TransactionIsolation]queries{
	types.IsolationDefault:                    {setIsolationLevel: "", openTransaction: "begin"},
	types.IsolationRepeatableRead:             {setIsolationLevel: "REPEATABLE READ", openTransaction: "begin"},
	types.IsolationReadCommitted:              {setIsolationLevel: "READ COMMITTED", openTransaction: "begin"},
	types.IsolationReadUncommitted:            {setIsolationLevel: "READ UNCOMMITTED", openTransaction: "begin"},
	types.IsolationSerializable:               {setIsolationLevel: "SERIALIZABLE", openTransaction: "begin"},
	types.IsolationConsistentSnapshotReadOnly: {setIsolationLevel: "REPEATABLE READ", openTransaction: "start transaction with consistent snapshot, read only"},
}

type (
	// TxPool does a lot of the transactional operations on StatefulConnections. It does not, with two exceptions,
	// concern itself with a connections life cycle. The two exceptions are Begin, which creates a new StatefulConnection,
	// and CompleteAndRelease, which does a Release after doing the rollback.
	TxPool struct {
		scp                *StatefulConnectionPool
		transactionTimeout sync2.AtomicDuration
		ticks              *timer.Timer
		limiter            TxLimiter

		logMu   sync.Mutex
		lastLog time.Time
	}
	queries struct {
		setIsolationLevel string
		openTransaction   string
	}
)

// NewTxPool creates a new TxPool. It's not operational until it's Open'd.
func NewTxPool(config DbConfig, limiter TxLimiter) *TxPool {
	transactionTimeout := config.Tx.Timeout
	axp := &TxPool{
		scp:                NewStatefulConnPool(config.Tx.Pool),
		transactionTimeout: sync2.NewAtomicDuration(transactionTimeout),
		ticks:              timer.NewTimer(transactionTimeout / 10),
		limiter:            limiter,
		//txStats:            DbMeter.NewMultiDurationValueRecorder("transactions", "Tx stats"),
	}
	// Careful: conns also exports name+"xxx" vars,
	// but we know it doesn't export TimeoutSeconds.
	DbMeter.NewDurationObserver("transaction_timeout", "Tx timeout", axp.transactionTimeout.Get)
	return axp
}

// Open makes the TxPool operational. This also starts the transaction killer
// that will kill long-running transactions.
func (tp *TxPool) Open(connParams *mysql.ConnParams) {
	tp.scp.Open(connParams)
	tp.ticks.Start(func() { tp.transactionKiller() })
}

// Close closes the TxPool. A closed pool can be reopened.
func (tp *TxPool) Close() {
	tp.ticks.Stop()
	tp.scp.Close()
}

// AdjustLastID adjusts the last transaction id to be at least
// as large as the input value. This will ensure that there are
// no dtid collisions with future transactions.
func (tp *TxPool) AdjustLastID(id int64) {
	tp.scp.AdjustLastID(id)
}

// Shutdown immediately rolls back all transactions that are not in use.
// In-use connections will be closed when they are unlocked (not in use).
func (tp *TxPool) Shutdown(ctx context.Context) {
	for _, v := range tp.scp.ShutdownAll() {
		tp.CompleteAndRelease(ctx, v)
	}
}

func (tp *TxPool) transactionKiller() {
	defer RecoverError(log, context.TODO())
	for _, conn := range tp.scp.GetOutdated(tp.Timeout(), "for tx killer rollback") {
		log.Warnf("killing transaction (exceeded timeout: %v): %s", tp.Timeout(), conn.String())
		switch {
		case conn.IsTainted():
			conn.Close()
			DbStats.KillCounter.Add(context.TODO(), 1, label.String("type", "ReservedConnection"))
		case conn.IsInTransaction():
			_, err := conn.Exec(context.Background(), "rollback", 1, false)
			if err != nil {
				conn.Close()
			}
			DbStats.KillCounter.Add(context.TODO(), 1, label.String("type", "Transactions"))
		}
		// For logging, as transaction is killed as the connection is closed.
		if conn.IsTainted() && conn.IsInTransaction() {
			DbStats.KillCounter.Add(context.TODO(), 1, label.String("type", "Transactions"))
		}
		if conn.IsInTransaction() {
			tp.txComplete(conn, TxKill)
		}
		conn.Releasef("exceeded timeout: %v", tp.Timeout())
	}
}

// WaitForEmpty waits until all active transactions are completed.
func (tp *TxPool) WaitForEmpty() {
	tp.scp.WaitForEmpty()
}

// GetAndLock fetches the connection associated to the connID and blocks it from concurrent use
// You must call Unlock on TxConnection once done.
func (tp *TxPool) GetAndLock(connID int64, reason string) (*StatefulConnection, error) {
	conn, err := tp.scp.GetAndLock(connID, reason)
	if err != nil {
		return nil, fmt.Errorf("%w\ntransaction %d: %v", ErrHasAborted, connID, err)
	}
	return conn, nil
}

// Commit commits the transaction on the connection.
func (tp *TxPool) Commit(ctx context.Context, txConn *StatefulConnection) (string, error) {
	if !txConn.IsInTransaction() {
		return "", errors.New("not in a transaction")
	}
	ctx, span := telemetry.GlobalTracer.Start(ctx, "TxPool.Commit")
	defer span.End()
	defer tp.txComplete(txConn, TxCommit)
	if txConn.TxProperties().Autocommit {
		return "", nil
	}

	if _, err := txConn.Exec(ctx, "commit", 1, false); err != nil {
		txConn.Close()
		return "", err
	}
	return "commit", nil
}

// CompleteAndRelease auto commit or rolls back the transaction on the specified connection, and releases the connection when done
func (tp *TxPool) CompleteAndRelease(ctx context.Context, txConn *StatefulConnection) {
	defer txConn.Release(TxRollback)
	rollbackError := tp.Complete(ctx, txConn)
	if rollbackError != nil {
		log.Errorf("tried to rollback, but failed with: %v", rollbackError.Error())
	}
}

// Complete auto commit or rolls back the transaction on the specified connection.
func (tp *TxPool) Complete(ctx context.Context, txConn *StatefulConnection) error {
	ctx, span := telemetry.GlobalTracer.Start(ctx, "TxPool.Complete")
	defer span.End()
	if txConn.IsClosed() || !txConn.IsInTransaction() {
		return nil
	}
	if txConn.TxProperties().Autocommit {
		tp.txComplete(txConn, TxCommit)
		return nil
	}
	defer tp.txComplete(txConn, TxRollback)
	if _, err := txConn.Exec(ctx, "rollback", 1, false); err != nil {
		txConn.Close()
		return err
	}
	return nil
}

// Begin begins a transaction, and returns the associated connection and
// the statements (if any) executed to initiate the transaction. In autocommit
// mode the statement will be "".
// The connection returned is locked for the callee and its responsibility is to unlock the connection.
func (tp *TxPool) Begin(ctx context.Context, options *types.ExecuteOptions, readOnly bool, reservedID int64, preQueries []string) (*StatefulConnection, string, error) {
	ctx, span := telemetry.GlobalTracer.Start(ctx, "TxPool.Begin")
	defer span.End()

	var conn *StatefulConnection
	var err error
	if reservedID != 0 {
		conn, err = tp.scp.GetAndLock(reservedID, "start transaction on reserve conn")
	} else {
		c := CallerFromContext(ctx)
		if !tp.limiter.Get(c) {
			return nil, "", fmt.Errorf("%w\nper-user transaction pool connection limit exceeded", ErrResourceExhausted)
		}
		conn, err = tp.createConn(ctx, options)
		defer func() {
			if err != nil {
				// The transaction limiter frees transactions on rollback or commit. If we fail to create the transaction,
				// release immediately since there will be no rollback or commit.
				tp.limiter.Release(c)
			}
		}()
	}
	if err != nil {
		return nil, "", err
	}
	sql, err := tp.begin(ctx, options, readOnly, conn, preQueries)
	if err != nil {
		conn.Close()
		conn.Release(ConnInitFail)
		return nil, "", err
	}
	return conn, sql, nil
}

func (tp *TxPool) begin(ctx context.Context, options *types.ExecuteOptions, readOnly bool, conn *StatefulConnection, preQueries []string) (string, error) {
	beginQueries, autocommit, err := createTransaction(ctx, options, conn, readOnly, preQueries)
	if err != nil {
		return "", err
	}
	c := CallerFromContext(ctx)
	conn.txProps = tp.NewTxProps(c.User(), c.From(), autocommit)

	return beginQueries, nil
}

func (tp *TxPool) createConn(ctx context.Context, options *types.ExecuteOptions) (*StatefulConnection, error) {
	conn, err := tp.scp.NewConn(ctx, options)
	if err != nil {
		switch err {
		case util.ErrCtxTimeout:
			//tp.LogActive()
			err = fmt.Errorf("%w\ntransaction pool aborting request due to already expired context", ErrResourceExhausted)
		case util.ErrTimeout:
			//tp.LogActive()
			err = fmt.Errorf("%w\ntransaction pool connection limit exceeded", ErrResourceExhausted)
		}
		return nil, err
	}
	return conn, nil
}

func createTransaction(ctx context.Context, options *types.ExecuteOptions, conn *StatefulConnection, readOnly bool, preQueries []string) (string, bool, error) {
	beginQueries := ""

	autocommitTransaction := false
	if txQueries, ok := txIsolations[options.TransactionIsolation]; ok {
		if txQueries.setIsolationLevel != "" {
			txQuery := "set transaction isolation level " + txQueries.setIsolationLevel
			if err := conn.execWithRetry(ctx, txQuery, 1, false); err != nil {
				return "", false, util.Wrap(err, txQuery)
			}
			beginQueries = txQueries.setIsolationLevel + "; "
		}
		beginSQL := txQueries.openTransaction
		if readOnly &&
			options.TransactionIsolation != types.IsolationConsistentSnapshotReadOnly {
			beginSQL = "start transaction read only"
		}
		if err := conn.execWithRetry(ctx, beginSQL, 1, false); err != nil {
			return "", false, util.Wrap(err, beginSQL)
		}
		beginQueries = beginQueries + beginSQL
	} else if options.TransactionIsolation == types.IsolationAutoCommit {
		autocommitTransaction = true
	} else {
		return "", false, fmt.Errorf("don't know how to open a transaction of this type: %v", options.TransactionIsolation)
	}

	for _, preQuery := range preQueries {
		if _, err := conn.Exec(ctx, preQuery, 1, false); err != nil {
			return "", false, util.Wrap(err, preQuery)
		}
	}
	return beginQueries, autocommitTransaction, nil
}

// LogActive causes all existing transactions to be logged when they complete.
// The logging is throttled to no more than once every txLogInterval.
//func (tp *TxPool) LogActive() {
//	tp.logMu.Lock()
//	defer tp.logMu.Unlock()
//	if time.Since(tp.lastLog) < txLogInterval {
//		return
//	}
//	tp.lastLog = time.Now()
//	tp.scp.ForAllTxProperties(func(props *tx.ReservedProperties) {
//		props.LogToFile = true
//	})
//}

// Timeout returns the transaction timeout.
func (tp *TxPool) Timeout() time.Duration {
	return tp.transactionTimeout.Get()
}

// SetTimeout sets the transaction timeout.
func (tp *TxPool) SetTimeout(timeout time.Duration) {
	tp.transactionTimeout.Set(timeout)
	tp.ticks.SetInterval(timeout / 10)
}

func (tp *TxPool) txComplete(conn *StatefulConnection, reason ReleaseReason) {
	conn.LogTransaction(reason)
	tp.limiter.Release(conn.TxProperties())
	conn.CleanTxState()
}
