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
	"fmt"
	"github.com/endink/go-sharding/core"
	"github.com/endink/go-sharding/core/provider"
	"github.com/endink/go-sharding/mysql"
	"github.com/endink/go-sharding/mysql/types"
	"github.com/endink/go-sharding/telemetry"
	"github.com/endink/go-sharding/util"
	"github.com/endink/go-sharding/util/timer"
	"sync"
	"time"

	"context"
)

type txEngineState int

// The TxEngine can be in any of these states
const (
	NotServing txEngineState = iota
	Transitioning
	AcceptingReadAndWrite
	AcceptingReadOnly
)

func (state txEngineState) String() string {
	names := [...]string{
		"NotServing",
		"Transitioning",
		"AcceptReadWrite",
		"AcceptingReadOnly"}

	if state < NotServing || state > AcceptingReadOnly {
		return fmt.Sprintf("Unknown - %d", int(state))
	}

	return names[state]
}

// TxEngine is responsible for handling the tx-pool and keeping read-write, read-only or not-serving
// states. It will start and shut down the underlying tx-pool as required.
type TxEngine struct {
	stateLock sync.Mutex
	state     txEngineState
	connParam *mysql.ConnParams
	coord     Coordinator

	// beginRequests is used to make sure that we do not make a state
	// transition while creating new transactions
	beginRequests sync.WaitGroup

	twopcEnabled        bool
	shutdownGracePeriod time.Duration
	abandonAge          time.Duration
	ticks               *timer.Timer

	txPool       *TxPool
	preparedPool *TxPreparedPool
	twoPC        *TwoPC
}

func NewTxEngine(conn *mysql.ConnParams, config DbConfig) *TxEngine {
	coord, _ := provider.DefaultRegistry().LoadFirst(provider.TxCoordinator).(Coordinator)
	return NewTxEngineWithCoord(conn, config, coord)
}

// NewTxEngineWithCoord creates a new TxEngine.
func NewTxEngineWithCoord(conn *mysql.ConnParams, config DbConfig, coordinator Coordinator) *TxEngine {
	te := &TxEngine{
		connParam:           conn,
		shutdownGracePeriod: config.GracePeriods.ShutdownSeconds,
	}
	limiter := NewTxLimiter(config.Tx)
	te.txPool = NewTxPool(config, limiter)
	te.coord = coordinator
	te.twopcEnabled = config.Tx.EnableTwoPC
	if te.twopcEnabled {
		if coordinator == nil {
			log.Error("Coordinator address not specified: Disabling 2PC")
			te.twopcEnabled = false
		}
		if config.Tx.TwoPCAbandonAge <= 0 {
			log.Error("2PC abandon age not specified: Disabling 2PC")
			te.twopcEnabled = false
		}
	}
	//te.coordinatorAddress = config.TwoPCCoordinatorAddress
	te.abandonAge = config.Tx.TwoPCAbandonAge
	te.ticks = timer.NewTimer(te.abandonAge / 2)

	// Set the prepared pool capacity to something lower than
	// tx pool capacity. Those spare connections are needed to
	// perform metadata state change operations. Without this,
	// the system can deadlock if all connections get moved to
	// the TxPreparedPool.
	te.preparedPool = NewTxPreparedPool(config.Tx.Pool.Size - 2)
	readPool := NewPool("TxReadPool", ConnPoolConfig{
		Size:               3,
		IdleTimeoutSeconds: config.Tx.Pool.IdleTimeoutSeconds,
		TimeoutSeconds:     120,
	})
	te.twoPC = NewTwoPC(readPool)

	te.state = NotServing
	return te
}

// AcceptReadWrite will start accepting all transactions.
// If transitioning from RO mode, transactions are rolled
// back before accepting new transactions. This is to allow
// for 2PC state to be correctly initialized.
func (te *TxEngine) AcceptReadWrite() {
	te.transition(AcceptingReadAndWrite)
}

// AcceptReadOnly transitions to read-only mode. If current state
// is read-write, then we wait for shutdown and then transition.
func (te *TxEngine) AcceptReadOnly() {
	te.transition(AcceptingReadOnly)
}

func (te *TxEngine) transition(state txEngineState) {
	te.stateLock.Lock()
	defer te.stateLock.Unlock()
	if te.state == state {
		return
	}

	log.Infof("TxEngine transition: %v", state)
	switch te.state {
	case AcceptingReadOnly, AcceptingReadAndWrite:
		te.shutdownLocked()
	case NotServing:
		// No special action.
	}

	te.state = state
	te.txPool.Open(te.connParam)

	if te.twopcEnabled && te.state == AcceptingReadAndWrite {
		// If there are errors, we choose to raise an alert and
		// continue anyway. Serving traffic is considered more important
		// than blocking everything for the sake of a few transactions.
		if err := te.twoPC.Open(te.connParam); err != nil {
			DbStats.AddInternalErrors(context.TODO(), "TwopcOpen", 1)
			log.Errorf("Could not open TwoPC engine: %v", err)
		}
		if err := te.prepareFromRedo(); err != nil {
			DbStats.AddInternalErrors(context.TODO(), "TwopcResurrection", 1)
			log.Errorf("Could not prepare transactions: %v", err)
		}
		if te.coord != nil {
			te.startWatchdog()
		} else {
			log.Error("Tx coordinator is nil, so can not start watchdog !")
		}
	}
}

// Close will disregard common rules for when to kill transactions
// and wait forever for transactions to wrap up
func (te *TxEngine) Close() {
	te.stateLock.Lock()
	defer func() {
		te.state = NotServing
		te.stateLock.Unlock()
	}()
	if te.state == NotServing {
		return
	}

	te.shutdownLocked()
	log.Info("TxEngine: closed")
}

// Begin begins a transaction, and returns the associated transaction id and the
// statement(s) used to execute the begin (if any).
//
// Subsequent statements can access the connection through the transaction id.
func (te *TxEngine) Begin(ctx context.Context, preQueries []string, reservedID int64, options *types.ExecuteOptions) (newResId int64, beginSQL string, err error) {
	start := time.Now()
	defer func() {
		if beginSQL != "" {
			DbStats.QueryTime.RecordLatency(ctx, "BEGIN", start)
		}
	}()
	ctx, span := telemetry.GlobalTracer.Start(ctx, "TxEngine.Begin")
	defer span.End()
	te.stateLock.Lock()

	canOpenTransactions := te.state == AcceptingReadOnly || te.state == AcceptingReadAndWrite
	if !canOpenTransactions {
		// We are not in a state where we can start new transactions. Abort.
		te.stateLock.Unlock()
		return 0, "", fmt.Errorf("tx engine can't accept new transactions in state %v", te.state)
	}

	// By Add() to beginRequests, we block others from initiating state
	// changes until we have finished adding this transaction
	te.beginRequests.Add(1)
	te.stateLock.Unlock()

	defer te.beginRequests.Done()
	conn, beginSQL, err := te.txPool.Begin(ctx, options, te.state == AcceptingReadOnly, reservedID, preQueries)
	if err != nil {
		return 0, "", err
	}
	defer conn.UnlockUpdateTime()
	newResId = conn.ReservedID()
	return newResId, beginSQL, err
}

// Commit commits the specified transaction and renews connection id if one exists.
func (te *TxEngine) Commit(ctx context.Context, transactionID int64) (int64, string, error) {
	startTime := time.Now()
	ctx, span := telemetry.GlobalTracer.Start(ctx, "TxEngine.Commit")
	defer span.End()
	var query string
	var err error
	connID, err := te.txFinish(transactionID, TxCommit, func(conn *StatefulConnection) error {
		query, err = te.txPool.Commit(ctx, conn)
		return err
	})
	if query != "" {
		DbStats.QueryTime.RecordLatency(ctx, "COMMIT", startTime)
	}
	return connID, query, err
}

// Rollback rolls back the specified transaction, if auto commit, it will try committing.
func (te *TxEngine) Rollback(ctx context.Context, transactionID int64) (int64, error) {
	ctx, span := telemetry.GlobalTracer.Start(ctx, "TxEngine.Complete")
	defer span.End()
	defer DbStats.QueryTime.RecordLatency(ctx, "ROLLBACK", time.Now())
	return te.txFinish(transactionID, TxRollback, func(conn *StatefulConnection) error {
		return te.txPool.Complete(ctx, conn)
	})
}

func (te *TxEngine) txFinish(transactionID int64, reason ReleaseReason, f func(*StatefulConnection) error) (int64, error) {
	conn, err := te.txPool.GetAndLock(transactionID, reason.String())
	if err != nil {
		return 0, err
	}
	err = f(conn)
	if err != nil || !conn.IsTainted() {
		conn.Release(reason)
		return 0, err
	}
	err = conn.Renew()
	if err != nil {
		conn.Release(ConnRenewFail)
		return 0, err
	}
	return conn.ConnID, nil
}

// shutdownLocked closes the TxEngine. If the immediate flag is on,
// then all current transactions are immediately rolled back.
// Otherwise, the function waits for all current transactions
// to conclude. If a shutdown grace period was specified,
// the transactions are rolled back if they're not resolved
// by that time.
func (te *TxEngine) shutdownLocked() {
	immediate := true
	if te.state == AcceptingReadAndWrite {
		immediate = false
	}

	// Unlock, wait for all begin requests to complete, and relock.
	te.state = Transitioning
	te.stateLock.Unlock()
	te.beginRequests.Wait()
	te.stateLock.Lock()

	// Shut down functions are idempotent.
	// No need to check if 2pc is enabled.
	te.stopWatchdog()

	poolEmpty := make(chan bool)
	rollbackDone := make(chan bool)
	// This goroutine decides if transactions have to be
	// forced to rollback, and if so, when. Once done,
	// the function closes rollbackDone, which can be
	// verified to make sure it won't kick in later.
	go func() {
		defer func() {
			RecoverError(log, context.TODO())
			close(rollbackDone)
		}()
		if immediate {
			// Immediately rollback everything and return.
			log.Info("Immediate shutdown: rolling back now.")
			te.txPool.scp.ShutdownNonTx()
			te.shutdownTransactions()
			return
		}
		// If not immediate, we start with shutting down non-tx (reserved)
		// connections.
		te.txPool.scp.ShutdownNonTx()
		if te.shutdownGracePeriod <= 0 {
			// No grace period was specified. Wait indefinitely for transactions to be concluded.
			// TODO(sougou): invoking rollbackPrepared is incorrect here. Prepared statements should
			// actually be rolled back last. But this will cause the shutdown to hang because the
			// tx pool will never become empty, because the prepared pool is holding on to connections
			// from the tx pool. But we plan to deprecate this approach to 2PC. So, this
			// should eventually be deleted.
			te.rollbackPrepared()
			log.Info("No grace period specified: performing normal wait.")
			return
		}
		tmr := time.NewTimer(te.shutdownGracePeriod)
		defer tmr.Stop()
		select {
		case <-tmr.C:
			log.Info("Grace period exceeded: rolling back now.")
			te.shutdownTransactions()
		case <-poolEmpty:
			// The pool cleared before the timer kicked in. Just return.
			log.Info("Transactions completed before grace period: shutting down.")
		}
	}()
	te.txPool.WaitForEmpty()
	// If the goroutine is still running, signal that it can exit.
	close(poolEmpty)
	// Make sure the goroutine has returned.
	<-rollbackDone

	te.txPool.Close()
	te.twoPC.Close()
}

// prepareFromRedo replays and prepares the transactions
// from the redo log, loads previously failed transactions
// into the reserved list, and adjusts the txPool LastID
// to ensure there are no future collisions.
func (te *TxEngine) prepareFromRedo() error {
	ctx := core.LocalContext()
	var allErr core.AllErrorRecorder
	prepared, failed, err := te.twoPC.ReadAllRedo(ctx)
	if err != nil {
		return err
	}

	maxid := int64(0)
outer:
	for _, preparedTx := range prepared {
		txid, err := TransactionID(preparedTx.Dtid)
		if err != nil {
			log.Errorf("Error extracting transaction ID from ditd: %v", err)
		}
		if txid > maxid {
			maxid = txid
		}
		conn, _, err := te.txPool.Begin(ctx, &types.ExecuteOptions{}, false, 0, nil)
		if err != nil {
			allErr.RecordError(err)
			continue
		}
		for _, stmt := range preparedTx.Queries {
			conn.TxProperties().RecordQuery(stmt)
			_, err := conn.Exec(ctx, stmt, 1, false)
			if err != nil {
				allErr.RecordError(err)
				te.txPool.CompleteAndRelease(ctx, conn)
				continue outer
			}
		}
		// We should not use the external Prepare because
		// we don't want to write again to the redo log.
		err = te.preparedPool.Put(conn, preparedTx.Dtid)
		if err != nil {
			allErr.RecordError(err)
			continue
		}
	}
	for _, preparedTx := range failed {
		txid, err := TransactionID(preparedTx.Dtid)
		if err != nil {
			log.Errorf("Error extracting transaction ID from ditd: %v", err)
		}
		if txid > maxid {
			maxid = txid
		}
		te.preparedPool.SetFailed(preparedTx.Dtid)
	}
	te.txPool.AdjustLastID(maxid)
	log.Infof("Prepared %d transactions, and registered %d failures.", len(prepared), len(failed))
	return allErr.Error()
}

// shutdownTransactions rolls back all open transactions
// including the prepared ones.
// This is used for transitioning from a master to a non-master
// serving type.
func (te *TxEngine) shutdownTransactions() {
	te.rollbackPrepared()
	ctx := core.LocalContext()
	// The order of rollbacks is currently not material because
	// we don't allow new statements or commits during
	// this function. In case of any such change, this will
	// have to be revisited.
	te.txPool.Shutdown(ctx)
}

func (te *TxEngine) rollbackPrepared() {
	ctx := core.LocalContext()
	for _, conn := range te.preparedPool.FetchAll() {
		if err := te.txPool.Complete(ctx, conn); err != nil {
			_ = fmt.Errorf("rollback transaction fault: %v", err)
		}
		conn.Release(TxRollback)
	}
}

// startWatchdog starts the watchdog goroutine, which looks for abandoned
// transactions and calls the notifier on them.
func (te *TxEngine) startWatchdog() {
	te.ticks.Start(func() {
		ctx, cancel := context.WithTimeout(core.LocalContext(), te.abandonAge/4)
		defer cancel()

		// Raise alerts on prepares that have been unresolved for too long.
		// Use 5x abandonAge to give opportunity for watchdog to resolve these.
		count, err := te.twoPC.CountUnresolvedRedo(ctx, time.Now().Add(-te.abandonAge*5))
		if err != nil {
			DbStats.AddInternalErrors(ctx, "WatchdogFail", 1)
			log.Errorf("Error reading unresolved prepares: %v", err)
		}
		DbStats.RecordUnresolved(ctx, "Prepares", count)

		// Resolve lingering distributed transactions.
		txs, err := te.twoPC.ReadAbandoned(ctx, time.Now().Add(-te.abandonAge))
		if err != nil {
			DbStats.AddInternalErrors(ctx, "WatchdogFail", 1)
			log.Errorf("Error reading transactions for 2pc watchdog: %v", err)
			return
		}
		if len(txs) == 0 {
			return
		}

		coordConn, err := te.coord.Connect(ctx, te)
		if err != nil {
			DbStats.AddInternalErrors(ctx, "WatchdogFail", 1)
			log.Errorf("Error connecting to coordinator: %v", err)
			return
		}
		defer coordConn.Close()

		var wg sync.WaitGroup
		for tx := range txs {
			wg.Add(1)
			go func(dtid string) {
				defer wg.Done()
				if err = coordConn.ResolveTransaction(ctx, dtid); err != nil {
					DbStats.AddInternalErrors(context.TODO(), "WatchdogFail", 1)
					log.Errorf("Error notifying for dtid %s: %v", dtid, err)
				}
			}(tx)
		}
		wg.Wait()
	})
}

// stopWatchdog stops the watchdog goroutine.
func (te *TxEngine) stopWatchdog() {
	te.ticks.Stop()
}

// ReserveBegin creates a reserved connection, and in it opens a transaction
func (te *TxEngine) ReserveBegin(ctx context.Context, options *types.ExecuteOptions, preQueries []string) (int64, error) {
	ctx, span := telemetry.GlobalTracer.Start(ctx, "TxEngine.ReserveBegin")
	defer span.End()
	conn, err := te.reserve(ctx, options, preQueries)
	if err != nil {
		return 0, util.Wrap(err, "TxEngine.ReserveBegin")
	}
	defer conn.UnlockUpdateTime()
	_, err = te.txPool.begin(ctx, options, te.state == AcceptingReadOnly, conn, nil)
	if err != nil {
		conn.Close()
		conn.Release(ConnInitFail)
		return 0, err
	}
	return conn.ReservedID(), nil
}

// Reserve creates a reserved connection and returns the id to it
func (te *TxEngine) Reserve(ctx context.Context, options *types.ExecuteOptions, txID int64, preQueries []string) (int64, error) {
	ctx, span := telemetry.GlobalTracer.Start(ctx, "TxEngine.Reserve")
	defer span.End()
	if txID == 0 {
		conn, err := te.reserve(ctx, options, preQueries)
		if err != nil {
			return 0, util.Wrap(err, "TxEngine.Reserve")
		}
		defer conn.Unlock()
		return conn.ReservedID(), nil
	}

	conn, err := te.txPool.GetAndLock(txID, "to reserve")
	if err != nil {
		return 0, util.Wrap(err, "TxEngine.Reserve")
	}
	defer conn.Unlock()

	err = te.taintConn(ctx, conn, preQueries)
	if err != nil {
		return 0, util.Wrap(err, "TxEngine.Reserve")
	}
	return conn.ReservedID(), nil
}

// Reserve creates a reserved connection and returns the id to it
func (te *TxEngine) reserve(ctx context.Context, options *types.ExecuteOptions, preQueries []string) (*StatefulConnection, error) {
	te.stateLock.Lock()

	canOpenTransactions := te.state == AcceptingReadOnly || te.state == AcceptingReadAndWrite
	if !canOpenTransactions {
		// We are not in a state where we can start new transactions. Abort.
		te.stateLock.Unlock()
		return nil, fmt.Errorf("cannot provide new connection in state %v", te.state)
	}
	te.stateLock.Unlock()

	conn, err := te.txPool.scp.NewConn(ctx, options)
	if err != nil {
		return nil, err
	}

	err = te.taintConn(ctx, conn, preQueries)
	if err != nil {
		return nil, err
	}

	return conn, err
}

func (te *TxEngine) taintConn(ctx context.Context, conn *StatefulConnection, preQueries []string) error {
	err := conn.Taint(ctx)
	if err != nil {
		return err
	}
	for _, query := range preQueries {
		_, err := conn.Exec(ctx, query, 0 /*maxrows*/, false /*wantFields*/)
		if err != nil {
			conn.Releasef("error during connection setup: %s\n%v", query, err)
			return err
		}
	}
	return nil
}

// Release closes the underlying connection.
func (te *TxEngine) Release(ctx context.Context, connID int64) error {
	defer DbStats.QueryTime.RecordLatency(ctx, "RELEASE", time.Now())
	conn, err := te.txPool.GetAndLock(connID, "for release")
	if err != nil {
		return err
	}

	conn.Release(ConnRelease)

	return nil
}
