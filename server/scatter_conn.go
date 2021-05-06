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
	"errors"
	"fmt"
	"github.com/XiaoMi/Gaea/core"
	errors2 "github.com/XiaoMi/Gaea/core/errors"
	"github.com/XiaoMi/Gaea/database"
	"github.com/XiaoMi/Gaea/logging"
	"github.com/XiaoMi/Gaea/mysql"
	"github.com/XiaoMi/Gaea/mysql/types"
	svrTelemetry "github.com/XiaoMi/Gaea/server/telemetry"
	"github.com/XiaoMi/Gaea/telemetry"
	"github.com/XiaoMi/Gaea/util"
	"go.opentelemetry.io/otel/label"
	"go.opentelemetry.io/otel/metric"
	"regexp"
	"strings"
	"sync"
	"time"
)

var (
	ErrDuplexTarget = errors.New("duplex target")
)

type ShardActionResult struct {
	Error error
}

// ScatterConn is used for executing queries across
// multiple shard level connections.
type ScatterConn struct {
	timings              *telemetry.MultiDurationValueRecorder
	tabletCallErrorCount metric.Int64Counter
	txConn               *TxConn
	gateway              Gateway
	maxMemoryRows        int
}

// shardActionFunc defines the contract for a shard action
// outside of a transaction. Every such function executes the
// necessary action on a shard, sends the results to sResults, and
// return an error if any.  multiGo is capable of executing
// multiple shardActionFunc actions in parallel and
// consolidating the results and errors for the caller.
type shardActionFunc func(rs *database.Target, i int) error

// shardActionTransactionFunc defines the contract for a shard action
// that may be in a transaction. Every such function executes the
// necessary action on a shard (with an optional Begin call), aggregates
// the results, and return an error if any.
// multiGoTransaction is capable of executing multiple
// shardActionTransactionFunc actions in parallel and consolidating
// the results and errors for the caller.
type shardActionTransactionFunc func(rs *database.Target, i int, shardActionInfo *shardActionInfo) (*shardActionInfo, error)

// NewScatterConn creates a new ScatterConn.
func NewScatterConn(statsName string, txConn *TxConn, gw Gateway) *ScatterConn {
	// this only works with TabletGateway
	tabletCallErrorCountStatsName := ""
	if statsName != "" {
		tabletCallErrorCountStatsName = statsName + "ErrorCount"
	}

	return &ScatterConn{
		timings:              svrTelemetry.ExecutionMeter.NewMultiDurationValueRecorder(telemetry.BuildMetricName(statsName), "Scatter connection timings"),
		tabletCallErrorCount: svrTelemetry.ExecutionMeter.NewInt64Counter(telemetry.BuildMetricName(tabletCallErrorCountStatsName), "Error count from tablet calls in scatter connections"),
		txConn:               txConn,
		gateway:              gw,
		maxMemoryRows:        1000,
	}
}

func (stc *ScatterConn) startAction(name string, target *database.Target) (time.Time, []string) {
	statsKey := []string{name, target.Schema, target.DataSource, strings.ToLower(target.TabletType.String())}
	startTime := time.Now()
	return startTime, statsKey
}

func (stc *ScatterConn) endAction(ctx context.Context, startTime time.Time, allErrors *core.AllErrorRecorder, statsKey []string, err *error, session *SafeSession) {
	if *err != nil {
		allErrors.RecordError(*err)
		// Don't increment the error counter for duplicate
		// keys or bad queries, as those errors are caused by
		// client queries and are not server fault.
		if !errors.Is(*err, ErrDuplexTarget) && !errors.Is(*err, errors2.ErrInvalidArgument) {
			if len(statsKey) == 4 {
				lb1 := label.String("operation", statsKey[0])
				lb2 := label.String("schema", statsKey[0])
				lb3 := label.String("data-source", statsKey[1])
				lb4 := label.String("db-type", statsKey[2])
				stc.tabletCallErrorCount.Add(ctx, 1, lb1, lb2, lb3, lb4)
			} else {
				stc.tabletCallErrorCount.Add(ctx, 1)
			}
		}
		if errors.Is(*err, database.ErrResourceExhausted) || errors.Is(*err, database.ErrHasAborted) {
			session.SetRollback()
		}
	}
	stc.timings.RecordMultiLatency(ctx, statsKey, startTime)
}

// ExecuteMultiShard is like Execute,
// but each shard gets its own Sql Queries and BindVariables.
//
// It always returns a non-nil query result and an array of
// shard errors which may be nil so that callers can optionally
// process a partially-successful operation.
func (stc *ScatterConn) ExecuteMultiShard(
	ctx context.Context,
	rss []*database.Target,
	queries []*types.BoundQuery,
	session *SafeSession,
	autocommit bool,
	ignoreMaxMemoryRows bool,
) (qr *types.Result, errs []error) {

	if len(rss) != len(queries) {
		return nil, []error{errors.New("BUG: got mismatched number of queries and shards")}
	}

	// mu protects qr
	var mu sync.Mutex
	qr = new(types.Result)

	if session.InLockSession() && session.TriggerLockHeartBeat() {
		go func() {
			_, lockErr := stc.ExecuteLock(ctx, session.LockSession.Target, &types.BoundQuery{
				Sql:           "select 1",
				BindVariables: nil,
			}, session)
			if lockErr != nil {
				logging.DefaultLogger.Warnf("Locking heartbeat failed, held locks might be released: %s", lockErr.Error())
			}
		}()
	}

	allErrors := stc.multiGoTransaction(
		ctx,
		"Execute",
		rss,
		session,
		autocommit,
		func(rs *database.Target, i int, info *shardActionInfo) (*shardActionInfo, error) {
			var (
				innerqr *types.Result
				err     error
				opts    *types.ExecuteOptions
				alias   *database.Target
			)
			transactionID := info.transactionID
			reservedID := info.reservedID

			if session != nil && session.Session != nil {
				opts = session.Session.Options
			}

			if autocommit {
				// As this is auto-commit, the transactionID is supposed to be zero.
				if info.transactionID != int64(0) {
					return nil, fmt.Errorf("in autocommit mode, transactionID should be zero but was: %d", info.transactionID)
				}
			}

			switch info.actionNeeded {
			case nothing:
				innerqr, err = stc.gateway.Execute(ctx, rs, queries[i].Sql, queries[i].BindVariables, info.transactionID, info.reservedID, opts)
				if err != nil {
					shouldRetry := checkAndResetShardSession(info, err, session)
					if shouldRetry {
						// we seem to have lost our connection. if it was a reserved connection, let's try to recreate it
						info.actionNeeded = reserve
						innerqr, reservedID, err = stc.gateway.ReserveExecute(ctx, rs, session.SetPreQueries(), queries[i].Sql, queries[i].BindVariables, 0 /*transactionId*/, opts)
					}
					if err != nil {
						return nil, err
					}
				}
			case begin:
				innerqr, transactionID, err = stc.gateway.BeginExecute(ctx, rs, session.SavePoints, queries[i].Sql, queries[i].BindVariables, info.reservedID, opts)
				if err != nil {
					return info.updateTransactionID(transactionID, alias), err
				}
			case reserve:
				innerqr, reservedID, err = stc.gateway.ReserveExecute(ctx, rs, session.SetPreQueries(), queries[i].Sql, queries[i].BindVariables, info.transactionID, opts)
				if err != nil {
					return info.updateReservedID(reservedID, alias), err
				}
			case reserveBegin:
				innerqr, transactionID, reservedID, err = stc.gateway.ReserveBeginExecute(ctx, rs, session.SetPreQueries(), queries[i].Sql, queries[i].BindVariables, opts)
				if err != nil {
					return info.updateTransactionAndReservedID(transactionID, reservedID, alias), err
				}
			default:
				return nil, fmt.Errorf("BUG: unexpected actionNeeded on ScatterConn#ExecuteMultiShard %v", info.actionNeeded)
			}
			mu.Lock()
			defer mu.Unlock()

			// Don't append more rows if row count is exceeded.
			if ignoreMaxMemoryRows || len(qr.Rows) <= stc.maxMemoryRows {
				qr.AppendResult(innerqr)
			}
			return info.updateTransactionAndReservedID(transactionID, reservedID, alias), nil
		},
	)

	if !ignoreMaxMemoryRows && len(qr.Rows) > stc.maxMemoryRows {
		return nil, []error{mysql.NewSQLError(mysql.ERNetPacketTooLarge, "", "in-memory row count exceeded allowed limit of %d", stc.maxMemoryRows)}
	}

	return qr, allErrors.GetErrors()
}

var errRegx = regexp.MustCompile("transaction ([a-z0-9:]+) ended")

func checkAndResetShardSession(info *shardActionInfo, err error, session *SafeSession) bool {
	if info.reservedID != 0 && info.transactionID == 0 && wasConnectionClosed(err) {
		if e := session.ResetShard(info.target); e != nil {
			logging.DefaultLogger.Error("Reset safe session fault\n%v", e)
		}
		return true
	}
	return false
}

func (stc *ScatterConn) processOneStreamingResult(mu *sync.Mutex, fieldSent *bool, qr *types.Result, callback func(*types.Result) error) error {
	mu.Lock()
	defer mu.Unlock()
	if *fieldSent {
		if len(qr.Rows) == 0 {
			// It's another field info result. Don't send.
			return nil
		}
	} else {
		if len(qr.Fields) == 0 {
			// Unreachable: this can happen only if vttablet misbehaves.
			return errors.New("received rows before fields for shard")
		}
		*fieldSent = true
	}

	return callback(qr)
}

// StreamExecute executes a streaming query on vttablet. The retry rules are the same.
// Note we guarantee the callback will not be called concurrently
// by multiple go routines, through processOneStreamingResult.
func (stc *ScatterConn) StreamExecute(
	ctx context.Context,
	query string,
	bindVars map[string]*types.BindVariable,
	rss []*database.Target,
	options *types.ExecuteOptions,
	callback func(reply *types.Result) error,
) error {

	// mu protects fieldSent, replyErr and callback
	var mu sync.Mutex
	fieldSent := false

	allErrors := stc.multiGo(ctx, "StreamExecute", rss, func(rs *database.Target, i int) error {
		return stc.gateway.StreamExecute(ctx, rs, query, bindVars, 0, options, func(qr *types.Result) error {
			return stc.processOneStreamingResult(&mu, &fieldSent, qr, callback)
		})
	})
	return allErrors.Error()
}

// StreamExecuteMulti is like StreamExecute,
// but each shard gets its own bindVars. If len(shards) is not equal to
// len(bindVars), the function panics.
// Note we guarantee the callback will not be called concurrently
// by multiple go routines, through processOneStreamingResult.
func (stc *ScatterConn) StreamExecuteMulti(
	ctx context.Context,
	query string,
	rss []*database.Target,
	bindVars []map[string]*types.BindVariable,
	options *types.ExecuteOptions,
	callback func(reply *types.Result) error,
) error {
	// mu protects fieldSent, callback and replyErr
	var mu sync.Mutex
	fieldSent := false

	allErrors := stc.multiGo(ctx, "StreamExecute", rss, func(rs *database.Target, i int) error {
		return stc.gateway.StreamExecute(ctx, rs, query, bindVars[i], 0, options, func(qr *types.Result) error {
			return stc.processOneStreamingResult(&mu, &fieldSent, qr, callback)
		})
	})
	return allErrors.Error()
}

// timeTracker is a convenience wrapper used by MessageStream
// to track how long a stream has been unavailable.
type timeTracker struct {
	mu         sync.Mutex
	timestamps map[*database.Target]time.Time
}

func newTimeTracker() *timeTracker {
	return &timeTracker{
		timestamps: make(map[*database.Target]time.Time),
	}
}

// Reset resets the timestamp set by Record.
func (tt *timeTracker) Reset(target *database.Target) {
	tt.mu.Lock()
	defer tt.mu.Unlock()
	delete(tt.timestamps, target)
}

// Record records the time to Now if there was no previous timestamp,
// and it keeps returning that value until the next Reset.
func (tt *timeTracker) Record(target *database.Target) time.Time {
	tt.mu.Lock()
	defer tt.mu.Unlock()
	last, ok := tt.timestamps[target]
	if !ok {
		last = time.Now()
		tt.timestamps[target] = last
	}
	return last
}

// multiGo performs the requested 'action' on the specified
// shards in parallel. This does not handle any transaction state.
// The action function must match the shardActionFunc2 signature.
func (stc *ScatterConn) multiGo(
	ctx context.Context,
	name string,
	rss []*database.Target,
	action shardActionFunc,
) (allErrors *core.AllErrorRecorder) {
	allErrors = new(core.AllErrorRecorder)
	if len(rss) == 0 {
		return allErrors
	}

	oneShard := func(rs *database.Target, i int) {
		var err error
		startTime, statsKey := stc.startAction(name, rs)
		// Send a dummy session.
		// TODO(sougou): plumb a real session through this call.
		defer stc.endAction(ctx, startTime, allErrors, statsKey, &err, NewSafeSession(nil))
		err = action(rs, i)
	}

	if len(rss) == 1 {
		// only one shard, do it synchronously.
		oneShard(rss[0], 0)
		return allErrors
	}

	var wg sync.WaitGroup
	for i, rs := range rss {
		wg.Add(1)
		go func(rs *database.Target, i int) {
			defer wg.Done()
			oneShard(rs, i)
		}(rs, i)
	}
	wg.Wait()
	return allErrors
}

// multiGoTransaction performs the requested 'action' on the specified
// ResolvedShards in parallel. For each shard, if the requested
// session is in a transaction, it opens a new transactions on the connection,
// and updates the Session with the transaction id. If the session already
// contains a transaction id for the shard, it reuses it.
// The action function must match the shardActionTransactionFunc signature.
//
// It returns an error recorder in which each shard error is recorded positionally,
// i.e. if rss[2] had an error, then the error recorder will store that error
// in the second position.
func (stc *ScatterConn) multiGoTransaction(
	ctx context.Context,
	name string,
	rss []*database.Target,
	session *SafeSession,
	autocommit bool,
	action shardActionTransactionFunc,
) (allErrors *core.AllErrorRecorder) {

	numShards := len(rss)
	allErrors = new(core.AllErrorRecorder)

	if numShards == 0 {
		return allErrors
	}
	oneShard := func(rs *database.Target, i int) {
		var err error
		startTime, statsKey := stc.startAction(name, rs)
		defer stc.endAction(ctx, startTime, allErrors, statsKey, &err, session)

		cmd := actionInfo(rs, session, autocommit)
		updated, err := action(rs, i, cmd)
		if updated == nil {
			return
		}
		if updated.actionNeeded != nothing && (updated.transactionID != 0 || updated.reservedID != 0) {
			appendErr := session.AppendOrUpdate(&database.DbSession{
				Target:        rs,
				TransactionId: updated.transactionID,
				ReservedId:    updated.reservedID,
				//TabletAlias:   updated.target,
			}, stc.txConn.mode)
			if appendErr != nil {
				err = appendErr
			}
		}
	}

	if numShards == 1 {
		// only one shard, do it synchronously.
		for i, rs := range rss {
			oneShard(rs, i)
		}
	} else {
		var wg sync.WaitGroup
		for i, rs := range rss {
			wg.Add(1)
			go func(rs *database.Target, i int) {
				defer wg.Done()
				oneShard(rs, i)
			}(rs, i)
		}
		wg.Wait()
	}

	if session.MustRollback() {
		if e := stc.txConn.Rollback(ctx, session); e != nil {
			logging.DefaultLogger.Error("TxConn rollback fault\n%v", e)
		}
	}
	return allErrors
}

// ExecuteLock performs the requested 'action' on the specified
// ResolvedShard. If the lock session already has a reserved connection,
// it reuses it. Otherwise open a new reserved connection.
// The action function must match the shardActionTransactionFunc signature.
//
// It returns an error recorder in which each shard error is recorded positionally,
// i.e. if rss[2] had an error, then the error recorder will store that error
// in the second position.
func (stc *ScatterConn) ExecuteLock(
	ctx context.Context,
	rs *database.Target,
	query *types.BoundQuery,
	session *SafeSession,
) (*types.Result, error) {

	var (
		qr   *types.Result
		err  error
		opts *types.ExecuteOptions
		//alias *database.Target
	)
	allErrors := new(core.AllErrorRecorder)
	startTime, statsKey := stc.startAction("ExecuteLock", rs)
	defer stc.endAction(ctx, startTime, allErrors, statsKey, &err, session)

	if session == nil || session.Session == nil {
		return nil, errors.New("session cannot be nil")
	}

	opts = session.Session.Options
	info, err := lockInfo(rs, session)
	// Lock session is created on alphabetic sorted keyspace.
	// This error will occur if the existing session target does not match the current target.
	// This will happen either due to re-sharding or a new keyspace which comes before the existing order.
	// In which case, we will try to release old locks and return error.
	if err != nil {
		_ = stc.txConn.ReleaseLock(ctx, session)
		return nil, util.Wrap(err, "Any previous held locks are released")
	}

	reservedID := info.reservedID

	switch info.actionNeeded {
	case nothing:
		if reservedID == 0 {
			return nil, fmt.Errorf("BUG: reservedID zero not expected %v", reservedID)
		}
		qr, err = stc.gateway.Execute(ctx, rs, query.Sql, query.BindVariables, 0 /* transactionID */, reservedID, opts)
		if err != nil && wasConnectionClosed(err) {
			session.ResetLock()
			err = util.Wrap(err, "held locks released")
		}
		session.UpdateLockHeartbeat()
	case reserve:
		qr, reservedID, err = stc.gateway.ReserveExecute(ctx, rs, session.SetPreQueries(), query.Sql, query.BindVariables, 0 /* transactionID */, opts)
		if err != nil && reservedID != 0 {
			_ = stc.txConn.ReleaseLock(ctx, session)
		}

		if reservedID != 0 {
			session.SetLockSession(&database.DbSession{
				Target:     rs,
				ReservedId: reservedID,
			})
		}
	default:
		return nil, fmt.Errorf("BUG: unexpected actionNeeded on ScatterConn#ExecuteLock %v", info.actionNeeded)
	}

	if err != nil {
		return nil, err
	}
	return qr, err
}

func wasConnectionClosed(err error) bool {
	sqlErr := mysql.NewSQLErrorFromError(err).(*mysql.SQLError)

	return sqlErr.Number() == mysql.CRServerGone ||
		sqlErr.Number() == mysql.CRServerLost ||
		(sqlErr.Number() == mysql.ERQueryInterrupted && errRegx.MatchString(sqlErr.Error()))
}

// actionInfo looks at the current session, and returns information about what needs to be done for this tablet
func actionInfo(target *database.Target, session *SafeSession, autocommit bool) *shardActionInfo {
	if !(session.InTransaction() || session.InReservedConn()) {
		return &shardActionInfo{}
	}
	// No need to protect ourselves from the race condition between
	// Find and AppendOrUpdate. The higher level functions ensure that no
	// duplicate (target) tuples can execute
	// this at the same time.
	transactionID, reservedID, alias := session.Find(target.Schema, target.DataSource, target.TabletType)

	shouldReserve := session.InReservedConn() && reservedID == 0
	shouldBegin := session.InTransaction() && transactionID == 0 && !autocommit

	var act = nothing
	switch {
	case shouldBegin && shouldReserve:
		act = reserveBegin
	case shouldReserve:
		act = reserve
	case shouldBegin:
		act = begin
	}

	return &shardActionInfo{
		actionNeeded:  act,
		transactionID: transactionID,
		reservedID:    reservedID,
		target:        alias,
	}
}

// lockInfo looks at the current session, and returns information about what needs to be done for this tablet
func lockInfo(target *database.Target, session *SafeSession) (*shardActionInfo, error) {
	if session.LockSession == nil {
		return &shardActionInfo{actionNeeded: reserve}, nil
	}

	if !target.Equals(session.LockSession.Target) {
		return nil, fmt.Errorf("%w\ntarget does match the existing lock session target: (%v, %v)", ErrDuplexTarget)
	}

	return &shardActionInfo{
		actionNeeded: nothing,
		reservedID:   session.LockSession.ReservedId,
		target:       session.LockSession.Target,
	}, nil
}

type shardActionInfo struct {
	actionNeeded              actionNeeded
	reservedID, transactionID int64
	target                    *database.Target
}

func (sai *shardActionInfo) updateTransactionID(txID int64, alias *database.Target) *shardActionInfo {
	return sai.updateTransactionAndReservedID(txID, sai.reservedID, alias)
}

func (sai *shardActionInfo) updateReservedID(rID int64, alias *database.Target) *shardActionInfo {
	return sai.updateTransactionAndReservedID(sai.transactionID, rID, alias)
}

func (sai *shardActionInfo) updateTransactionAndReservedID(txID int64, rID int64, target *database.Target) *shardActionInfo {
	newInfo := *sai
	newInfo.reservedID = rID
	newInfo.transactionID = txID
	newInfo.target = target
	return &newInfo
}

type actionNeeded int

const (
	nothing actionNeeded = iota
	reserveBegin
	reserve
	begin
)
