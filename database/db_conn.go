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
	"github.com/XiaoMi/Gaea/mysql"
	"github.com/XiaoMi/Gaea/mysql/types"
	"github.com/XiaoMi/Gaea/telemetry"
	"github.com/XiaoMi/Gaea/util"
	"github.com/XiaoMi/Gaea/util/sync2"
	"go.opentelemetry.io/otel/label"
	"strings"
	"sync"
	"time"

	"context"
)

// DBConn is a db connection for tabletserver.
// It performs automatic reconnects as needed.
// Its Execute function has a timeout that can kill
// its own queries and the underlying connection.
// It will also trigger a CheckMySQL whenever applicable.
type DBConn struct {
	conn    *DBConnection
	info    *mysql.ConnParams
	pool    *Pool
	dbaPool *ConnectionPool
	current sync2.AtomicString

	// err will be set if a query is killed through a Kill.
	errmu sync.Mutex
	err   error
}

// NewDBConn creates a new DBConn. It triggers a CheckMySQL if creation fails.
func NewDBConn(ctx context.Context, cp *Pool, appParams *mysql.ConnParams) (*DBConn, error) {
	start := time.Now()

	c, err := NewDBConnection(ctx, appParams)
	if err != nil {
		DbStats.DbConnectErrLatency.Record(ctx, time.Since(start))
		//cp.env.CheckMySQL()
		return nil, err
	}

	DbStats.DbConnectLatency.Record(ctx, time.Since(start))

	return &DBConn{
		conn:    c,
		info:    appParams,
		dbaPool: cp.dbaPool,
		pool:    cp,
	}, nil
}

// NewDBConnNoPool creates a new DBConn without a pool.
func NewDBConnNoPool(ctx context.Context, params *mysql.ConnParams, dbaPool *ConnectionPool) (*DBConn, error) {
	c, err := NewDBConnection(ctx, params)
	if err != nil {
		return nil, err
	}
	return &DBConn{
		conn:    c,
		info:    params,
		dbaPool: dbaPool,
		pool:    nil,
	}, nil
}

// Err returns an error if there was a client initiated error
// like a query kill.
func (dbc *DBConn) Err() error {
	dbc.errmu.Lock()
	defer dbc.errmu.Unlock()
	return dbc.err
}

// Exec executes the specified query. If there is a connection error, it will reconnect
// and retry. A failed reconnect will trigger a CheckMySQL.
func (dbc *DBConn) Exec(ctx context.Context, query string, maxrows int, wantfields bool) (*types.Result, error) {
	ctx, span := telemetry.GlobalTracer.Start(ctx, "DBConn.Exec")
	defer span.End()

	for attempt := 1; attempt <= 2; attempt++ {
		r, err := dbc.execOnce(ctx, query, maxrows, wantfields)
		switch {
		case err == nil:
			// Success.
			return r, nil
		case !mysql.IsConnErr(err):
			// Not a connection error. Don't retry.
			return nil, err
		case attempt == 2:
			// Reached the retry limit.
			return nil, err
		}

		// Connection error. Retry if context has not expired.
		select {
		case <-ctx.Done():
			return nil, err
		default:
		}

		if reconnectErr := dbc.reconnect(ctx); reconnectErr != nil {
			//dbc.pool.env.CheckMySQL()
			// Return the error of the reconnect and not the original connection error.
			return nil, reconnectErr
		}

		// Reconnect succeeded. Retry query at second attempt.
	}
	panic("unreachable")
}

func (dbc *DBConn) execOnce(ctx context.Context, query string, maxrows int, wantfields bool) (*types.Result, error) {
	dbc.current.Set(query)
	defer dbc.current.Set("")

	// Check if the context is already past its deadline before
	// trying to execute the query.
	select {
	case <-ctx.Done():
		return nil, fmt.Errorf("%v before execution started", ctx.Err())
	default:
	}
	defer DbStats.DbExecLatency.RecordLatency(ctx, time.Now())

	done, wg := dbc.setDeadline(ctx)
	qr, err := dbc.conn.ExecuteFetch(query, maxrows, wantfields)

	if done != nil {
		close(done)
		wg.Wait()
	}
	if dbcerr := dbc.Err(); dbcerr != nil {
		return nil, dbcerr
	}
	return qr, err
}

// ExecOnce executes the specified query, but does not retry on connection errors.
func (dbc *DBConn) ExecOnce(ctx context.Context, query string, maxrows int, wantfields bool) (*types.Result, error) {
	return dbc.execOnce(ctx, query, maxrows, wantfields)
}

// FetchNext returns the next result set.
func (dbc *DBConn) FetchNext(ctx context.Context, maxrows int, wantfields bool) (*types.Result, error) {
	// Check if the context is already past its deadline before
	// trying to fetch the next result.
	select {
	case <-ctx.Done():
		return nil, fmt.Errorf("%v before reading next result set", ctx.Err())
	default:
	}
	res, _, _, err := dbc.conn.ReadQueryResult(maxrows, wantfields)
	if err != nil {
		return nil, err
	}
	return res, err

}

// Stream executes the query and streams the results.
func (dbc *DBConn) Stream(ctx context.Context, query string, callback func(*types.Result) error, streamBufferSize int, includedFields types.IncludedFields) error {
	ctx, span := telemetry.GlobalTracer.Start(ctx, "DBConn.Stream")
	span.SetAttributes(label.String("query", query))
	defer span.End()

	resultSent := false
	for attempt := 1; attempt <= 2; attempt++ {
		err := dbc.streamOnce(
			ctx,
			query,
			func(r *types.Result) error {
				if !resultSent {
					resultSent = true
					r = r.StripMetadata(includedFields)
				}
				return callback(r)
			},
			streamBufferSize,
		)
		switch {
		case err == nil:
			// Success.
			return nil
		case !mysql.IsConnErr(err):
			// Not a connection error. Don't retry.
			return err
		case attempt == 2:
			// Reached the retry limit.
			return err
		case resultSent:
			// Don't retry if streaming has started.
			return err
		}

		// Connection error. Retry if context has not expired.
		select {
		case <-ctx.Done():
			return err
		default:
		}
		if reconnectErr := dbc.reconnect(ctx); reconnectErr != nil {
			//dbc.pool.env.CheckMySQL()
			// Return the error of the reconnect and not the original connection error.
			return reconnectErr
		}
	}
	panic("unreachable")
}

func (dbc *DBConn) streamOnce(ctx context.Context, query string, callback func(*types.Result) error, streamBufferSize int) error {
	defer DbStats.DbExecStreamLatency.RecordLatency(ctx, time.Now())

	dbc.current.Set(query)
	defer dbc.current.Set("")

	done, wg := dbc.setDeadline(ctx)
	err := dbc.conn.ExecuteStreamFetch(query, callback, streamBufferSize)

	if done != nil {
		close(done)
		wg.Wait()
	}
	if dbcerr := dbc.Err(); dbcerr != nil {
		return dbcerr
	}
	return err
}

var (
	getModeSQL    = "select @@global.sql_mode"
	getAutocommit = "select @@autocommit"
	getAutoIsNull = "select @@sql_auto_is_null"
)

// VerifyMode is a helper method to verify mysql is running with
// sql_mode = STRICT_TRANS_TABLES or STRICT_ALL_TABLES and autocommit=ON.
func (dbc *DBConn) VerifyMode(strictTransTables bool) error {
	if strictTransTables {
		qr, err := dbc.conn.ExecuteFetch(getModeSQL, 2, false)
		if err != nil {
			return util.Wrap(err, "could not verify mode")
		}
		if len(qr.Rows) != 1 {
			return fmt.Errorf("incorrect rowcount received for %s: %d", getModeSQL, len(qr.Rows))
		}
		sqlMode := qr.Rows[0][0].ToString()
		if !(strings.Contains(sqlMode, "STRICT_TRANS_TABLES") || strings.Contains(sqlMode, "STRICT_ALL_TABLES")) {
			return fmt.Errorf("require sql_mode to be STRICT_TRANS_TABLES or STRICT_ALL_TABLES: got '%s'", qr.Rows[0][0].ToString())
		}
	}
	qr, err := dbc.conn.ExecuteFetch(getAutocommit, 2, false)
	if err != nil {
		return util.Wrap(err, "could not verify mode")
	}
	if len(qr.Rows) != 1 {
		return fmt.Errorf("incorrect rowcount received for %s: %d", getAutocommit, len(qr.Rows))
	}
	if !strings.Contains(qr.Rows[0][0].ToString(), "1") {
		return fmt.Errorf("require autocommit to be 1: got %s", qr.Rows[0][0].ToString())
	}
	qr, err = dbc.conn.ExecuteFetch(getAutoIsNull, 2, false)
	if err != nil {
		return util.Wrap(err, "could not verify mode")
	}
	if len(qr.Rows) != 1 {
		return fmt.Errorf("incorrect rowcount received for %s: %d", getAutoIsNull, len(qr.Rows))
	}
	if !strings.Contains(qr.Rows[0][0].ToString(), "0") {
		return fmt.Errorf("require sql_auto_is_null to be 0: got %s", qr.Rows[0][0].ToString())
	}
	return nil
}

// Close closes the DBConn.
func (dbc *DBConn) Close() {
	dbc.conn.Close()
}

// IsClosed returns true if DBConn is closed.
func (dbc *DBConn) IsClosed() bool {
	return dbc.conn.IsClosed()
}

// Recycle returns the DBConn to the pool.
func (dbc *DBConn) Recycle() {
	switch {
	case dbc.pool == nil:
		dbc.Close()
	case dbc.conn.IsClosed():
		dbc.pool.Put(nil)
	default:
		dbc.pool.Put(dbc)
	}
}

// Taint unregister connection from original pool and taints the connection.
func (dbc *DBConn) Taint() {
	if dbc.pool == nil {
		return
	}
	dbc.pool.Put(nil)
	dbc.pool = nil
}

// Kill kills the currently executing query both on MySQL side
// and on the connection side. If no query is executing, it's a no-op.
// Kill will also not kill a query more than once.
func (dbc *DBConn) Kill(reason string, elapsed time.Duration) error {
	DbStats.KillQueriesCounter.Add(context.TODO(), 1)
	log.Infof("Due to %s, elapsed time: %v, killing query ID %v %s", reason, elapsed, dbc.conn.ID(), dbc.Current())

	// Client side action. Set error and close connection.
	dbc.errmu.Lock()
	dbc.err = fmt.Errorf("(errno 2013) due to %s, elapsed time: %v, killing query ID %v", reason, elapsed, dbc.conn.ID())
	dbc.errmu.Unlock()
	dbc.conn.Close()

	//Server side action. Kill the session.
	killConn, err := dbc.dbaPool.Get(context.TODO())
	if err != nil {
		log.Warnf("Failed to get conn from dba pool: %v", err)
		return err
	}
	defer killConn.Recycle()
	sql := fmt.Sprintf("kill %d", dbc.conn.ID())
	_, err = killConn.ExecuteFetch(sql, 10000, false)
	if err != nil {
		log.Errorf("Could not kill query ID %v %s: %v", dbc.conn.ID(),
			dbc.Current(), err)
		return err
	}
	return nil
}

// Current returns the currently executing query.
func (dbc *DBConn) Current() string {
	return dbc.current.Get()
}

// ID returns the connection id.
func (dbc *DBConn) ID() int64 {
	return dbc.conn.ID()
}

func (dbc *DBConn) reconnect(ctx context.Context) error {
	dbc.conn.Close()
	// Reuse MySQLTimings from dbc.conn.
	newConn, err := NewDBConnection(ctx, dbc.info)
	if err != nil {
		return err
	}
	dbc.errmu.Lock()
	dbc.err = nil
	dbc.errmu.Unlock()
	dbc.conn = newConn
	return nil
}

// setDeadline starts a goroutine that will kill the currently executing query
// if the deadline is exceeded. It returns a channel and a waitgroup. After the
// query is done executing, the caller is required to close the done channel
// and wait for the waitgroup to make sure that the necessary cleanup is done.
func (dbc *DBConn) setDeadline(ctx context.Context) (chan bool, *sync.WaitGroup) {
	if ctx.Done() == nil {
		return nil, nil
	}
	done := make(chan bool)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		startTime := time.Now()
		select {
		case <-ctx.Done():
			_ = dbc.Kill(ctx.Err().Error(), time.Since(startTime))
		case <-done:
			return
		}
		elapsed := time.Since(startTime)

		// Give 2x the elapsed time and some buffer as grace period
		// for the query to get killed.
		tmr2 := time.NewTimer(2*elapsed + 5*time.Second)
		defer tmr2.Stop()
		select {
		case <-tmr2.C:
			DbStats.KillQueriesCounter.Add(ctx, 1)
			log.Warnf("Query may be hung: %s", dbc.Current())
		case <-done:
			return
		}
		<-done
		log.Warnf("Hung query returned")
	}()
	return done, &wg
}