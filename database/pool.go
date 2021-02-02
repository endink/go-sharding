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
	"github.com/XiaoMi/Gaea/telemetry"
	"github.com/XiaoMi/Gaea/util"
	"github.com/XiaoMi/Gaea/util/sync2"
	"go.opentelemetry.io/otel/label"
	"sync"
	"time"

	"context"
)

// Pool implements a custom connection pool for tabletserver.
// It's similar to dbconnpool.ConnPool, but the connections it creates
// come with built-in ability to kill in-flight queries. These connections
// also trigger a CheckMySQL call if we fail to connect to MySQL.
// Other than the connection type, ConnPool maintains an additional
// pool of dba connections that are used to kill connections.
type Pool struct {
	name               string
	mu                 sync.Mutex
	connections        *util.ResourcePool
	capacity           int
	prefillParallelism int
	timeout            time.Duration
	idleTimeout        time.Duration
	waiterCap          int64
	waiterCount        sync2.AtomicInt64
	isNoPool           bool
	connParam          *mysql.ConnParams
	dbaPool            *ConnectionPool
}

// NewPool creates a new Pool. The name is used
// to publish stats only.
func NewPool(name string, cfg ConnPoolConfig) *Pool {
	idleTimeout := time.Duration(cfg.IdleTimeoutSeconds) * time.Second
	cp := &Pool{
		name:               name,
		capacity:           cfg.Size,
		prefillParallelism: cfg.PrefillParallelism,
		timeout:            time.Duration(cfg.TimeoutSeconds) * time.Second,
		idleTimeout:        idleTimeout,
		waiterCap:          int64(cfg.MaxWaiters),
		dbaPool:            NewConnectionPool("", 1, idleTimeout, 0),
		isNoPool:           cfg.IsNoPool,
	}
	if name == "" {
		return cp
	}
	meter := telemetry.GetMeter("db.pool")

	meter.NewInt64Observer(telemetry.BuildMetricName(name, "Capacity"), "Tablet server conn pool capacity", cp.Capacity)
	meter.NewInt64Observer(telemetry.BuildMetricName(name, "Available"), "Tablet server conn pool available", cp.Available)
	meter.NewInt64Observer(telemetry.BuildMetricName(name, "Active"), "Tablet server conn pool active", cp.Active)
	meter.NewInt64Observer(telemetry.BuildMetricName(name, "InUse"), "Tablet server conn pool in use", cp.InUse)
	meter.NewInt64Observer(telemetry.BuildMetricName(name, "MaxCap"), "Tablet server conn pool max cap", cp.MaxCap)
	meter.NewInt64SumObserver(telemetry.BuildMetricName(name, "WaitCount"), "Tablet server conn pool wait count", cp.WaitCount)
	meter.NewDurationSumObserver(telemetry.BuildMetricName(name, "WaitTime"), "Tablet server wait time", cp.WaitTime)
	meter.NewDurationObserver(telemetry.BuildMetricName(name, "IdleTimeout"), "Tablet server idle timeout", cp.IdleTimeout)
	meter.NewInt64SumObserver(telemetry.BuildMetricName(name, "IdleClosed"), "Tablet server conn pool idle closed", cp.IdleClosed)
	meter.NewInt64SumObserver(telemetry.BuildMetricName(name, "Exhausted"), "Number of times pool had zero available slots", cp.Exhausted)
	return cp
}

func (cp *Pool) pool() (p *util.ResourcePool) {
	cp.mu.Lock()
	p = cp.connections
	cp.mu.Unlock()
	return p
}

// Open must be called before starting to use the pool.
func (cp *Pool) Open(connParam *mysql.ConnParams) {
	cp.mu.Lock()
	defer cp.mu.Unlock()

	if cp.prefillParallelism != 0 {
		log.Infof("Opening pool: '%s'", cp.name)
		defer log.Infof("Done opening pool: '%s'", cp.name)
	}

	f := func(ctx context.Context) (util.Resource, error) {
		return NewDBConn(ctx, cp, connParam)
	}
	cp.connections = util.NewResourcePool(f, cp.capacity, cp.capacity, cp.idleTimeout, cp.prefillParallelism, cp.getLogWaitCallback())
	if cp.isNoPool {
		cp.connParam = connParam
	}
	cp.dbaPool.Open(connParam)
}

func (cp *Pool) getLogWaitCallback() func(context.Context, time.Time) {
	if cp.name == "" {
		return func(ctx context.Context, start time.Time) {} // no op
	}
	return func(ctx context.Context, start time.Time) {
		DbStats.ResourceWaitTime.RecordLatency(ctx, cp.name, start)
	}
}

// Close will close the pool and wait for connections to be returned before
// exiting.
func (cp *Pool) Close() {
	p := cp.pool()
	if p == nil {
		return
	}
	// We should not hold the lock while calling Close
	// because it waits for connections to be returned.
	p.Close()
	cp.mu.Lock()
	cp.connections = nil
	cp.mu.Unlock()

	cp.dbaPool.Close()
}

// Get returns a connection.
// You must call Recycle on DBConn once done.
func (cp *Pool) Get(ctx context.Context) (*DBConn, error) {
	ctx, span := telemetry.GlobalTracer.Start(ctx, "Pool.Get")
	defer span.End()

	if cp.waiterCap > 0 {
		waiterCount := cp.waiterCount.Add(1)
		defer cp.waiterCount.Add(-1)
		if waiterCount > cp.waiterCap {
			return nil, fmt.Errorf("pool %s waiter count exceeded", cp.name)
		}
	}

	if cp.isNoPool {
		return NewDBConnNoPool(ctx, cp.connParam, cp.dbaPool)
	}
	p := cp.pool()
	if p == nil {
		return nil, ErrConnPoolClosed
	}
	span.SetAttributes(label.Int64("capacity", p.Capacity()))
	span.SetAttributes(label.Int64("in_use", p.InUse()))
	span.SetAttributes(label.Int64("available", p.Available()))
	span.SetAttributes(label.Int64("active", p.Active()))

	if cp.timeout != 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, cp.timeout)
		defer cancel()
	}
	r, err := p.Get(ctx)
	if err != nil {
		return nil, err
	}
	return r.(*DBConn), nil
}

// Put puts a connection into the pool.
func (cp *Pool) Put(conn *DBConn) {
	p := cp.pool()
	if p == nil {
		panic(ErrConnPoolClosed)
	}
	if conn == nil {
		p.Put(nil)
	} else {
		p.Put(conn)
	}
}

// SetCapacity alters the size of the pool at runtime.
func (cp *Pool) SetCapacity(capacity int) (err error) {
	cp.mu.Lock()
	defer cp.mu.Unlock()
	if cp.connections != nil {
		err = cp.connections.SetCapacity(capacity)
		if err != nil {
			return err
		}
	}
	cp.capacity = capacity
	return nil
}

// SetIdleTimeout sets the idleTimeout on the pool.
func (cp *Pool) SetIdleTimeout(idleTimeout time.Duration) {
	cp.mu.Lock()
	defer cp.mu.Unlock()
	if cp.connections != nil {
		cp.connections.SetIdleTimeout(idleTimeout)
	}
	cp.idleTimeout = idleTimeout
	cp.dbaPool.SetIdleTimeout(idleTimeout)
}

// StatsJSON returns the pool stats as a JSON object.
func (cp *Pool) StatsJSON() string {
	p := cp.pool()
	if p == nil {
		return "{}"
	}
	return p.StatsJSON()
}

// Capacity returns the pool capacity.
func (cp *Pool) Capacity() int64 {
	p := cp.pool()
	if p == nil {
		return 0
	}
	return p.Capacity()
}

// Available returns the number of available connections in the pool
func (cp *Pool) Available() int64 {
	p := cp.pool()
	if p == nil {
		return 0
	}
	return p.Available()
}

// Active returns the number of active connections in the pool
func (cp *Pool) Active() int64 {
	p := cp.pool()
	if p == nil {
		return 0
	}
	return p.Active()
}

// InUse returns the number of in-use connections in the pool
func (cp *Pool) InUse() int64 {
	p := cp.pool()
	if p == nil {
		return 0
	}
	return p.InUse()
}

// MaxCap returns the maximum size of the pool
func (cp *Pool) MaxCap() int64 {
	p := cp.pool()
	if p == nil {
		return 0
	}
	return p.MaxCap()
}

// WaitCount returns how many clients are waiting for a connection
func (cp *Pool) WaitCount() int64 {
	p := cp.pool()
	if p == nil {
		return 0
	}
	return p.WaitCount()
}

// WaitTime return the pool WaitTime.
func (cp *Pool) WaitTime() time.Duration {
	p := cp.pool()
	if p == nil {
		return 0
	}
	return p.WaitTime()
}

// IdleTimeout returns the idle timeout for the pool.
func (cp *Pool) IdleTimeout() time.Duration {
	p := cp.pool()
	if p == nil {
		return 0
	}
	return p.IdleTimeout()
}

// IdleClosed returns the number of closed connections for the pool.
func (cp *Pool) IdleClosed() int64 {
	p := cp.pool()
	if p == nil {
		return 0
	}
	return p.IdleClosed()
}

// Exhausted returns the number of times available went to zero for the pool.
func (cp *Pool) Exhausted() int64 {
	p := cp.pool()
	if p == nil {
		return 0
	}
	return p.Exhausted()
}
