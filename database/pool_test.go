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
	"github.com/XiaoMi/Gaea/mysql/fakesqldb"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"context"
)

func TestConnPoolGet(t *testing.T) {
	db := fakesqldb.New(t)
	defer db.Close()
	connPool := newPool()
	connPool.Open(db.ConnParams())
	defer connPool.Close()
	dbConn, err := connPool.Get(context.Background())
	if err != nil {
		t.Fatalf("should not get an error, but got: %v", err)
	}
	if dbConn == nil {
		t.Fatalf("db conn should not be nil")
	}
	// There is no context, it should not use appdebug connection
	if dbConn.pool == nil {
		t.Fatalf("db conn pool should not be nil")
	}
	dbConn.Recycle()
}

func TestConnPoolTimeout(t *testing.T) {
	db := fakesqldb.New(t)
	defer db.Close()
	connPool := NewPool("TestPool", ConnPoolConfig{
		Size:               1,
		TimeoutSeconds:     1,
		IdleTimeoutSeconds: 10,
	})
	connPool.Open(db.ConnParams())
	defer connPool.Close()
	dbConn, err := connPool.Get(context.Background())
	require.NoError(t, err)
	defer dbConn.Recycle()
	_, err = connPool.Get(context.Background())
	assert.EqualError(t, err, "resource pool timed out")
}

func TestConnPoolMaxWaiters(t *testing.T) {
	db := fakesqldb.New(t)
	defer db.Close()
	connPool := NewPool("TestPool", ConnPoolConfig{
		Size:       1,
		MaxWaiters: 1,
	})
	connPool.Open(db.ConnParams())
	defer connPool.Close()
	dbConn, err := connPool.Get(context.Background())
	require.NoError(t, err)

	// waiter 1
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		c1, err := connPool.Get(context.Background())
		if err != nil {
			t.Errorf("unexpected error: %v", err)
			return
		}
		c1.Recycle()
	}()
	// Wait for the first waiter to increment count.
	for {
		runtime.Gosched()
		if connPool.waiterCount.Get() == 1 {
			break
		}
	}

	// waiter 2
	_, err = connPool.Get(context.Background())
	assert.EqualError(t, err, "pool TestPool waiter count exceeded")

	// This recycle will make waiter1 succeed.
	dbConn.Recycle()
	wg.Wait()
}

func TestConnPoolGetEmptyDebugConfig(t *testing.T) {
	db := fakesqldb.New(t)
	defer db.Close()
	connPool := newPool()
	connPool.Open(db.ConnParams())
	defer connPool.Close()
	dbConn, err := connPool.Get(context.Background())
	if err != nil {
		t.Fatalf("should not get an error, but got: %v", err)
	}
	if dbConn == nil {
		t.Fatalf("db conn should not be nil")
	}
	// Context is empty, it should not use appdebug connection
	if dbConn.pool == nil {
		t.Fatalf("db conn pool should not be nil")
	}
	dbConn.Recycle()
}

func TestConnPoolGetAppDebug(t *testing.T) {
	db := fakesqldb.New(t)
	ctx := context.Background()

	defer db.Close()
	connPool := nonePool()
	connPool.Open(db.ConnParams())
	defer connPool.Close()
	dbConn, err := connPool.Get(ctx)
	assert.Nil(t, err, "should not get an error, but got: %v", err)
	assert.NotNil(t, dbConn, "db conn should not be nil")
	assert.Nil(t, dbConn.pool, "db conn pool should be nil for appDebug")
	dbConn.Recycle()
	assert.True(t, dbConn.IsClosed(), "db conn should be closed after recycle")
}

func TestConnPoolPutWhilePoolIsClosed(t *testing.T) {
	connPool := newPool()
	defer func() {
		if recover() == nil {
			t.Fatalf("pool is closed, should get an error")
		}
	}()
	connPool.Put(nil)
}

func TestConnPoolSetCapacity(t *testing.T) {
	db := fakesqldb.New(t)
	defer db.Close()
	connPool := newPool()
	connPool.Open(db.ConnParams())
	defer connPool.Close()
	err := connPool.SetCapacity(-10)
	if err == nil {
		t.Fatalf("set capacity should return error for negative capacity")
	}
	err = connPool.SetCapacity(10)
	if err != nil {
		t.Fatalf("set capacity should succeed")
	}
	if connPool.Capacity() != 10 {
		t.Fatalf("capacity should be 10")
	}
}

func TestConnPoolStatJSON(t *testing.T) {
	db := fakesqldb.New(t)
	defer db.Close()
	connPool := newPool()
	if connPool.StatsJSON() != "{}" {
		t.Fatalf("pool is closed, stats json should be empty: {}")
	}
	connPool.Open(db.ConnParams())
	defer connPool.Close()
	statsJSON := connPool.StatsJSON()
	if statsJSON == "" || statsJSON == "{}" {
		t.Fatalf("stats json should not be empty")
	}
}

func TestConnPoolStateWhilePoolIsClosed(t *testing.T) {
	connPool := newPool()
	if connPool.Capacity() != 0 {
		t.Fatalf("pool capacity should be 0 because it is still closed")
	}
	if connPool.Available() != 0 {
		t.Fatalf("pool available connections should be 0 because it is still closed")
	}
	if connPool.MaxCap() != 0 {
		t.Fatalf("pool max capacity should be 0 because it is still closed")
	}
	if connPool.WaitCount() != 0 {
		t.Fatalf("pool wait count should be 0 because it is still closed")
	}
	if connPool.WaitTime() != 0 {
		t.Fatalf("pool wait time should be 0 because it is still closed")
	}
	if connPool.IdleTimeout() != 0 {
		t.Fatalf("pool idle timeout should be 0 because it is still closed")
	}
}

func TestConnPoolStateWhilePoolIsOpen(t *testing.T) {
	db := fakesqldb.New(t)
	defer db.Close()
	idleTimeout := 10 * time.Second
	connPool := newPool()
	connPool.Open(db.ConnParams())
	defer connPool.Close()
	if connPool.Capacity() != 100 {
		t.Fatalf("pool capacity should be 100")
	}
	if connPool.MaxCap() != 100 {
		t.Fatalf("pool max capacity should be 100")
	}
	if connPool.WaitTime() != 0 {
		t.Fatalf("pool wait time should be 0")
	}
	if connPool.WaitCount() != 0 {
		t.Fatalf("pool wait count should be 0")
	}
	if connPool.IdleTimeout() != idleTimeout {
		t.Fatalf("pool idle timeout should be 0")
	}
	if connPool.Available() != 100 {
		t.Fatalf("pool available connections should be 100")
	}
	if connPool.Active() != 0 {
		t.Fatalf("pool active connections should be 0")
	}
	if connPool.InUse() != 0 {
		t.Fatalf("pool inUse connections should be 0")
	}
	dbConn, _ := connPool.Get(context.Background())
	if connPool.Available() != 99 {
		t.Fatalf("pool available connections should be 99")
	}
	if connPool.Active() != 1 {
		t.Fatalf("pool active connections should be 1")
	}
	if connPool.InUse() != 1 {
		t.Fatalf("pool inUse connections should be 1")
	}

	dbConn.Recycle()
	if connPool.Available() != 100 {
		t.Fatalf("pool available connections should be 100")
	}
	if connPool.Active() != 1 {
		t.Fatalf("pool active connections should be 1")
	}
	if connPool.InUse() != 0 {
		t.Fatalf("pool inUse connections should be 0")
	}
}

func nonePool() *Pool {
	return NewPool("TestPool", ConnPoolConfig{
		Size:               100,
		IdleTimeoutSeconds: 10,
		IsNoPool:           true,
	})
}

func newPool() *Pool {
	return NewPool("TestPool", ConnPoolConfig{
		Size:               100,
		IdleTimeoutSeconds: 10,
	})
}