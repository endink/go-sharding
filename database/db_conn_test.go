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
	"errors"
	"fmt"
	"github.com/XiaoMi/Gaea/mysql/fakesqldb"
	"github.com/XiaoMi/Gaea/mysql/types"
	"reflect"
	"strings"
	"testing"
	"time"

	"context"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func compareTimingCounts(t *testing.T, op string, delta int64, before, after map[string]int64) {
	t.Helper()
	countBefore := before[op]
	countAfter := after[op]
	if countAfter-countBefore != delta {
		t.Errorf("Expected %s to increase by %d, got %d (%d => %d)", op, delta, countAfter-countBefore, countBefore, countAfter)
	}
}

//func TestDBConnExec(t *testing.T) {
//	db := fakesqldb.NewTxLimiter(t)
//	defer db.Close()
//
//	sql := "select * from test_table limit 1000"
//	expectedResult := &types.Result{
//		Fields: []*types.Field{
//			{Type: types.VarChar},
//		},
//		RowsAffected: 1,
//		Rows: [][]types.Value{
//			{types.NewVarChar("123")},
//		},
//	}
//	db.AddQuery(sql, expectedResult)
//	connPool := newPool()
//	mysqlTimings := DbStats.MySQLTimings
//	startCounts := mysqlTimings.Counts()
//	connPool.Open(db.ConnParams())
//	defer connPool.Close()
//	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(10*time.Second))
//	defer cancel()
//	dbConn, err := NewDBConn(context.Background(), connPool, db.ConnParams())
//	if dbConn != nil {
//		defer dbConn.Close()
//	}
//	if err != nil {
//		t.Fatalf("should not get an error, err: %v", err)
//	}
//	// Exec succeed, not asking for fields.
//	result, err := dbConn.Exec(ctx, sql, 1, false)
//	if err != nil {
//		t.Fatalf("should not get an error, err: %v", err)
//	}
//	expectedResult.Fields = nil
//	if !reflect.DeepEqual(expectedResult, result) {
//		t.Errorf("Exec: %v, want %v", expectedResult, result)
//	}
//
//	compareTimingCounts(t, "PoolTest.Exec", 1, startCounts, mysqlTimings.Counts())
//
//	startCounts = mysqlTimings.Counts()
//
//	// Exec fail due to client side error
//	db.AddRejectedQuery(sql, &mysql.SQLError{
//		Num:     2012,
//		Message: "connection fail",
//		Query:   "",
//	})
//	_, err = dbConn.Exec(ctx, sql, 1, false)
//	want := "connection fail"
//	if err == nil || !strings.Contains(err.Error(), want) {
//		t.Errorf("Exec: %v, want %s", err, want)
//	}
//
//	// The client side error triggers a retry in exec.
//	compareTimingCounts(t, "PoolTest.Exec", 2, startCounts, mysqlTimings.Counts())
//
//	startCounts = mysqlTimings.Counts()
//
//	// Set the connection fail flag and try again.
//	// This time the initial query fails as does the reconnect attempt.
//	db.EnableConnFail()
//	_, err = dbConn.Exec(ctx, sql, 1, false)
//	want = "packet read failed"
//	if err == nil || !strings.Contains(err.Error(), want) {
//		t.Errorf("Exec: %v, want %s", err, want)
//	}
//	db.DisableConnFail()
//
//	compareTimingCounts(t, "PoolTest.Exec", 1, startCounts, mysqlTimings.Counts())
//}

//func TestDBConnDeadline(t *testing.T) {
//	db := fakesqldb.NewTxLimiter(t)
//	defer db.Close()
//	sql := "select * from test_table limit 1000"
//	expectedResult := &types.Result{
//		Fields: []*types.Field{
//			{Type: types.VarChar},
//		},
//		RowsAffected: 1,
//		Rows: [][]types.Value{
//			{types.NewVarChar("123")},
//		},
//	}
//	db.AddQuery(sql, expectedResult)
//
//	connPool := newPool()
//	mysqlTimings := connPool.env.Stats().MySQLTimings
//	startCounts := mysqlTimings.Counts()
//	connPool.Open(db.ConnParams())
//	defer connPool.Close()
//
//	db.SetConnDelay(100 * time.Millisecond)
//	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(50*time.Millisecond))
//	defer cancel()
//
//	dbConn, err := NewDBConn(context.Background(), connPool, db.ConnParams())
//	if dbConn != nil {
//		defer dbConn.Close()
//	}
//	if err != nil {
//		t.Fatalf("should not get an error, err: %v", err)
//	}
//
//	_, err = dbConn.Exec(ctx, sql, 1, false)
//	want := "context deadline exceeded before execution started"
//	if err == nil || !strings.Contains(err.Error(), want) {
//		t.Errorf("Exec: %v, want %s", err, want)
//	}
//
//	compareTimingCounts(t, "PoolTest.Exec", 0, startCounts, mysqlTimings.Counts())
//
//	startCounts = mysqlTimings.Counts()
//
//	ctx, cancel = context.WithDeadline(context.Background(), time.Now().Add(10*time.Second))
//	defer cancel()
//
//	result, err := dbConn.Exec(ctx, sql, 1, false)
//	if err != nil {
//		t.Fatalf("should not get an error, err: %v", err)
//	}
//	expectedResult.Fields = nil
//	if !reflect.DeepEqual(expectedResult, result) {
//		t.Errorf("Exec: %v, want %v", expectedResult, result)
//	}
//
//	compareTimingCounts(t, "PoolTest.Exec", 1, startCounts, mysqlTimings.Counts())
//
//	startCounts = mysqlTimings.Counts()
//
//	// Test with just the Background context (with no deadline)
//	result, err = dbConn.Exec(context.Background(), sql, 1, false)
//	if err != nil {
//		t.Fatalf("should not get an error, err: %v", err)
//	}
//	expectedResult.Fields = nil
//	if !reflect.DeepEqual(expectedResult, result) {
//		t.Errorf("Exec: %v, want %v", expectedResult, result)
//	}
//
//	compareTimingCounts(t, "PoolTest.Exec", 1, startCounts, mysqlTimings.Counts())
//}

func TestDBConnKill(t *testing.T) {
	db := fakesqldb.New(t)
	defer db.Close()
	connPool := newPool()
	connPool.Open(db.ConnParams())
	defer connPool.Close()
	dbConn, err := NewDBConn(context.Background(), connPool, db.ConnParams())
	if dbConn != nil {
		defer dbConn.Close()
	}
	if err != nil {
		t.Fatalf("should not get an error, err: %v", err)
	}
	query := fmt.Sprintf("kill %d", dbConn.ID())
	db.AddQuery(query, &types.Result{})
	// Kill failed because we are not able to connect to the database
	db.EnableConnFail()
	err = dbConn.Kill("test kill", 0)
	want := "errno 2013"
	if err == nil || !strings.Contains(err.Error(), want) {
		t.Errorf("Exec: %v, want %s", err, want)
	}
	db.DisableConnFail()

	// Kill succeed
	err = dbConn.Kill("test kill", 0)
	if err != nil {
		t.Fatalf("kill should succeed, but got error: %v", err)
	}

	err = dbConn.reconnect(context.Background())
	if err != nil {
		t.Fatalf("reconnect should succeed, but got error: %v", err)
	}
	newKillQuery := fmt.Sprintf("kill %d", dbConn.ID())
	// Kill failed because "kill query_id" failed
	db.AddRejectedQuery(newKillQuery, errors.New("rejected"))
	err = dbConn.Kill("test kill", 0)
	want = "rejected"
	if err == nil || !strings.Contains(err.Error(), want) {
		t.Errorf("Exec: %v, want %s", err, want)
	}
}

// TestDBConnClose tests that an Exec returns immediately if a connection
// is asynchronously killed (and closed) in the middle of an execution.
func TestDBConnClose(t *testing.T) {
	db := fakesqldb.New(t)
	defer db.Close()
	connPool := newPool()
	connPool.Open(db.ConnParams())
	defer connPool.Close()
	dbConn, err := NewDBConn(context.Background(), connPool, db.ConnParams())
	require.NoError(t, err)
	defer dbConn.Close()

	query := "sleep"
	db.AddQuery(query, &types.Result{})
	db.SetBeforeFunc(query, func() {
		time.Sleep(100 * time.Millisecond)
	})

	start := time.Now()
	go func() {
		time.Sleep(10 * time.Millisecond)
		dbConn.Kill("test kill", 0)
	}()
	_, err = dbConn.Exec(context.Background(), query, 1, false)
	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), "(errno 2013) due to")
	assert.True(t, time.Since(start) < 100*time.Millisecond, "%v %v", time.Since(start), 100*time.Millisecond)
}

func TestDBNoPoolConnKill(t *testing.T) {
	db := fakesqldb.New(t)
	connPool := newPool()
	connPool.Open(db.ConnParams())
	defer connPool.Close()
	dbConn, err := NewDBConnNoPool(context.Background(), db.ConnParams(), connPool.dbaPool)
	if dbConn != nil {
		defer dbConn.Close()
	}
	if err != nil {
		t.Fatalf("should not get an error, err: %v", err)
	}
	query := fmt.Sprintf("kill %d", dbConn.ID())
	db.AddQuery(query, &types.Result{})
	// Kill failed because we are not able to connect to the database
	db.EnableConnFail()
	err = dbConn.Kill("test kill", 0)
	want := "errno 2013"
	if err == nil || !strings.Contains(err.Error(), want) {
		t.Errorf("Exec: %v, want %s", err, want)
	}
	db.DisableConnFail()

	// Kill succeed
	err = dbConn.Kill("test kill", 0)
	if err != nil {
		t.Fatalf("kill should succeed, but got error: %v", err)
	}

	err = dbConn.reconnect(context.Background())
	if err != nil {
		t.Fatalf("reconnect should succeed, but got error: %v", err)
	}
	newKillQuery := fmt.Sprintf("kill %d", dbConn.ID())
	// Kill failed because "kill query_id" failed
	db.AddRejectedQuery(newKillQuery, errors.New("rejected"))
	err = dbConn.Kill("test kill", 0)
	want = "rejected"
	if err == nil || !strings.Contains(err.Error(), want) {
		t.Errorf("Exec: %v, want %s", err, want)
	}
}

func TestDBConnStream(t *testing.T) {
	db := fakesqldb.New(t)
	defer db.Close()
	sql := "select * from test_table limit 1000"
	expectedResult := &types.Result{
		Fields: []*types.Field{
			{Type: types.VarChar},
		},
		Rows: [][]types.Value{
			{types.NewVarChar("123")},
		},
	}
	db.AddQuery(sql, expectedResult)
	connPool := newPool()
	connPool.Open(db.ConnParams())
	defer connPool.Close()
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(10*time.Second))
	defer cancel()
	dbConn, err := NewDBConn(context.Background(), connPool, db.ConnParams())
	if dbConn != nil {
		defer dbConn.Close()
	}
	if err != nil {
		t.Fatalf("should not get an error, err: %v", err)
	}
	var result types.Result
	err = dbConn.Stream(
		ctx, sql, func(r *types.Result) error {
			// Aggregate Fields and Rows
			if r.Fields != nil {
				result.Fields = r.Fields
			}
			if r.Rows != nil {
				result.Rows = append(result.Rows, r.Rows...)
			}
			return nil
		}, 10, types.IncludeFieldsAll)
	if err != nil {
		t.Fatalf("should not get an error, err: %v", err)
	}
	if !reflect.DeepEqual(expectedResult, &result) {
		t.Errorf("Exec: %v, want %v", expectedResult, &result)
	}
	// Stream fail
	db.Close()
	dbConn.Close()
	err = dbConn.Stream(
		ctx, sql, func(r *types.Result) error {
			return nil
		}, 10, types.IncludeFieldsAll)
	db.DisableConnFail()
	want := "refused"
	if err == nil || !strings.Contains(err.Error(), want) {
		t.Errorf("Error: '%v', must contain '%s'", err, want)
	}
}

func TestDBConnStreamKill(t *testing.T) {
	db := fakesqldb.New(t)
	defer db.Close()
	sql := "select * from test_table limit 1000"
	expectedResult := &types.Result{
		Fields: []*types.Field{
			{Type: types.VarChar},
		},
	}
	db.AddQuery(sql, expectedResult)
	connPool := newPool()
	connPool.Open(db.ConnParams())
	defer connPool.Close()
	dbConn, err := NewDBConn(context.Background(), connPool, db.ConnParams())
	require.NoError(t, err)
	defer dbConn.Close()

	go func() {
		time.Sleep(10 * time.Millisecond)
		dbConn.Kill("test kill", 0)
	}()

	err = dbConn.Stream(context.Background(), sql, func(r *types.Result) error {
		time.Sleep(100 * time.Millisecond)
		return nil
	}, 10, types.IncludeFieldsAll)

	assert.Contains(t, err.Error(), "(errno 2013) due to")
}
