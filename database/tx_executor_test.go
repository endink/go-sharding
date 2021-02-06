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
	"errors"
	"fmt"
	"github.com/XiaoMi/Gaea/mysql/fakesqldb"
	"github.com/XiaoMi/Gaea/mysql/types"
	"github.com/XiaoMi/Gaea/util"
	"reflect"
	"testing"
	"time"

	"context"

	"github.com/stretchr/testify/require"
)

func TestTxExecutorEmptyPrepare(t *testing.T) {
	txe, db := newTestTxExecutor(t)
	defer db.Close()
	txid := newTransaction(txe, nil)
	err := txe.Prepare(context.TODO(), txid, "aa")
	require.NoError(t, err)
	// Nothing should be prepared.
	require.Empty(t, txe.te.preparedPool.conns, "txe.te.preparedPool.conns")
}

func TestTxExecutorPrepare(t *testing.T) {
	txe, db := newTestTxExecutor(t)
	defer db.Close()
	txid := newTxForPrep(txe)
	err := txe.Prepare(context.TODO(), txid, "aa")
	require.NoError(t, err)
	err = txe.RollbackPrepared(context.TODO(), "aa", 1)
	require.NoError(t, err)
	// A retry should still succeed.
	err = txe.RollbackPrepared(context.TODO(), "aa", 1)
	require.NoError(t, err)
	// A retry  with no original id should also succeed.
	err = txe.RollbackPrepared(context.TODO(), "aa", 0)
	require.NoError(t, err)
}

func TestTxExecutorPrepareNotInTx(t *testing.T) {
	txe, db := newTestTxExecutor(t)
	defer db.Close()
	err := txe.Prepare(context.TODO(), 0, "aa")
	require.EqualError(t, err, "transaction 0: not found")
}

func TestTxExecutorPreparePoolFail(t *testing.T) {
	txe, db := newTestTxExecutor(t)
	defer db.Close()
	txid1 := newTxForPrep(txe)
	txid2 := newTxForPrep(txe)
	err := txe.Prepare(context.TODO(), txid1, "aa")
	require.NoError(t, err)
	defer txe.RollbackPrepared(context.TODO(), "aa", 0)
	err = txe.Prepare(context.TODO(), txid2, "bb")
	require.Error(t, err)
	require.Contains(t, err.Error(), "prepared transactions exceeded limit")
}

func TestTxExecutorPrepareRedoBeginFail(t *testing.T) {
	txe, db := newTestTxExecutor(t)
	defer db.Close()
	txid := newTxForPrep(txe)
	db.AddRejectedQuery("begin", errors.New("begin fail"))
	err := txe.Prepare(context.TODO(), txid, "aa")
	defer txe.RollbackPrepared(context.TODO(), "aa", 0)
	require.Error(t, err)
	require.Contains(t, err.Error(), "begin fail")
}

func TestTxExecutorPrepareRedoFail(t *testing.T) {
	txe, db := newTestTxExecutor(t)
	defer db.Close()
	txid := newTxForPrep(txe)
	err := txe.Prepare(context.TODO(), txid, "bb")
	defer txe.RollbackPrepared(context.TODO(), "bb", 0)
	require.Error(t, err)
	require.Contains(t, err.Error(), "is not supported")
}

func TestTxExecutorPrepareRedoCommitFail(t *testing.T) {
	txe, db := newTestTxExecutor(t)
	defer db.Close()
	txid := newTxForPrep(txe)
	db.AddRejectedQuery("commit", errors.New("commit fail"))
	err := txe.Prepare(context.TODO(), txid, "aa")
	defer txe.RollbackPrepared(context.TODO(), "aa", 0)
	require.Error(t, err)
	require.Contains(t, err.Error(), "commit fail")
}

func TestTxExecutorCommit(t *testing.T) {
	txe, db := newTestTxExecutor(t)
	defer db.Close()
	txid := newTxForPrep(txe)
	err := txe.Prepare(context.TODO(), txid, "aa")
	require.NoError(t, err)
	err = txe.CommitPrepared(context.TODO(), "aa")
	require.NoError(t, err)
	// Committing an absent transaction should succeed.
	err = txe.CommitPrepared(context.TODO(), "bb")
	require.NoError(t, err)
}

func TestTxExecutorCommitRedoFail(t *testing.T) {
	txe, db := newTestTxExecutor(t)
	defer db.Close()
	txid := newTxForPrep(txe)
	// Allow all additions to redo logs to succeed
	db.AddQueryPattern(fmt.Sprintf("insert into %s\\.redo_state.*", TwopcDbName), &types.Result{})
	err := txe.Prepare(context.TODO(), txid, "bb")
	require.NoError(t, err)
	defer txe.RollbackPrepared(context.TODO(), "bb", 0)
	db.AddQuery(fmt.Sprintf("update %s.redo_state set state = 'Failed' where dtid = 'bb'", TwopcDbName), &types.Result{})
	err = txe.CommitPrepared(context.TODO(), "bb")
	require.Error(t, err)
	require.Contains(t, err.Error(), "is not supported")
	// A retry should fail differently.
	err = txe.CommitPrepared(context.TODO(), "bb")
	require.Error(t, err)
	require.Contains(t, err.Error(), "cannot commit dtid bb, state: failed")
}

func TestTxExecutorCommitRedoCommitFail(t *testing.T) {
	txe, db := newTestTxExecutor(t)
	defer db.Close()
	txid := newTxForPrep(txe)
	err := txe.Prepare(context.TODO(), txid, "aa")
	require.NoError(t, err)
	defer txe.RollbackPrepared(context.TODO(), "aa", 0)
	db.AddRejectedQuery("commit", errors.New("commit fail"))
	err = txe.CommitPrepared(context.TODO(), "aa")
	require.Error(t, err)
	require.Contains(t, err.Error(), "commit fail")
}

func TestTxExecutorRollbackBeginFail(t *testing.T) {
	txe, db := newTestTxExecutor(t)
	defer db.Close()
	txid := newTxForPrep(txe)
	err := txe.Prepare(context.TODO(), txid, "aa")
	require.NoError(t, err)
	db.AddRejectedQuery("begin", errors.New("begin fail"))
	err = txe.RollbackPrepared(context.TODO(), "aa", txid)
	require.Error(t, err)
	require.Contains(t, err.Error(), "begin fail")
}

func TestTxExecutorRollbackRedoFail(t *testing.T) {
	txe, db := newTestTxExecutor(t)
	defer db.Close()
	txid := newTxForPrep(txe)
	// Allow all additions to redo logs to succeed
	db.AddQueryPattern(fmt.Sprintf("insert into %s\\.redo_state.*", TwopcDbName), &types.Result{})
	err := txe.Prepare(context.TODO(), txid, "bb")
	require.NoError(t, err)
	err = txe.RollbackPrepared(context.TODO(), "bb", txid)
	require.Error(t, err)
	require.Contains(t, err.Error(), "is not supported")
}

func TestExecutorCreateTransaction(t *testing.T) {
	txe, db := newTestTxExecutor(t)
	defer db.Close()

	db.AddQueryPattern(fmt.Sprintf("insert into %s\\.dt_state\\(dtid, state, time_created\\) values \\('aa', %d,.*", TwopcDbName, int(TransactionStatePrepare)), &types.Result{})
	db.AddQueryPattern(fmt.Sprintf("insert into %s\\.dt_participant\\(dtid, id, keyspace, shard\\) values \\('aa', 1,.*", TwopcDbName), &types.Result{})
	err := txe.CreateTransaction(context.TODO(), "aa", []*Target{{
		Schema:     "t1",
		DataSource: "0",
	}})
	require.NoError(t, err)
}

func TestExecutorStartCommit(t *testing.T) {
	txe, db := newTestTxExecutor(t)
	defer db.Close()

	commitTransition := fmt.Sprintf("update %s.dt_state set state = %d where dtid = 'aa' and state = %d", TwopcDbName, int(TransactionStateCommit), int(TransactionStatePrepare))
	db.AddQuery(commitTransition, &types.Result{RowsAffected: 1})
	txid := newTxForPrep(txe)
	err := txe.StartCommit(context.TODO(), txid, "aa")
	require.NoError(t, err)

	db.AddQuery(commitTransition, &types.Result{})
	txid = newTxForPrep(txe)
	err = txe.StartCommit(context.TODO(), txid, "aa")
	require.Error(t, err)
	require.Contains(t, err.Error(), "could not transition to COMMIT: aa")
}

func TestExecutorSetRollback(t *testing.T) {
	txe, db := newTestTxExecutor(t)
	defer db.Close()

	rollbackTransition := fmt.Sprintf("update %s.dt_state set state = %d where dtid = 'aa' and state = %d",
		TwopcDbName,
		int(TransactionStateRollback), int(TransactionStatePrepare))
	db.AddQuery(rollbackTransition, &types.Result{RowsAffected: 1})
	txid := newTxForPrep(txe)
	err := txe.SetRollback(context.TODO(), "aa", txid)
	require.NoError(t, err)

	db.AddQuery(rollbackTransition, &types.Result{})
	txid = newTxForPrep(txe)
	err = txe.SetRollback(context.TODO(), "aa", txid)
	require.Error(t, err)
	require.Contains(t, err.Error(), "could not transition to ROLLBACK: aa")
}

func TestExecutorConcludeTransaction(t *testing.T) {
	txe, db := newTestTxExecutor(t)
	defer db.Close()

	db.AddQuery(fmt.Sprintf("delete from %s.dt_state where dtid = 'aa'", TwopcDbName), &types.Result{})
	db.AddQuery(fmt.Sprintf("delete from %s.dt_participant where dtid = 'aa'", TwopcDbName), &types.Result{})
	err := txe.ConcludeTransaction(context.TODO(), "aa")
	require.NoError(t, err)
}

func TestExecutorReadTransaction(t *testing.T) {
	txe, db := newTestTxExecutor(t)
	defer db.Close()

	db.AddQuery(fmt.Sprintf("select dtid, state, time_created from %s.dt_state where dtid = 'aa'", TwopcDbName), &types.Result{})
	got, err := txe.ReadTransaction(context.TODO(), "aa")
	require.NoError(t, err)
	want := &TransactionMetadata{}
	if !util.JsonEqual(got, want) {
		t.Errorf("ReadTransaction: %v, want %v", got, want)
	}

	txResult := &types.Result{
		Fields: []*types.Field{
			{Type: types.VarChar},
			{Type: types.Int64},
			{Type: types.Int64},
		},
		Rows: [][]types.Value{{
			types.NewVarBinary("aa"),
			types.NewInt64(int64(TransactionStatePrepare)),
			types.NewVarBinary("1"),
		}},
	}
	db.AddQuery(fmt.Sprintf("select dtid, state, time_created from %s.dt_state where dtid = 'aa'", TwopcDbName), txResult)
	db.AddQuery(fmt.Sprintf("select keyspace, shard from %s.dt_participant where dtid = 'aa'", TwopcDbName), &types.Result{
		Fields: []*types.Field{
			{Type: types.VarChar},
			{Type: types.VarChar},
		},
		Rows: [][]types.Value{{
			types.NewVarBinary("test1"),
			types.NewVarBinary("0"),
		}, {
			types.NewVarBinary("test2"),
			types.NewVarBinary("1"),
		}},
	})
	got, err = txe.ReadTransaction(context.TODO(), "aa")
	require.NoError(t, err)
	want = &TransactionMetadata{
		Dtid:        "aa",
		State:       TransactionStatePrepare,
		TimeCreated: 1,
		Participants: []*Target{{
			Schema:     "test1",
			DataSource: "0",
			TabletType: TabletTypeMaster,
		}, {
			Schema:     "test2",
			DataSource: "1",
			TabletType: TabletTypeMaster,
		}},
	}
	if !util.JsonEqual(got, want) {
		t.Errorf("ReadTransaction: %v, want %v", got, want)
	}

	txResult = &types.Result{
		Fields: []*types.Field{
			{Type: types.VarChar},
			{Type: types.Int64},
			{Type: types.Int64},
		},
		Rows: [][]types.Value{{
			types.NewVarBinary("aa"),
			types.NewInt64(int64(TransactionStateCommit)),
			types.NewVarBinary("1"),
		}},
	}
	db.AddQuery(fmt.Sprintf("select dtid, state, time_created from %s.dt_state where dtid = 'aa'", TwopcDbName), txResult)
	want.State = TransactionStateCommit
	got, err = txe.ReadTransaction(context.TODO(), "aa")
	require.NoError(t, err)
	if !util.JsonEqual(got, want) {
		t.Errorf("ReadTransaction: %v, want %v", got, want)
	}

	txResult = &types.Result{
		Fields: []*types.Field{
			{Type: types.VarChar},
			{Type: types.Int64},
			{Type: types.Int64},
		},
		Rows: [][]types.Value{{
			types.NewVarBinary("aa"),
			types.NewInt64(int64(TransactionStateRollback)),
			types.NewVarBinary("1"),
		}},
	}
	db.AddQuery(fmt.Sprintf("select dtid, state, time_created from %s.dt_state where dtid = 'aa'", TwopcDbName), txResult)
	want.State = TransactionStateRollback
	got, err = txe.ReadTransaction(context.TODO(), "aa")
	require.NoError(t, err)
	if !util.JsonEqual(got, want) {
		t.Errorf("ReadTransaction: %v, want %v", got, want)
	}
}

func TestExecutorReadAllTransactions(t *testing.T) {
	txe, db := newTestTxExecutor(t)
	defer db.Close()

	db.AddQuery(txe.te.twoPC.readAllTransactions, &types.Result{
		Fields: []*types.Field{
			{Type: types.VarChar},
			{Type: types.Int64},
			{Type: types.Int64},
			{Type: types.VarChar},
			{Type: types.VarChar},
		},
		Rows: [][]types.Value{{
			types.NewVarBinary("dtid0"),
			types.NewInt64(int64(TransactionStatePrepare)),
			types.NewVarBinary("1"),
			types.NewVarBinary("ks01"),
			types.NewVarBinary("shard01"),
		}},
	})
	got, _, _, err := txe.ReadTwopcInflight(context.TODO())
	require.NoError(t, err)
	want := []*DistributedTx{{
		Dtid:    "dtid0",
		State:   "PREPARE",
		Created: time.Unix(0, 1),
		Participants: []Target{{
			Schema:     "ks01",
			DataSource: "shard01",
		}},
	}}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("ReadAllTransactions:\n%s, want\n%s", jsonStr(got), jsonStr(want))
	}
}

func TestExecutorResolveTransaction(t *testing.T) {
	_, db := newTestTxExecutor(t)
	defer db.Close()
	want := "aa"
	db.AddQueryPattern(
		fmt.Sprintf("select dtid, time_created from %s\\.dt_state where time_created.*", TwopcDbName),
		&types.Result{
			Fields: []*types.Field{
				{Type: types.VarChar},
				{Type: types.Int64},
			},
			Rows: [][]types.Value{{
				types.NewVarBinary(want),
				types.NewVarBinary("1"),
			}},
		})
	got := <-dtidCh
	if got != want {
		t.Errorf("ResolveTransaction: %s, want %s", got, want)
	}
}

func TestNoTwopc(t *testing.T) {
	txe, db := newNoTwopcExecutor(t)
	defer db.Close()

	c := context.TODO()
	testcases := []struct {
		desc string
		fun  func() error
	}{{
		desc: "Prepare",
		fun:  func() error { return txe.Prepare(c, 1, "aa") },
	}, {
		desc: "CommitPrepared",
		fun:  func() error { return txe.CommitPrepared(c, "aa") },
	}, {
		desc: "RollbackPrepared",
		fun:  func() error { return txe.RollbackPrepared(c, "aa", 1) },
	}, {
		desc: "CreateTransaction",
		fun:  func() error { return txe.CreateTransaction(c, "aa", nil) },
	}, {
		desc: "StartCommit",
		fun:  func() error { return txe.StartCommit(c, 1, "aa") },
	}, {
		desc: "SetRollback",
		fun:  func() error { return txe.SetRollback(c, "aa", 1) },
	}, {
		desc: "ConcludeTransaction",
		fun:  func() error { return txe.ConcludeTransaction(c, "aa") },
	}, {
		desc: "ReadTransaction",
		fun: func() error {
			_, err := txe.ReadTransaction(c, "aa")
			return err
		},
	}, {
		desc: "ReadAllTransactions",
		fun: func() error {
			_, _, _, err := txe.ReadTwopcInflight(c)
			return err
		},
	}}

	want := "2pc is not enabled"
	for _, tc := range testcases {
		err := tc.fun()
		require.EqualError(t, err, want)
	}
}

var TestUpdateSql = "update test_table set `name` = 2 where pk = 1"

func newTestTxExecutor(t *testing.T) (txe *TxExecutor, db *fakesqldb.DB) {
	db = setUpQueryExecutorTest(t)
	te := newTestTxEngine(db, smallTxPool)
	db.AddQueryPattern(fmt.Sprintf("insert into %s\\.redo_state\\(dtid, state, time_created\\) values \\('aa', 1,.*", TwopcDbName), &types.Result{})
	db.AddQueryPattern(fmt.Sprintf("insert into %s\\.redo_statement.*", TwopcDbName), &types.Result{})
	db.AddQuery(fmt.Sprintf("delete from %s.redo_state where dtid = 'aa'", TwopcDbName), &types.Result{})
	db.AddQuery(fmt.Sprintf("delete from %s.redo_statement where dtid = 'aa'", TwopcDbName), &types.Result{})
	db.AddQuery(TestUpdateSql, &types.Result{})
	return &TxExecutor{
		te: te,
	}, db
}

// newNoTwopcExecutor is same as newTestTxExecutor, but 2pc disabled.
func newNoTwopcExecutor(t *testing.T) (txe *TxExecutor, db *fakesqldb.DB) {
	db = setUpQueryExecutorTest(t)
	te := newTestTxEngine(db, noTwopc)
	return &TxExecutor{
		te: te,
	}, db
}

// newTxForPrep creates a non-empty transaction.
func newTxForPrep(txe *TxExecutor) int64 {
	txid := newTransaction(txe, nil)
	conn, err := txe.te.txPool.GetAndLock(txid, "for exec")
	if err != nil {
		panic(err)
	}
	defer conn.Unlock()

	_, err = conn.Exec(context.TODO(), TestUpdateSql, 1, false)
	if err != nil {
		panic(err)
	}
	conn.TxProperties().RecordQuery(TestUpdateSql)
	return txid
}

func newTransaction(txe *TxExecutor, options *types.ExecuteOptions) int64 {
	if options == nil {
		options = &types.ExecuteOptions{}
	}
	transactionID, _, err := txe.te.Begin(context.Background(), nil, 0, options)
	if err != nil {
		panic(util.Wrap(err, "failed to start a transaction"))
	}
	return transactionID
}
