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
	"fmt"
	"github.com/XiaoMi/Gaea/mysql"
	"github.com/XiaoMi/Gaea/mysql/fakesqldb"
	"github.com/XiaoMi/Gaea/mysql/types"
	"testing"
	"time"
)

//func newTestQueryExecutor(ctx context.Context, tsv *TabletServer, sql string, txID int64) *QueryExecutor {
//	logStats := tabletenv.NewLogStats(ctx, "TestQueryExecutor")
//	plan, err := tsv.qe.GetPlan(ctx, logStats, sql, false, false /* inReservedConn */)
//	if err != nil {
//		panic(err)
//	}
//	return &QueryExecutor{
//		ctx:      ctx,
//		query:    sql,
//		bindVars: make(map[string]*types.BindVariable),
//		connID:   txID,
//		plan:     plan,
//		logStats: logStats,
//		tsv:      tsv,
//	}
//}

type executorFlags int64

const (
	noFlags              executorFlags = 0
	enableStrictTableACL               = 1 << iota
	smallTxPool
	noTwopc
	shortTwopcAge
	smallResultSize
)

func setUpQueryExecutorTest(t *testing.T) *fakesqldb.DB {
	db := fakesqldb.New(t)
	initQueryExecutorTestDB(db, true)
	return db
}

func newTestTxEngine(db *fakesqldb.DB, flags executorFlags) *TxEngine {
	dbCfg := newTestDbConfig()

	if flags&smallTxPool > 0 {
		dbCfg.Tx.Pool.Size = 3
	} else {
		dbCfg.Tx.Pool.Size = 100
	}

	if flags&shortTwopcAge > 0 {
		dbCfg.Tx.TwoPCAbandonAge = time.Second * 1
	} else {
		dbCfg.Tx.TwoPCAbandonAge = time.Second * 10
	}

	if flags&noTwopc > 0 {
		dbCfg.Tx.EnableTwoPC = false
	} else {
		dbCfg.Tx.EnableTwoPC = true
	}

	dbCfg.Tx.Pool.Size = 3
	te := NewTxEngineWithCoord(db.ConnParams(), *dbCfg, FakeCoordinator{})
	te.AcceptReadWrite()
	return te
}

func initQueryExecutorTestDB(db *fakesqldb.DB, testTableHasMultipleUniqueKeys bool) {
	for query, result := range getQueryExecutorSupportedQueries(testTableHasMultipleUniqueKeys) {
		db.AddQuery(query, result)
	}
}

func getTestTableFields() []*types.Field {
	return []*types.Field{
		{Name: "pk", Type: types.Int32},
		{Name: "name", Type: types.Int32},
		{Name: "addr", Type: types.Int32},
	}
}

func getQueryExecutorSupportedQueries(testTableHasMultipleUniqueKeys bool) map[string]*types.Result {
	return map[string]*types.Result{
		// queries for twopc
		fmt.Sprintf(sqlCreateSidecarDB, TwopcDbName):          {},
		fmt.Sprintf(sqlDropLegacy1, TwopcDbName):              {},
		fmt.Sprintf(sqlDropLegacy2, TwopcDbName):              {},
		fmt.Sprintf(sqlDropLegacy3, TwopcDbName):              {},
		fmt.Sprintf(sqlDropLegacy4, TwopcDbName):              {},
		fmt.Sprintf(sqlCreateTableRedoState, TwopcDbName):     {},
		fmt.Sprintf(sqlCreateTableRedoStatement, TwopcDbName): {},
		fmt.Sprintf(sqlCreateTableDTState, TwopcDbName):       {},
		fmt.Sprintf(sqlCreateTableDTParticipant, TwopcDbName): {},
		// queries for schema info
		"select unix_timestamp()": {
			Fields: []*types.Field{{
				Type: types.Uint64,
			}},
			Rows: [][]types.Value{
				{types.NewInt32(1427325875)},
			},
			RowsAffected: 1,
		},
		"select @@global.sql_mode": {
			Fields: []*types.Field{{
				Type: types.VarChar,
			}},
			Rows: [][]types.Value{
				{types.NewVarBinary("STRICT_TRANS_TABLES")},
			},
			RowsAffected: 1,
		},
		"select @@autocommit": {
			Fields: []*types.Field{{
				Type: types.Uint64,
			}},
			Rows: [][]types.Value{
				{types.NewVarBinary("1")},
			},
			RowsAffected: 1,
		},
		"select @@sql_auto_is_null": {
			Fields: []*types.Field{{
				Type: types.Uint64,
			}},
			Rows: [][]types.Value{
				{types.NewVarBinary("0")},
			},
			RowsAffected: 1,
		},
		"select @@version_comment from dual where 1 != 1": {
			Fields: []*types.Field{{
				Type: types.VarChar,
			}},
		},
		"select @@version_comment from dual limit 1": {
			Fields: []*types.Field{{
				Type: types.VarChar,
			}},
			Rows: [][]types.Value{
				{types.NewVarBinary("fakedb server")},
			},
			RowsAffected: 1,
		},
		"(select 0 as x from dual where 1 != 1) union (select 1 as y from dual where 1 != 1)": {
			Fields: []*types.Field{{
				Type: types.Uint64,
			}},
			Rows:         [][]types.Value{},
			RowsAffected: 0,
		},
		"(select 0 as x from dual where 1 != 1) union (select 1 as y from dual where 1 != 1) limit 10001": {
			Fields: []*types.Field{{
				Type: types.Uint64,
			}},
			Rows:         [][]types.Value{},
			RowsAffected: 0,
		},
		mysql.BaseShowTables: {
			Fields: mysql.BaseShowTablesFields,
			Rows: [][]types.Value{
				mysql.BaseShowTablesRow("test_table", false, ""),
				mysql.BaseShowTablesRow("seq", false, "vitess_sequence"),
				mysql.BaseShowTablesRow("msg", false, "vitess_message,vt_ack_wait=30,vt_purge_after=120,vt_batch_size=1,vt_cache_size=10,vt_poller_interval=30"),
			},
			RowsAffected: 3,
		},
		mysql.BaseShowPrimary: {
			Fields: mysql.ShowPrimaryFields,
			Rows: [][]types.Value{
				mysql.ShowPrimaryRow("test_table", "pk"),
				mysql.ShowPrimaryRow("seq", "id"),
				mysql.ShowPrimaryRow("msg", "id"),
			},
			RowsAffected: 3,
		},
		"select * from test_table where 1 != 1": {
			Fields: []*types.Field{{
				Name: "pk",
				Type: types.Int32,
			}, {
				Name: "name",
				Type: types.Int32,
			}, {
				Name: "addr",
				Type: types.Int32,
			}},
		},
		"select * from seq where 1 != 1": {
			Fields: []*types.Field{{
				Name: "id",
				Type: types.Int32,
			}, {
				Name: "next_id",
				Type: types.Int64,
			}, {
				Name: "cache",
				Type: types.Int64,
			}, {
				Name: "increment",
				Type: types.Int64,
			}},
		},
		"select * from msg where 1 != 1": {
			Fields: []*types.Field{{
				Name: "id",
				Type: types.Int64,
			}, {
				Name: "priority",
				Type: types.Int64,
			}, {
				Name: "time_next",
				Type: types.Int64,
			}, {
				Name: "epoch",
				Type: types.Int64,
			}, {
				Name: "time_acked",
				Type: types.Int64,
			}, {
				Name: "message",
				Type: types.Int64,
			}},
		},
		"begin":    {},
		"commit":   {},
		"rollback": {},
		fmt.Sprintf(sqlReadAllRedo, TwopcDbName, TwopcDbName): {},
	}
}
