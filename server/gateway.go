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
	"github.com/endink/go-sharding/database"
	"github.com/endink/go-sharding/mysql/types"
)

type Gateway interface {

	// CreateTransaction creates the metadata for a 2PC transaction.
	CreateTransaction(ctx context.Context, mmShard *database.Target, dtid string, participants []*database.Target) (err error)

	// Prepare prepares the specified transaction.
	Prepare(ctx context.Context, target *database.Target, transactionID int64, dtid string) (err error)

	// StartCommit atomically commits the transaction along with the
	// decision to commit the associated 2pc transaction.
	StartCommit(ctx context.Context, target *database.Target, transactionID int64, dtid string) (err error)

	// CommitPrepared commits the prepared transaction.
	CommitPrepared(ctx context.Context, target *database.Target, dtid string) (err error)

	// ConcludeTransaction deletes the 2pc transaction metadata
	// essentially resolving it.
	ConcludeTransaction(ctx context.Context, target *database.Target, dtid string) (err error)

	// RollbackPrepared rolls back the prepared transaction.
	RollbackPrepared(ctx context.Context, target *database.Target, dtid string, originalID int64) (err error)

	// ReadTransaction returns the metadata for the specified dtid.
	ReadTransaction(ctx context.Context, target *database.Target, dtid string) (metadata *database.TransactionMetadata, err error)

	// Complete transitions the 2pc transaction to the Rollback state. if auto commit, commit it.
	// If a transaction id is provided, that transaction is also rolled back.
	Complete(ctx context.Context, target *database.Target, dtid string, transactionID int64) (err error)

	// Query execution
	Execute(ctx context.Context, target *database.Target, sql string, bindVariables map[string]*types.BindVariable, transactionID, reservedID int64, options *types.ExecuteOptions) (*types.Result, error)
	// Currently always called with transactionID = 0
	StreamExecute(ctx context.Context, target *database.Target, sql string, bindVariables map[string]*types.BindVariable, transactionID int64, options *types.ExecuteOptions, callback func(*types.Result) error) error
	// Currently always called with transactionID = 0
	ExecuteBatch(ctx context.Context, target *database.Target, queries []*types.BoundQuery, asTransaction bool, transactionID int64, options *types.ExecuteOptions) ([]types.Result, error)

	ReserveExecute(ctx context.Context, target *database.Target, preQueries []string, sql string, bindVariables map[string]*types.BindVariable, transactionID int64, options *types.ExecuteOptions) (*types.Result, int64, error)

	ReserveBeginExecute(ctx context.Context, target *database.Target, preQueries []string, sql string, bindVariables map[string]*types.BindVariable, options *types.ExecuteOptions) (*types.Result, int64, int64, error)

	// Combo methods, they also return the transactionID from the
	// Begin part. If err != nil, the transactionID may still be
	// non-zero, and needs to be propagated back (like for a DB
	// Integrity Error)
	BeginExecute(ctx context.Context, target *database.Target, preQueries []string, sql string, bindVariables map[string]*types.BindVariable, reservedID int64, options *types.ExecuteOptions) (*types.Result, int64, error)
	BeginExecuteBatch(ctx context.Context, target *database.Target, queries []*types.BoundQuery, asTransaction bool, options *types.ExecuteOptions) ([]types.Result, int64, error)
	// Rollback aborts the current transaction
	Rollback(ctx context.Context, target *database.Target, transactionID int64) (int64, error)

	Release(ctx context.Context, target *database.Target, transactionID, reservedID int64) (err error)
	// Commit commits the current transaction
	Commit(ctx context.Context, target *database.Target, transactionId int64) (reservedId int64, err error)
}
