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
	"fmt"
	"github.com/endink/go-sharding/core/errors"
	"github.com/endink/go-sharding/database"
	"github.com/endink/go-sharding/mysql/types"
)

// Begin starts a new transaction. This is allowed only if the state is StateServing.
func (ec *Executor) Begin(ctx context.Context, target *database.Target, options *types.ExecuteOptions) (transactionID int64, err error) {
	return ec.begin(ctx, target, nil, 0, options)
}

func (ec *Executor) begin(ctx context.Context, target *database.Target, preQueries []string, reservedID int64, options *types.ExecuteOptions) (transactionID int64, err error) {
	err = ec.exec(
		ctx,
		"Begin", "begin", nil,
		target, options,
		func(e executionContext) error {
			//if ec.txThrottler.Throttle() {
			//	return vterrors.Errorf(vtrpcpb.Code_RESOURCE_EXHAUSTED, "Transaction throttled")
			//}
			transactionID, _, err = e.te.Begin(ctx, preQueries, reservedID, options)
			return err
		},
	)
	return transactionID, err
}

// Commit commits the specified transaction.
func (ec *Executor) Commit(ctx context.Context, target *database.Target, transactionID int64) (newReservedID int64, err error) {
	err = ec.exec(
		ctx,
		"Commit", "commit", nil,
		target, nil,
		func(e executionContext) error {
			newReservedID, _, err = e.te.Commit(ctx, transactionID)
			return err
		},
	)
	return newReservedID, err
}

// Rollback rollsback the specified transaction.
func (ec *Executor) Rollback(ctx context.Context, target *database.Target, transactionID int64) (newReservedID int64, err error) {
	err = ec.exec(
		ctx,
		"Rollback", "rollback", nil,
		target, nil,
		func(e executionContext) error {
			newReservedID, err = e.te.Rollback(ctx, transactionID)
			return err
		},
	)
	return newReservedID, err
}

//Release implements the QueryService interface
func (ec *Executor) Release(ctx context.Context, target *database.Target, transactionID, reservedID int64) error {
	if reservedID == 0 && transactionID == 0 {
		return fmt.Errorf("%w\nConnection Id and Transaction ID does not exists", errors.ErrInvalidArgument)
	}
	return ec.exec(
		ctx,
		"Release", "", nil,
		target, nil,
		func(e executionContext) error {
			if reservedID != 0 {
				//Release to close the underlying connection.
				return e.te.Release(ctx, reservedID)
			}
			// Rollback to cleanup the transaction before returning to the pool.
			_, err := e.te.Rollback(ctx, transactionID)
			return err
		},
	)
}
