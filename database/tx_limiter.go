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
	"context"
	"go.opentelemetry.io/otel/label"
	"go.opentelemetry.io/otel/metric"
	"strings"
	"sync"
)

const unknown string = "unknown"

// TxLimiter is the transaction limiter interface.
type TxLimiter interface {
	Get(Caller) bool
	GetWith(context.Context, Caller) bool
	Release(Caller)
}

// NewTxLimiter creates a new TxLimiter.
// slotCount: total slot count in transaction pool
// maxPerUser: fraction of the pool that may be taken by single user
// enabled: should the feature be enabled. If false, will return
// "allow-all" limiter
// dryRun: if true, does no limiting, but records stats of the decisions made
// byXXX: whether given field from immediate/effective caller id should be taken
// into account when deciding "user" identity for purposes of transaction
// limiting.
func NewTxLimiter(config TxConfig) TxLimiter {
	if !config.EnableLimit && !config.EnableLimitDryRun {
		return &TxAllowAll{}
	}

	return &Impl{
		maxPerUser:       int64(float64(config.Pool.Size) * config.LimitPerCaller),
		dryRun:           config.EnableLimitDryRun,
		byUsername:       config.LimitByUsername,
		byIP:             config.LimitByAddr,
		usageMap:         make(map[string]int64),
		rejections:       DbMeter.NewInt64Counter("tx_limiter_rejections", "rejections from TxLimiter"),
		rejectionsDryRun: DbMeter.NewInt64Counter("tx_limiter_rejections_dry_run", "rejections from TxLimiter in dry run"),
	}
}

// TxAllowAll is a TxLimiter that allows all Get requests and does no tracking.
// Implements Txlimiter.
type TxAllowAll struct{}

// Get always returns true (allows all requests).
// Implements TxLimiter.Get
func (txa *TxAllowAll) GetWith(context.Context, Caller) bool {
	return true
}

// Get always returns true (allows all requests).
// Implements TxLimiter.Get
func (txa *TxAllowAll) Get(Caller) bool {
	return true
}

// Release is noop, because TxAllowAll does no tracking.
// Implements TxLimiter.Release
func (txa *TxAllowAll) Release(Caller) {
	// NOOP
}

// Impl limits the total number of transactions a single user may use
// concurrently.
// Implements TxLimiter.
type Impl struct {
	maxPerUser int64
	usageMap   map[string]int64
	mu         sync.Mutex

	dryRun     bool
	byUsername bool
	byIP       bool

	rejections, rejectionsDryRun metric.Int64Counter
}

func (txl *Impl) Get(caller Caller) bool {
	return txl.GetWith(nil, caller)
}

// Get tells whether given user (identified by context.Context) is allowed
// to use another transaction slot. If this method returns true, it's
// necessary to call Release once transaction is returned to the pool.
// Implements TxLimiter.Get
func (txl *Impl) GetWith(ctx context.Context, caller Caller) bool {
	c := ctx
	if c == nil {
		ctx = context.TODO()
	}
	key := txl.extractKey(caller)

	txl.mu.Lock()
	defer txl.mu.Unlock()

	usage := txl.usageMap[key]
	if usage < txl.maxPerUser {
		txl.usageMap[key] = usage + 1
		return true
	}

	if txl.dryRun {
		log.Infof("TxLimiter: DRY RUN: user over limit: %s", key)
		txl.rejectionsDryRun.Add(c, 1, label.String("user", key))
		return true
	}

	log.Infof("TxLimiter: Over limit, rejecting transaction request for user: %s", key)
	txl.rejections.Add(c, 1, label.String("user", key))
	return false
}

// Release marks that given user (identified by caller ID) is no longer using
// a transaction slot.
// Implements TxLimiter.Release
func (txl *Impl) Release(conn Caller) {
	key := txl.extractKey(conn)

	txl.mu.Lock()
	defer txl.mu.Unlock()

	usage, ok := txl.usageMap[key]
	if !ok {
		return
	}
	if usage == 1 {
		delete(txl.usageMap, key)
		return
	}

	txl.usageMap[key] = usage - 1
}

// extractKey builds a string key used to differentiate users, based
// on fields specified in configuration and their values from caller ID.
func (txl *Impl) extractKey(caller Caller) string {
	var parts []string

	if txl.byIP {
		if caller != nil {
			parts = append(parts, caller.From())
		} else {
			parts = append(parts, unknown)
		}
	}

	if txl.byUsername {
		if caller != nil {
			parts = append(parts, caller.User())
		} else {
			parts = append(parts, unknown)
		}
	}

	return strings.Join(parts, "/")
}
