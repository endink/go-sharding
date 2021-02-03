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
	"testing"
)

func createCallers(username, ip string) Caller {
	return &caller{
		user: username,
		from: ip,
	}
}

func TestTxLimiter_DisabledAllowsAll(t *testing.T) {
	config := defaultTxConfig()
	config.Pool.Size = 10
	config.LimitPerCaller = 0.1
	limiter := NewTxLimiter(config)
	caller := createCallers("", "")
	for i := 0; i < 5; i++ {
		if got, want := limiter.Get(caller), true; got != want {
			t.Errorf("Tx number %d, Get(): got %v, want %v", i, got, want)
		}
	}

}

func TestTxLimiter_LimitsOnlyOffendingUser(t *testing.T) {
	config := defaultTxConfig()
	config.EnableLimit = true
	config.Pool.Size = 10
	config.LimitPerCaller = 0.3
	config.LimitByUsername = true

	// This should allow 3 slots to all users
	newlimiter := NewTxLimiter(config)
	limiter, ok := newlimiter.(*Impl)
	if !ok {
		t.Fatalf("NewTxLimiter returned limiter of unexpected type: got %T, want %T", newlimiter, limiter)
	}
	c1 := createCallers("user1", "")
	c2 := createCallers("user2", "")

	// user1 uses 3 slots
	for i := 0; i < 3; i++ {
		if got, want := limiter.Get(c1), true; got != want {
			t.Errorf("Tx number %d, Get(c1): got %v, want %v", i, got, want)
		}
	}

	// user1 not allowed to use 4th slot, which increases counter
	if got, want := limiter.Get(c1), false; got != want {
		t.Errorf("Get(c1) after using up all allowed attempts: got %v, want %v", got, want)
	}

	// user2 uses 3 slots
	for i := 0; i < 3; i++ {
		if got, want := limiter.Get(c2), true; got != want {
			t.Errorf("Tx number %d, Get(c2): got %v, want %v", i, got, want)
		}
	}

	// user2 not allowed to use 4th slot, which increases counter
	if got, want := limiter.Get(c2), false; got != want {
		t.Errorf("Get(c2) after using up all allowed attempts: got %v, want %v", got, want)
	}

	limiter.Release(c1)
	if got, want := limiter.Get(c1), true; got != want {
		t.Errorf("Get(c1) after releasing: got %v, want %v", got, want)
	}
}

func TestTxLimiterDryRun(t *testing.T) {
	config := defaultTxConfig()
	config.Pool.Size = 10
	config.EnableLimit = true
	config.EnableLimitDryRun = true
	config.LimitPerCaller = 0.3
	config.LimitByUsername = true

	// This should allow 3 slots to all users
	newlimiter := NewTxLimiter(config)
	limiter, ok := newlimiter.(*Impl)
	if !ok {
		t.Fatalf("NewTxLimiter returned limiter of unexpected type: got %T, want %T", newlimiter, limiter)
	}
	c := createCallers("user", "")

	// uses 3 slots
	for i := 0; i < 3; i++ {
		if got, want := limiter.Get(c), true; got != want {
			t.Errorf("Tx number %d, Get(c): got %v, want %v", i, got, want)
		}
	}

	// allowed to use 4th slot, but dry run rejection counter increased
	if got, want := limiter.Get(c), true; got != want {
		t.Errorf("Get(im, ef) after using up all allowed attempts: got %v, want %v", got, want)
	}

}
