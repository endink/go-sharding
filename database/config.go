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

import "time"

type TxConfig struct {
	TwoPCAbandonAge   time.Duration
	EnableTwoPC       bool
	EnableLimit       bool
	EnableLimitDryRun bool
	LimitPerCaller    float64
	LimitByUsername   bool
	LimitByAddr       bool
	Timout            time.Duration
	Pool              ConnPoolConfig
	GracePeriods      time.Duration
}

type GracePeriodsConfig struct {
	ShutdownSeconds   time.Duration `json:"shutdownSeconds,omitempty"`
	TransitionSeconds time.Duration `json:"transitionSeconds,omitempty"`
}

func defaultTxConfig() TxConfig {
	return TxConfig{
		TwoPCAbandonAge:   time.Second * 2,
		EnableTwoPC:       true,
		EnableLimit:       false,
		EnableLimitDryRun: false,

		// Single user can use up to 40% of transaction pool slots. Enough to
		// accommodate 2 misbehaving users.
		LimitPerCaller: 0.4,

		LimitByUsername: true,
		LimitByAddr:     true,
		Pool: ConnPoolConfig{
			Size:               20,
			TimeoutSeconds:     1,
			IdleTimeoutSeconds: 30 * 60,
			MaxWaiters:         5000,
		},
	}
}

// ConnPoolConfig contains the config for a conn pool.
type ConnPoolConfig struct {
	Size               int    `json:"size,omitempty"`
	TimeoutSeconds     uint64 `json:"timeoutSeconds,omitempty"`
	IdleTimeoutSeconds uint64 `json:"idleTimeoutSeconds,omitempty"`
	PrefillParallelism int    `json:"prefillParallelism,omitempty"`
	MaxWaiters         int    `json:"maxWaiters,omitempty"`
	IsNoPool           bool   `json:"isNoPool"`
}

type DbConfig struct {
	Tx           TxConfig
	Pool         ConnPoolConfig
	GracePeriods GracePeriodsConfig
}

var defaultDbConfig = &DbConfig{
	Pool: ConnPoolConfig{
		Size:               16,
		IdleTimeoutSeconds: 30 * 60,
		MaxWaiters:         5000,
	},

	Tx: TxConfig{
		EnableTwoPC:       true,
		TwoPCAbandonAge:   time.Second * 2,
		EnableLimit:       false,
		EnableLimitDryRun: false,
		LimitPerCaller:    0,
		LimitByUsername:   false,
		LimitByAddr:       false,
		Timout:            0,
		Pool: ConnPoolConfig{
			Size:               20,
			TimeoutSeconds:     1,
			IdleTimeoutSeconds: 30 * 60,
			MaxWaiters:         5000,
		},
	},

	GracePeriods: GracePeriodsConfig{
		ShutdownSeconds:   time.Second * 2,
		TransitionSeconds: time.Second * 2,
	},
}

func NewDbConfig() *DbConfig {
	v := *defaultDbConfig
	return &v
}
