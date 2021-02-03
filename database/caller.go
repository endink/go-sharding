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

import "context"

const (
	CallerUserKey = "caller_u"
	CallerFromKey = "caller_f"
)

type Caller interface {
	User() string
	From() string
}

type caller struct {
	user string
	from string
}

func (c caller) User() string {
	return c.user
}

func (c caller) From() string {
	return c.from
}

// NewContext adds the provided EffectiveCallerID(vtrpcpb.CallerID) and ImmediateCallerID(querypb.VTGateCallerID)
// into the Context
func NewContext(ctx context.Context, user string, from string) context.Context {
	ctx = context.WithValue(
		context.WithValue(ctx, CallerUserKey, user),
		CallerFromKey,
		from,
	)
	return ctx
}

// NewContext adds the provided EffectiveCallerID(vtrpcpb.CallerID) and ImmediateCallerID(querypb.VTGateCallerID)
// into the Context
func NewContextWithCaller(ctx context.Context, caller Caller) context.Context {
	return NewContext(ctx, caller.User(), caller.From())
}

// EffectiveCallerIDFromContext returns the EffectiveCallerID(vtrpcpb.CallerID)
// stored in the Context, if any
func CallerFromContext(ctx context.Context) Caller {
	u, ok := ctx.Value(CallerUserKey).(string)
	if ok {
		from, f := ctx.Value(CallerFromKey).(string)
		if f {
			return caller{
				user: u,
				from: from,
			}
		}
	}
	return caller{}
}
