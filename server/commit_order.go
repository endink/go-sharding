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

// CommitOrder is used to designate which of the ShardSessions
// get used for transactions.
type CommitOrder int32

const (
	// NORMAL is the default commit order.
	CommitOrderNormal CommitOrder = iota
	// PRE is used to designate pre_sessions.
	CommitOrderPre
	// POST is used to designate post_sessions.
	CommitOrderPost
	// AUTOCOMMIT is used to run the statement as autocommitted transaction.
	CommitOrderAutocommit
)
