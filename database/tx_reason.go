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

// ReleaseReason as type int
type ReleaseReason int

const (
	// TxClose - connection released on close.
	TxClose ReleaseReason = iota

	// TxCommit - connection released on commit.
	TxCommit

	// TxRollback - connection released on rollback.
	TxRollback

	// TxKill - connection released on tx kill.
	TxKill

	// ConnInitFail - connection released on failed to start tx.
	ConnInitFail

	// ConnRelease - connection closed.
	ConnRelease

	// ConnRenewFail - reserve connection renew failed.
	ConnRenewFail
)

func (r ReleaseReason) String() string {
	return txResolutions[r]
}

//Name return the name of enum.
func (r ReleaseReason) Name() string {
	return txNames[r]
}

var txResolutions = map[ReleaseReason]string{
	TxClose:       "closed",
	TxCommit:      "transaction committed",
	TxRollback:    "transaction rolled back",
	TxKill:        "kill",
	ConnInitFail:  "initFail",
	ConnRelease:   "release connection",
	ConnRenewFail: "connection renew failed",
}

var txNames = map[ReleaseReason]string{
	TxClose:       "close",
	TxCommit:      "commit",
	TxRollback:    "rollback",
	TxKill:        "kill",
	ConnInitFail:  "initFail",
	ConnRelease:   "release",
	ConnRenewFail: "renewFail",
}
