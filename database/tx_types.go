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

// TransactionState represents the state of a distributed transaction.
type TransactionState int32

const (
	TransactionStateUnknown  TransactionState = 0
	TransactionStatePrepare  TransactionState = 1
	TransactionStateCommit   TransactionState = 2
	TransactionStateRollback TransactionState = 3
)

var TransactionStateNames = map[TransactionState]string{
	TransactionStateUnknown:  "UNKNOWN",
	TransactionStatePrepare:  "PREPARE",
	TransactionStateCommit:   "COMMIT",
	TransactionStateRollback: "ROLLBACK",
}

var TransactionStateValues = map[string]TransactionState{
	"UNKNOWN":  TransactionStateUnknown,
	"PREPARE":  TransactionStatePrepare,
	"COMMIT":   TransactionStateCommit,
	"ROLLBACK": TransactionStateRollback,
}

func (x TransactionState) String() string {
	if n, ok := TransactionStateNames[x]; ok {
		return n
	}
	return "UNKNOWN"
}

type TransactionMetadata struct {
	Dtid         string
	State        TransactionState
	TimeCreated  int64
	Participants []*Target
}
