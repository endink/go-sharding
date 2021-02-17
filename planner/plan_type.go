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

package planner

import "encoding/json"

// PlanType indicates a query plan type.
type PlanType int

// The following are PlanType values.
const (
	PlanSelect PlanType = iota
	PlanSelectLock
	PlanNextval
	PlanSelectImpossible
	PlanInsert
	PlanInsertMessage
	PlanUpdate
	PlanUpdateLimit
	PlanDelete
	PlanDeleteLimit
	PlanDDL
	PlanSet
	// PlanOtherRead is for statements like show, etc.
	PlanOtherRead
	// PlanOtherAdmin is for statements like repair, lock table, etc.
	PlanOtherAdmin
	PlanSelectStream
	// PlanMessageStream is for "stream" statements.
	PlanMessageStream
	PlanSavepoint
	PlanRelease
	PlanSRollback
	PlanShowTables
	// PlanLoad is for Load data statements
	PlanLoad
	// PlanFlush is for FLUSH statements
	PlanFlush
	PlanLockTables
	PlanUnlockTables
	PlanCallProc
	NumPlans
)

// Must exactly match order of plan constants.
var planName = []string{
	"Select",
	"SelectLock",
	"Nextval",
	"SelectImpossible",
	"Insert",
	"InsertMessage",
	"Update",
	"UpdateLimit",
	"Delete",
	"DeleteLimit",
	"DDL",
	"Set",
	"OtherRead",
	"OtherAdmin",
	"SelectStream",
	"MessageStream",
	"Savepoint",
	"Release",
	"RollbackSavepoint",
	"ShowTables",
	"Load",
	"Flush",
	"LockTables",
	"UnlockTables",
	"CallProcedure",
}

func (pt PlanType) String() string {
	if pt < 0 || pt >= NumPlans {
		return ""
	}
	return planName[pt]
}

// PlanByName find a PlanType by its string name.
func PlanByName(s string) (pt PlanType, ok bool) {
	for i, v := range planName {
		if v == s {
			return PlanType(i), true
		}
	}
	return NumPlans, false
}

// IsSelect returns true if PlanType is about a select query.
func (pt PlanType) IsSelect() bool {
	return pt == PlanSelect || pt == PlanSelectLock || pt == PlanSelectImpossible
}

// MarshalJSON returns a json string for PlanType.
func (pt PlanType) MarshalJSON() ([]byte, error) {
	return json.Marshal(pt.String())
}
