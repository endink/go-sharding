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

package core

import (
	"strings"
)

// Role defines the level of access on a table
type Role int

const (
	// RoleReader can run SELECT statements
	RoleReader Role = iota
	// RoleWriter can run SELECT, INSERT & UPDATE statements
	RoleWriter
	// RoleAdmin can run any statements including DDLs
	RoleAdmin
	// NumRoles is number of Roles defined
	NumRoles
)

var roleNames = []string{
	"RoleReader",
	"RoleWriter",
	"RoleAdmin",
}

// Name returns the name of a role
func (r Role) Name() string {
	if r < RoleReader || r > RoleAdmin {
		return ""
	}
	return roleNames[r]
}

// RoleByName returns the Role corresponding to a name
func RoleByName(s string) (Role, bool) {
	for i, v := range roleNames {
		if v == strings.ToUpper(s) {
			return Role(i), true
		}
	}
	return NumRoles, false
}
