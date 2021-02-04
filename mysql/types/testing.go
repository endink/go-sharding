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

package types

// TestBindVariable makes a *types.BindVariable from
// an interface{}.It panics on invalid input.
// This function should only be used for testing.
func TestBindVariable(v interface{}) *BindVariable {
	if v == nil {
		return NullBindVariable
	}
	bv, err := BuildBindVariable(v)
	if err != nil {
		panic(err)
	}
	return bv
}

// TestValue builds a Value from typ and val.
// This function should only be used for testing.
func TestValue(typ MySqlType, val string) Value {
	return MakeTrusted(typ, []byte(val))
}
