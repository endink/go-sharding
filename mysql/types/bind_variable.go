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

import (
	"bytes"
	"fmt"
)

var NullBindVariable = &BindVariable{Type: Null}

type BindVariable struct {
	Type  MySqlType
	Value []byte
	// values are set if type is TUPLE.
	Values []*Value
}

func (bv *BindVariable) Equals(v interface{}) bool {
	if bv2, ok := v.(*BindVariable); ok {
		eq1 := bv.Type == bv2.Type && bytes.Equal(bv.Value, bv2.Value)
		if eq1 && len(bv.Values) == len(bv2.Values) {
			for i, tv := range bv.Values {
				if !tv.Equals(bv2.Values[i]) {
					return false
				}
				return false
			}
			return true
		}
	}
	return false
}

func (bv *BindVariable) Clone() *BindVariable {
	var values []*Value
	if bv.Values != nil {
		values = make([]*Value, len(values))
		for i, value := range bv.Values {
			values[i] = value.Clone()
		}
	}

	var value []byte
	if bv.Value != nil {
		value = make([]byte, len(bv.Value))
		copy(value, bv.Value)
	}

	return &BindVariable{
		Type:   bv.Type,
		Value:  value,
		Values: values,
	}
}

func (bv *BindVariable) GetGolangValue() (interface{}, error) {
	val, err := BindVariableToValue(bv)
	if err != nil {
		return nil, err
	}
	return ToNative(val)
}

func BindVarsArrayEquals(excepted []*BindVariable, actual []*BindVariable) bool {
	if fmt.Sprintf("%p", actual) == fmt.Sprintf("%p", excepted) {
		return true
	}

	if len(excepted) != len(actual) {
		return false
	}
	for i, v := range actual {
		if !v.Equals(excepted[i]) {
			return false
		}
	}
	return true
}

func BindVarsMapEquals(excepted map[string]*BindVariable, actual map[string]*BindVariable) bool {
	if len(excepted) != len(actual) {
		return false
	}
	for n, v := range actual {
		if ev, ok := excepted[n]; !ok {
			return false
		} else {
			if !ev.Equals(v) {
				return false
			}
		}
	}

	for n, _ := range excepted {
		if _, ok := actual[n]; !ok {
			return false
		}
	}
	return true
}
