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

var NullBindVariable = &BindVariable{Type: Null}

type BindVariable struct {
	Type  MySqlType
	Value []byte
	// values are set if type is TUPLE.
	Values []*Value
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
