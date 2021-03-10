/*
 *
 *  * Copyright 2021. Go-Sharding Author All Rights Reserved.
 *  *
 *  *  Licensed under the Apache License, Version 2.0 (the "License");
 *  *  you may not use this file except in compliance with the License.
 *  *  You may obtain a copy of the License at
 *  *
 *  *      http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  *  Unless required by applicable law or agreed to in writing, software
 *  *  distributed under the License is distributed on an "AS IS" BASIS,
 *  *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  *  See the License for the specific language governing permissions and
 *  *  limitations under the License.
 *  *
 *  *  File author: Anders Xiao
 *
 */

package explain

import (
	"errors"
	"fmt"
	"github.com/XiaoMi/Gaea/core"
	"github.com/XiaoMi/Gaea/mysql/types"
)

type ValueReference interface {
	ParamIndexes() []int
	GetValue(variables []*types.BindVariable) (interface{}, error)
	IsLiteral() bool
}

type ArgScalarRef struct {
	Index   int
	varType types.MySqlType
}

func (a ArgScalarRef) ParamIndexes() []int {
	if a.Index < 0 {
		return make([]int, 0)
	}
	return []int{a.Index}
}

func (a ArgScalarRef) IsLiteral() bool {
	return false
}

func (a ArgScalarRef) GetValue(variables []*types.BindVariable) (interface{}, error) {
	err := checkArgIndex(a.Index, variables)
	if err != nil {
		return nil, err
	}
	v := variables[a.Index]
	val, err := types.BindVariableToValue(v)
	if err != nil {
		return nil, err
	}
	return types.ToNative(val)
}

func checkArgIndex(index int, variables []*types.BindVariable) error {
	if index < 0 || index >= len(variables) {
		return errors.New(fmt.Sprintf("Argument index '%d' out of range.", index))
	}
	return nil
}

type ArgRangeRef struct {
	lowerArgIndex int
	upperArgIndex int
	closeLower    bool
	closeUpper    bool
	varType       types.MySqlType
}

func NewArgRangeCloseOpen(lowerArgName, upperArgName int, valueType types.MySqlType) *ArgRangeRef {
	return &ArgRangeRef{
		lowerArgIndex: lowerArgName,
		upperArgIndex: upperArgName,
		closeLower:    true,
		closeUpper:    false,
		varType:       valueType,
	}
}

func NewArgRangeOpenClose(lowerArgName, upperArgName int, valueType types.MySqlType) *ArgRangeRef {
	return &ArgRangeRef{
		lowerArgIndex: lowerArgName,
		upperArgIndex: upperArgName,
		closeLower:    true,
		closeUpper:    false,
		varType:       valueType,
	}
}

func NewArgRangeOpen(lowerArgName, upperArgName int, valueType types.MySqlType) *ArgRangeRef {
	return &ArgRangeRef{
		lowerArgIndex: lowerArgName,
		upperArgIndex: upperArgName,
		closeLower:    false,
		closeUpper:    false,
		varType:       valueType,
	}
}

func NewArgRangeClose(lowerArgName, upperArgName int, valueType types.MySqlType) *ArgRangeRef {
	return &ArgRangeRef{
		lowerArgIndex: lowerArgName,
		upperArgIndex: upperArgName,
		closeLower:    true,
		closeUpper:    true,
		varType:       valueType,
	}
}

func (arf ArgRangeRef) ParamIndexes() []int {
	if arf.lowerArgIndex < 0 && arf.upperArgIndex < 0 {
		return nil
	}
	names := make([]int, 0, 2)
	if arf.lowerArgIndex < 0 {
		names = append(names, arf.lowerArgIndex)
	}
	if arf.upperArgIndex < 0 {
		names = append(names, arf.upperArgIndex)
	}
	return names
}

func (arf ArgRangeRef) IsLiteral() bool {
	return false
}

func (arf ArgRangeRef) GetValue(variables []*types.BindVariable) (interface{}, error) {
	var min, max interface{}
	var err error
	if arf.upperArgIndex >= 0 {
		lv := variables[arf.lowerArgIndex]
		min, err = lv.GetGolangValue()
		if err != nil {
			return nil, err
		}
	}

	if arf.upperArgIndex >= 0 {
		uv := variables[arf.upperArgIndex]
		max, err = uv.GetGolangValue()
		if err != nil {
			return nil, err
		}
	}

	return core.NewRange(min, max, arf.closeLower, arf.closeUpper)
}

type ConstRef struct {
	value interface{}
}

func (c ConstRef) ParamIndexes() []int {
	return nil
}

func NewConstRef(value interface{}) *ConstRef {
	return &ConstRef{value: value}
}

func (c ConstRef) IsLiteral() bool {
	return true
}

func (c ConstRef) GetValue(_ []*types.BindVariable) (interface{}, error) {
	return c.value, nil
}
