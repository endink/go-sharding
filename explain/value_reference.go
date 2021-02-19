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
	ParamNames() []string
	GetValue(variables map[string]*types.BindVariable) (interface{}, error)
	IsConst() bool
}

type ArgScalarRef struct {
	argName string
	varType types.MySqlType
}

func (a ArgScalarRef) ParamNames() []string {
	if a.argName == "" {
		return make([]string, 0)
	}
	return []string{a.argName}
}

func (a ArgScalarRef) IsConst() bool {
	return false
}

func (a ArgScalarRef) GetValue(variables map[string]*types.BindVariable) (interface{}, error) {
	if v, ok := variables[a.argName]; ok {
		val, err := types.BindVariableToValue(v)
		if err != nil {
			return nil, err
		}
		return types.ToNative(val)
	}
	return nil, errors.New(fmt.Sprintf("Can not find argument '%s' from bind variable list.", a.argName))
}

type ArgRangeRef struct {
	lowerArgName string
	upperArgName string
	closeLower   bool
	closeUpper   bool
	varType      types.MySqlType
}

func NewArgRangeCloseOpen(lowerArgName, upperArgName string, valueType types.MySqlType) *ArgRangeRef {
	return &ArgRangeRef{
		lowerArgName: lowerArgName,
		upperArgName: upperArgName,
		closeLower:   true,
		closeUpper:   false,
		varType:      valueType,
	}
}

func NewArgRangeOpenClose(lowerArgName, upperArgName string, valueType types.MySqlType) *ArgRangeRef {
	return &ArgRangeRef{
		lowerArgName: lowerArgName,
		upperArgName: upperArgName,
		closeLower:   true,
		closeUpper:   false,
		varType:      valueType,
	}
}

func NewArgRangeOpen(lowerArgName, upperArgName string, valueType types.MySqlType) *ArgRangeRef {
	return &ArgRangeRef{
		lowerArgName: lowerArgName,
		upperArgName: upperArgName,
		closeLower:   false,
		closeUpper:   false,
		varType:      valueType,
	}
}

func NewArgRangeClose(lowerArgName, upperArgName string, valueType types.MySqlType) *ArgRangeRef {
	return &ArgRangeRef{
		lowerArgName: lowerArgName,
		upperArgName: upperArgName,
		closeLower:   true,
		closeUpper:   true,
		varType:      valueType,
	}
}

func (arf ArgRangeRef) ParamNames() []string {
	if arf.lowerArgName == "" && arf.upperArgName == "" {
		return nil
	}
	names := make([]string, 0, 2)
	if arf.lowerArgName != "" {
		names = append(names, arf.lowerArgName)
	}
	if arf.upperArgName != "" {
		names = append(names, arf.upperArgName)
	}
	return names
}

func (arf ArgRangeRef) IsConst() bool {
	return false
}

func (arf ArgRangeRef) GetValue(variables map[string]*types.BindVariable) (interface{}, error) {
	var min, max interface{}
	var bv *types.BindVariable
	var ok bool
	var err error
	if arf.upperArgName != "" {
		if bv, ok = variables[arf.lowerArgName]; ok {
			min, err = bv.GetGolangValue()
			if err != nil {
				return nil, err
			}
		} else {
			return nil, errors.New(fmt.Sprintf("Can not find argument '%s' from bind variable list.", arf.lowerArgName))
		}
	}

	if arf.upperArgName != "" {
		if bv, ok = variables[arf.upperArgName]; ok {
			max, err = bv.GetGolangValue()
			if err != nil {
				return nil, err
			}
		} else {
			return nil, errors.New(fmt.Sprintf("Can not find argument '%s' from bind variable list.", arf.upperArgName))
		}
	}

	return core.NewRange(min, max, arf.closeLower, arf.closeUpper)
}

type ConstRef struct {
	value interface{}
}

func (c ConstRef) ParamNames() []string {
	return nil
}

func NewConstRef(value interface{}) *ConstRef {
	return &ConstRef{value: value}
}

func (c ConstRef) IsConst() bool {
	return true
}

func (c ConstRef) GetValue(variables map[string]*types.BindVariable) (interface{}, error) {
	return c.value, nil
}
