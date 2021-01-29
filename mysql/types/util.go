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
	"errors"
	"fmt"
	"strconv"
)

// BuildBindVariables builds a map[string]*BindVariable from a map[string]interface{}.
func BuildBindVariables(in map[string]interface{}) (map[string]*BindVariable, error) {
	if len(in) == 0 {
		return nil, nil
	}

	out := make(map[string]*BindVariable, len(in))
	for k, v := range in {
		bv, err := BuildBindVariable(v)
		if err != nil {
			return nil, fmt.Errorf("%s: %v", k, err)
		}
		out[k] = bv
	}
	return out, nil
}

// Int8BindVariable converts an int8 to a bind var.
func Int8BindVariable(v int8) *BindVariable {
	return ValueBindVariable(NewInt8(v))
}

// Int32BindVariable converts an int32 to a bind var.
func Int32BindVariable(v int32) *BindVariable {
	return ValueBindVariable(NewInt32(v))
}

// BoolBindVariable converts an bool to a int32 bind var.
func BoolBindVariable(v bool) *BindVariable {
	if v {
		return Int32BindVariable(1)
	}
	return Int32BindVariable(0)
}

// Int64BindVariable converts an int64 to a bind var.
func Int64BindVariable(v int64) *BindVariable {
	return ValueBindVariable(NewInt64(v))
}

// Uint64BindVariable converts a uint64 to a bind var.
func Uint64BindVariable(v uint64) *BindVariable {
	return ValueBindVariable(NewUint64(v))
}

// Float64BindVariable converts a float64 to a bind var.
func Float64BindVariable(v float64) *BindVariable {
	return ValueBindVariable(NewFloat64(v))
}

// StringBindVariable converts a string to a bind var.
func StringBindVariable(v string) *BindVariable {
	return ValueBindVariable(NewVarBinary(v))
}

// BytesBindVariable converts a []byte to a bind var.
func BytesBindVariable(v []byte) *BindVariable {
	return &BindVariable{Type: VarBinary, Value: v}
}

// ValueBindVariable converts a Value to a bind var.
func ValueBindVariable(v Value) *BindVariable {
	return &BindVariable{Type: v.ValueType, Value: v.Value}
}

// BuildBindVariable builds a *BindVariable from a valid input type.
func BuildBindVariable(v interface{}) (*BindVariable, error) {
	switch v := v.(type) {
	case string:
		return BytesBindVariable([]byte(v)), nil
	case []byte:
		return BytesBindVariable(v), nil
	case bool:
		if v {
			return Int8BindVariable(1), nil
		}
		return Int8BindVariable(0), nil
	case int:
		return &BindVariable{
			Type:  Int64,
			Value: strconv.AppendInt(nil, int64(v), 10),
		}, nil
	case int64:
		return Int64BindVariable(v), nil
	case uint64:
		return Uint64BindVariable(v), nil
	case float64:
		return Float64BindVariable(v), nil
	case nil:
		return NullBindVariable, nil
	case Value:
		return ValueBindVariable(v), nil
	case *BindVariable:
		return v, nil
	case []interface{}:
		bv := &BindVariable{
			Type:   Tuple,
			Values: make([]*Value, len(v)),
		}
		values := make([]Value, len(v))
		for i, lv := range v {
			lbv, err := BuildBindVariable(lv)
			if err != nil {
				return nil, err
			}
			values[i].ValueType = lbv.Type
			values[i].Value = lbv.Value
			bv.Values[i] = &values[i]
		}
		return bv, nil
	case []string:
		bv := &BindVariable{
			Type:   Tuple,
			Values: make([]*Value, len(v)),
		}
		values := make([]Value, len(v))
		for i, lv := range v {
			values[i].ValueType = VarBinary
			values[i].Value = []byte(lv)
			bv.Values[i] = &values[i]
		}
		return bv, nil
	case [][]byte:
		bv := &BindVariable{
			Type:   Tuple,
			Values: make([]*Value, len(v)),
		}
		values := make([]Value, len(v))
		for i, lv := range v {
			values[i].ValueType = VarBinary
			values[i].Value = lv
			bv.Values[i] = &values[i]
		}
		return bv, nil
	case []int:
		bv := &BindVariable{
			Type:   Tuple,
			Values: make([]*Value, len(v)),
		}
		values := make([]Value, len(v))
		for i, lv := range v {
			values[i].ValueType = Int64
			values[i].Value = strconv.AppendInt(nil, int64(lv), 10)
			bv.Values[i] = &values[i]
		}
		return bv, nil
	case []int64:
		bv := &BindVariable{
			Type:   Tuple,
			Values: make([]*Value, len(v)),
		}
		values := make([]Value, len(v))
		for i, lv := range v {
			values[i].ValueType = Int64
			values[i].Value = strconv.AppendInt(nil, lv, 10)
			bv.Values[i] = &values[i]
		}
		return bv, nil
	case []uint64:
		bv := &BindVariable{
			Type:   Tuple,
			Values: make([]*Value, len(v)),
		}
		values := make([]Value, len(v))
		for i, lv := range v {
			values[i].ValueType = Uint64
			values[i].Value = strconv.AppendUint(nil, lv, 10)
			bv.Values[i] = &values[i]
		}
		return bv, nil
	case []float64:
		bv := &BindVariable{
			Type:   Tuple,
			Values: make([]*Value, len(v)),
		}
		values := make([]Value, len(v))
		for i, lv := range v {
			values[i].ValueType = Float64
			values[i].Value = strconv.AppendFloat(nil, lv, 'g', -1, 64)
			bv.Values[i] = &values[i]
		}
		return bv, nil
	case []Value:
		bv := &BindVariable{
			Type:   Tuple,
			Values: make([]*Value, len(v)),
		}
		values := make([]Value, len(v))
		for i, lv := range v {
			lbv, err := BuildBindVariable(lv)
			if err != nil {
				return nil, err
			}
			values[i].ValueType = lbv.Type
			values[i].Value = lbv.Value
			bv.Values[i] = &values[i]
		}
		return bv, nil
	}
	return nil, fmt.Errorf("type %T not supported as bind var: %v", v, v)
}

// ValidateBindVariables validates a map[string]*BindVariable.
func ValidateBindVariables(bv map[string]*BindVariable) error {
	for k, v := range bv {
		if err := ValidateBindVariable(v); err != nil {
			return fmt.Errorf("%s: %v", k, err)
		}
	}
	return nil
}

// ValidateBindVariable returns an error if the bind variable has inconsistent
// fields.
func ValidateBindVariable(bv *BindVariable) error {
	if bv == nil {
		return errors.New("bind variable is nil")
	}

	if bv.Type == Tuple {
		if len(bv.Values) == 0 {
			return errors.New("empty tuple is not allowed")
		}
		for _, val := range bv.Values {
			if val.ValueType == Tuple {
				return errors.New("tuple not allowed inside another tuple")
			}
			if err := ValidateBindVariable(&BindVariable{Type: val.ValueType, Value: val.Value}); err != nil {
				return err
			}
		}
		return nil
	}

	// If NewValue succeeds, the value is valid.
	_, err := NewValue(bv.Type, bv.Value)
	return err
}

// BindVariableToValue converts a bind var into a Value.
func BindVariableToValue(bv *BindVariable) (Value, error) {
	if bv.Type == Tuple {
		return NULL, errors.New("cannot convert a TUPLE bind var into a value")
	}
	return MakeTrusted(bv.Type, bv.Value), nil
}
