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
	"fmt"
	"strconv"
)

// ToUint64 converts Value to uint64.
func ToUint64(v Value) (uint64, error) {
	num, err := newIntegralNumeric(v)
	if err != nil {
		return 0, err
	}
	switch num.typ {
	case Int64:
		if num.ival < 0 {
			return 0, fmt.Errorf("negative number cannot be converted to unsigned: %d", num.ival)
		}
		return uint64(num.ival), nil
	case Uint64:
		return num.uval, nil
	}
	panic("unreachable")
}

// ToInt64 converts Value to int64.
func ToInt64(v Value) (int64, error) {
	num, err := newIntegralNumeric(v)
	if err != nil {
		return 0, err
	}
	switch num.typ {
	case Int64:
		return num.ival, nil
	case Uint64:
		ival := int64(num.uval)
		if ival < 0 {
			return 0, fmt.Errorf("unsigned number overflows int64 value: %d", num.uval)
		}
		return ival, nil
	}
	panic("unreachable")
}

// ToFloat64 converts Value to float64.
func ToFloat64(v Value) (float64, error) {
	num, err := newEvalResult(v)
	if err != nil {
		return 0, err
	}
	switch num.typ {
	case Int64:
		return float64(num.ival), nil
	case Uint64:
		return float64(num.uval), nil
	case Float64:
		return num.fval, nil
	}

	if IsText(num.typ) || IsBinary(num.typ) {
		fval, err := strconv.ParseFloat(string(v.Value), 64)
		if err != nil {
			return 0, fmt.Errorf("%v", err)
		}
		return fval, nil
	}

	return 0, fmt.Errorf("cannot convert to float: %s", v.String())
}

// ToNative converts Value to a native go type.
// Decimal is returned as []byte.
func ToNative(v Value) (interface{}, error) {
	var out interface{}
	var err error
	switch {
	case v.ValueType == Null:
		// no-op
	case v.IsSigned():
		return ToInt64(v)
	case v.IsUnsigned():
		return ToUint64(v)
	case v.IsFloat():
		return ToFloat64(v)
	case v.IsQuoted() || v.ValueType == Bit || v.ValueType == Decimal:
		out = v.ToBytes()
	case v.ValueType == Expression:
		err = fmt.Errorf("%v cannot be converted to a go type", v)
	}
	return out, err
}

// newIntegralNumeric parses a value and produces an Int64 or Uint64.
func newIntegralNumeric(v Value) (EvalResult, error) {
	str := v.ToString()
	switch {
	case v.IsSigned():
		ival, err := strconv.ParseInt(str, 10, 64)
		if err != nil {
			return EvalResult{}, fmt.Errorf("%v", err)
		}
		return EvalResult{ival: ival, typ: Int64}, nil
	case v.IsUnsigned():
		uval, err := strconv.ParseUint(str, 10, 64)
		if err != nil {
			return EvalResult{}, fmt.Errorf("%v", err)
		}
		return EvalResult{uval: uval, typ: Uint64}, nil
	}

	// For other types, do best effort.
	if ival, err := strconv.ParseInt(str, 10, 64); err == nil {
		return EvalResult{ival: ival, typ: Int64}, nil
	}
	if uval, err := strconv.ParseUint(str, 10, 64); err == nil {
		return EvalResult{uval: uval, typ: Uint64}, nil
	}
	return EvalResult{}, fmt.Errorf("could not parse value: '%s'", str)
}
