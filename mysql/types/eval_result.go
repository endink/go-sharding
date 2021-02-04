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

type EvalResult struct {
	typ   MySqlType
	ival  int64
	uval  uint64
	fval  float64
	bytes []byte
}

// newEvalResult parses a value and produces an EvalResult containing the value
func newEvalResult(v Value) (EvalResult, error) {
	raw := v.Value
	switch {
	case v.IsBinary() || v.IsText():
		return EvalResult{bytes: raw, typ: VarBinary}, nil
	case v.IsSigned():
		ival, err := strconv.ParseInt(string(raw), 10, 64)
		if err != nil {
			return EvalResult{}, fmt.Errorf("%v", err)
		}
		return EvalResult{ival: ival, typ: Int64}, nil
	case v.IsUnsigned():
		uval, err := strconv.ParseUint(string(raw), 10, 64)
		if err != nil {
			return EvalResult{}, fmt.Errorf("%v", err)
		}
		return EvalResult{uval: uval, typ: Uint64}, nil
	case v.IsFloat() || v.ValueType == Decimal:
		fval, err := strconv.ParseFloat(string(raw), 64)
		if err != nil {
			return EvalResult{}, fmt.Errorf("%v", err)
		}
		return EvalResult{fval: fval, typ: Float64}, nil
	default:
		return EvalResult{typ: v.ValueType, bytes: raw}, nil
	}
}

//Value allows for retrieval of the value we expose for public consumption
func (e EvalResult) Value() Value {
	return e.toSQLValue(e.typ)
}

func (v EvalResult) toSQLValue(resultType MySqlType) Value {
	switch {
	case IsSigned(resultType):
		switch v.typ {
		case Int64:
			return MakeTrusted(resultType, strconv.AppendInt(nil, v.ival, 10))
		case Uint64:
			return MakeTrusted(resultType, strconv.AppendInt(nil, int64(v.uval), 10))
		case Float64:
			return MakeTrusted(resultType, strconv.AppendInt(nil, int64(v.fval), 10))
		}
	case IsUnsigned(resultType):
		switch v.typ {
		case Uint64:
			return MakeTrusted(resultType, strconv.AppendUint(nil, v.uval, 10))
		case Int64:
			return MakeTrusted(resultType, strconv.AppendUint(nil, uint64(v.ival), 10))
		case Float64:
			return MakeTrusted(resultType, strconv.AppendUint(nil, uint64(v.fval), 10))
		}
	case IsFloat(resultType) || resultType == Decimal:
		switch v.typ {
		case Int64:
			return MakeTrusted(resultType, strconv.AppendInt(nil, v.ival, 10))
		case Uint64:
			return MakeTrusted(resultType, strconv.AppendUint(nil, v.uval, 10))
		case Float64:
			format := byte('g')
			if resultType == Decimal {
				format = 'f'
			}
			return MakeTrusted(resultType, strconv.AppendFloat(nil, v.fval, format, -1, 64))
		}
	default:
		return MakeTrusted(resultType, v.bytes)
	}
	return NULL
}
