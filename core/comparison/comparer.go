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

package comparison

import (
	"errors"
	"fmt"
	"reflect"
	"runtime"
	"strings"
)

func IsCompareSupported(value interface{}) bool {
	switch reflect.TypeOf(value).Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64,
		reflect.Float32, reflect.Float64,
		reflect.String:
		return true
	}
	return false
}

func Compare(a, b interface{}) (int, error) {
	kind, err := checkTypeEquals(a, b)
	if err != nil {
		return 0, err
	}

	switch kind {
	case reflect.Int:
		ia := a.(int)
		ib := b.(int)
		return CompareInt(ia, ib), nil
	case reflect.Int8:
		ia := a.(int8)
		ib := b.(int8)
		return CompareInt8(ia, ib), nil
	case reflect.Int16:
		ia := a.(int16)
		ib := b.(int16)
		return CompareInt16(ia, ib), nil
	case reflect.Int32:
		ia := a.(int32)
		ib := b.(int32)
		return CompareInt32(ia, ib), nil
	case reflect.Int64:
		ia := a.(int64)
		ib := b.(int64)
		return CompareInt64(ia, ib), nil

	case reflect.Uint:
		ia := a.(uint)
		ib := b.(uint)
		return CompareUInt(ia, ib), nil
	case reflect.Uint8:
		ia := a.(uint8)
		ib := b.(uint8)
		return CompareUInt8(ia, ib), nil
	case reflect.Uint16:
		ia := a.(uint16)
		ib := b.(uint16)
		return CompareUInt16(ia, ib), nil
	case reflect.Uint32:
		ia := a.(uint32)
		ib := b.(uint32)
		return CompareUInt32(ia, ib), nil
	case reflect.Uint64:
		ia := a.(uint64)
		ib := b.(uint64)
		return CompareUInt64(ia, ib), nil

	case reflect.Float32:
		ia := a.(float32)
		ib := b.(float32)
		return CompareFloat32(ia, ib), nil
	case reflect.Float64:
		ia := a.(float64)
		ib := b.(float64)
		return CompareFloat64(ia, ib), nil

	case reflect.String:
		ia := a.(string)
		ib := b.(string)
		return strings.Compare(ia, ib), nil
	}

	return 0, fmt.Errorf("unsupported type for comparison: %T", a)
}

func Min(a, b interface{}) (interface{}, error) {
	kind, err := checkTypeEquals(a, b)
	if err != nil {
		return 0, err
	}

	switch kind {
	case reflect.Int:
		ia := a.(int)
		ib := b.(int)
		return MinInt(ia, ib), nil
	case reflect.Int8:
		ia := a.(int8)
		ib := b.(int8)
		return MinInt8(ia, ib), nil
	case reflect.Int16:
		ia := a.(int16)
		ib := b.(int16)
		return MinInt16(ia, ib), nil
	case reflect.Int32:
		ia := a.(int32)
		ib := b.(int32)
		return MinInt32(ia, ib), nil
	case reflect.Int64:
		ia := a.(int64)
		ib := b.(int64)
		return MinInt64(ia, ib), nil

	case reflect.Uint:
		ia := a.(uint)
		ib := b.(uint)
		return MinUInt(ia, ib), nil
	case reflect.Uint8:
		ia := a.(uint8)
		ib := b.(uint8)
		return MinUInt8(ia, ib), nil
	case reflect.Uint16:
		ia := a.(uint16)
		ib := b.(uint16)
		return MinUInt16(ia, ib), nil
	case reflect.Uint32:
		ia := a.(uint32)
		ib := b.(uint32)
		return MinUInt32(ia, ib), nil
	case reflect.Uint64:
		ia := a.(uint64)
		ib := b.(uint64)
		return MinUInt64(ia, ib), nil

	case reflect.Float32:
		ia := a.(float32)
		ib := b.(float32)
		return MinFloat32(ia, ib), nil
	case reflect.Float64:
		ia := a.(float64)
		ib := b.(float64)
		return MinFloat64(ia, ib), nil

	case reflect.String:
		ia := a.(string)
		ib := b.(string)
		return MinString(ia, ib), nil
	}

	return 0, fmt.Errorf("unsupported type for min operation: %T", a)
}

func Max(a, b interface{}) (interface{}, error) {
	kind, err := checkTypeEquals(a, b)
	if err != nil {
		return 0, err
	}

	switch kind {
	case reflect.Int:
		ia := a.(int)
		ib := b.(int)
		return MaxInt(ia, ib), nil
	case reflect.Int8:
		ia := a.(int8)
		ib := b.(int8)
		return MaxInt8(ia, ib), nil
	case reflect.Int16:
		ia := a.(int16)
		ib := b.(int16)
		return MaxInt16(ia, ib), nil
	case reflect.Int32:
		ia := a.(int32)
		ib := b.(int32)
		return MaxInt32(ia, ib), nil
	case reflect.Int64:
		ia := a.(int64)
		ib := b.(int64)
		return MaxInt64(ia, ib), nil

	case reflect.Uint:
		ia := a.(uint)
		ib := b.(uint)
		return MaxUInt(ia, ib), nil
	case reflect.Uint8:
		ia := a.(uint8)
		ib := b.(uint8)
		return MaxUInt8(ia, ib), nil
	case reflect.Uint16:
		ia := a.(uint16)
		ib := b.(uint16)
		return MaxUInt16(ia, ib), nil
	case reflect.Uint32:
		ia := a.(uint32)
		ib := b.(uint32)
		return MaxUInt32(ia, ib), nil
	case reflect.Uint64:
		ia := a.(uint64)
		ib := b.(uint64)
		return MaxUInt64(ia, ib), nil

	case reflect.Float32:
		ia := a.(float32)
		ib := b.(float32)
		return MaxFloat32(ia, ib), nil
	case reflect.Float64:
		ia := a.(float64)
		ib := b.(float64)
		return MaxFloat64(ia, ib), nil

	case reflect.String:
		ia := a.(string)
		ib := b.(string)
		return MaxString(ia, ib), nil
	}

	return 0, fmt.Errorf("unsupported type for min operation: %T", a)
}

func isWindows() bool {
	sysType := runtime.GOOS
	return strings.ToLower(sysType) == "windows"
}

func checkTypeEquals(a, b interface{}) (reflect.Kind, error) {
	aType := reflect.TypeOf(a)
	bType := reflect.TypeOf(b)

	line := "\n"
	if isWindows() {
		line = "\r\n"
	}

	if aType.Kind() != bType.Kind() {
		return reflect.Invalid, errors.New(fmt.Sprint(
			"values have different types cannot be compared",
			line,
			fmt.Sprintf("type a: %#v", a),
			line,
			fmt.Sprintf("type b: %#v", b)))
	}
	return aType.Kind(), nil
}
