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

package types

import (
	"github.com/pingcap/tidb/types"
)

func BindVariableFromDatum(value *types.Datum) (*BindVariable, bool) {
	switch value.Kind() {
	case types.KindNull:
		return NullBindVariable, true
	case types.KindInt64:
		return Int64BindVariable(value.GetInt64()), true
	case types.KindUint64:
		return Uint64BindVariable(value.GetUint64()), true
	case types.KindString, types.KindBinaryLiteral:
		return StringBindVariable(value.GetString()), true
	case types.KindBytes:
		return BytesBindVariable(value.GetBytes()), true
	case types.KindFloat32:
		return Float32BindVariable(value.GetFloat32()), true
	case types.KindFloat64:
		return Float64BindVariable(value.GetFloat64()), true
	case types.KindMysqlDuration:
		return Int64BindVariable(int64(value.GetMysqlDuration().Duration)), true
	case types.KindMysqlDecimal:
		return &BindVariable{
			Type:  Decimal,
			Value: []byte(value.GetString()),
		}, true
	case types.KindMysqlTime:
		return &BindVariable{
			Type:  Time,
			Value: []byte(value.GetString()),
		}, true
	default:
		return nil, false
	}
}
