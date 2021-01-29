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

type Field struct {
	// name of the field as returned by mysql C API
	Name string
	// vitess-defined type. Conversion function is in sqltypes package.
	Type MySqlType
	// Remaining fields from mysql C API.
	// These fields are only populated when ExecuteOptions.included_fields
	// is set to IncludedFields.ALL.
	Table    string
	OrgTable string
	Database string
	OrgName  string
	// column_length is really a uint32. All 32 bits can be used.
	ColumnLength uint32
	// charset is actually a uint16. Only the lower 16 bits are used.
	Charset uint32
	// decimals is actually a uint8. Only the lower 8 bits are used.
	Decimals uint32
	// flags is actually a uint16. Only the lower 16 bits are used.
	Flags uint32
	// column_type is optionally populated from information_schema.columns
	ColumnType string
}
