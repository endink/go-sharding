/*
Copyright 2019 The Vitess Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package mysql

import (
	"github.com/XiaoMi/Gaea/mysql/types"
)

// This file provides a few utility variables and methods, mostly for tests.
// The assumptions made about the types of fields and data returned
// by MySQl are validated in schema_test.go. This way all tests
// can use these variables and methods to simulate a MySQL server
// (using fakesqldb/ package for instance) and still be guaranteed correct
// data.

const (
	// BaseShowTables is the base query used in further methods.
	BaseShowTables = "SELECT table_name, table_type, unix_timestamp(create_time), table_comment FROM information_schema.tables WHERE table_schema = database()"

	// BaseShowPrimary is the base query for fetching primary key info.
	BaseShowPrimary = "SELECT table_name, column_name FROM information_schema.key_column_usage WHERE table_schema=database() AND constraint_name='PRIMARY' ORDER BY table_name, ordinal_position"
)

// BaseShowTablesFields contains the fields returned by a BaseShowTables or a BaseShowTablesForTable command.
// They are validated by the
// testBaseShowTables test.
var BaseShowTablesFields = []*types.Field{
	{
		Name:         "table_name",
		Type:         types.VarChar,
		Table:        "tables",
		OrgTable:     "TABLES",
		Database:     "information_schema",
		OrgName:      "TABLE_NAME",
		ColumnLength: 192,
		Charset:      uint32(DefaultCollationID),
		Flags:        uint32(types.MySqlFlag_NOT_NULL_FLAG),
	},
	{
		Name:         "table_type",
		Type:         types.VarChar,
		Table:        "tables",
		OrgTable:     "TABLES",
		Database:     "information_schema",
		OrgName:      "TABLE_TYPE",
		ColumnLength: 192,
		Charset:      uint32(DefaultCollationID),
		Flags:        uint32(types.MySqlFlag_NOT_NULL_FLAG),
	},
	{
		Name:         "unix_timestamp(create_time)",
		Type:         types.Int64,
		ColumnLength: 11,
		Charset:      uint32(CharsetBinary),
		Flags:        uint32(types.MySqlFlag_BINARY_FLAG | types.MySqlFlag_NUM_FLAG),
	},
	{
		Name:         "table_comment",
		Type:         types.VarChar,
		Table:        "tables",
		OrgTable:     "TABLES",
		Database:     "information_schema",
		OrgName:      "TABLE_COMMENT",
		ColumnLength: 6144,
		Charset:      uint32(DefaultCollationID),
		Flags:        uint32(types.MySqlFlag_NOT_NULL_FLAG),
	},
}

// BaseShowTablesRow returns the fields from a BaseShowTables or
// BaseShowTablesForTable command.
func BaseShowTablesRow(tableName string, isView bool, comment string) []types.Value {
	tableType := "BASE TABLE"
	if isView {
		tableType = "VIEW"
	}
	return []types.Value{
		types.MakeTrusted(types.VarChar, []byte(tableName)),
		types.MakeTrusted(types.VarChar, []byte(tableType)),
		types.MakeTrusted(types.Int64, []byte("1427325875")), // unix_timestamp(create_time)
		types.MakeTrusted(types.VarChar, []byte(comment)),
	}
}

// ShowPrimaryFields contains the fields for a BaseShowPrimary.
var ShowPrimaryFields = []*types.Field{{
	Name: "table_name",
	Type: types.VarChar,
}, {
	Name: "column_name",
	Type: types.VarChar,
}}

// ShowPrimaryRow returns a row for a primary key column.
func ShowPrimaryRow(tableName, colName string) []types.Value {
	return []types.Value{
		types.MakeTrusted(types.VarChar, []byte(tableName)),
		types.MakeTrusted(types.VarChar, []byte(colName)),
	}
}
