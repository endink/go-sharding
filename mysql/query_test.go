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
	"fmt"
	"github.com/endink/go-sharding/mysql/types"
	"reflect"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Utility function to write sql query as packets to test parseComPrepare
func MockQueryPackets(t *testing.T, query string) []byte {
	data := make([]byte, len(query)+1+packetHeaderSize)
	// Not sure if it makes a difference
	pos := packetHeaderSize
	pos = writeByte(data, pos, ComPrepare)
	copy(data[pos:], query)
	return data
}

func MockPrepareData(t *testing.T) (*PrepareData, *types.Result) {
	sql := "select * from test_table where id = ?"

	result := &types.Result{
		Fields: []*types.Field{
			{
				Name: "id",
				Type: types.Int32,
			},
		},
		Rows: [][]types.Value{
			{
				types.MakeTrusted(types.Int32, []byte("1")),
			},
		},
		RowsAffected: 1,
	}

	prepare := &PrepareData{
		StatementID: 18,
		PrepareStmt: sql,
		ParamsCount: 1,
		ParamsType:  []int32{263},
		ColumnNames: []string{"id"},
		BindVars: map[string]*types.BindVariable{
			"v1": types.Int32BindVariable(10),
		},
	}

	return prepare, result
}

func TestComInitDB(t *testing.T) {
	listener, sConn, cConn := createSocketPair(t)
	defer func() {
		listener.Close()
		sConn.Close()
		cConn.Close()
	}()

	// Write ComInitDB packet, read it, compare.
	if err := cConn.writeComInitDB("my_db"); err != nil {
		t.Fatalf("writeComInitDB failed: %v", err)
	}
	data, err := sConn.ReadPacket()
	if err != nil || len(data) == 0 || data[0] != ComInitDB {
		t.Fatalf("sConn.ReadPacket - ComInitDB failed: %v %v", data, err)
	}
	db := sConn.parseComInitDB(data)
	if db != "my_db" {
		t.Errorf("parseComInitDB returned unexpected data: %v", db)
	}
}

func TestComSetOption(t *testing.T) {
	listener, sConn, cConn := createSocketPair(t)
	defer func() {
		listener.Close()
		sConn.Close()
		cConn.Close()
	}()

	// Write ComSetOption packet, read it, compare.
	if err := cConn.writeComSetOption(1); err != nil {
		t.Fatalf("writeComSetOption failed: %v", err)
	}
	data, err := sConn.ReadPacket()
	if err != nil || len(data) == 0 || data[0] != ComSetOption {
		t.Fatalf("sConn.ReadPacket - ComSetOption failed: %v %v", data, err)
	}
	operation, ok := sConn.parseComSetOption(data)
	if !ok {
		t.Fatalf("parseComSetOption failed unexpectedly")
	}
	if operation != 1 {
		t.Errorf("parseComSetOption returned unexpected data: %v", operation)
	}
}

func TestComStmtPrepare(t *testing.T) {
	listener, sConn, cConn := createSocketPair(t)
	defer func() {
		listener.Close()
		sConn.Close()
		cConn.Close()
	}()

	sql := "select * from test_table where id = ?"
	mockData := MockQueryPackets(t, sql)

	if err := cConn.writePacket(mockData); err != nil {
		t.Fatalf("writePacket failed: %v", err)
	}

	data, err := sConn.ReadPacket()
	if err != nil {
		t.Fatalf("sConn.ReadPacket - ComPrepare failed: %v", err)
	}

	parsedQuery := sConn.parseComPrepare(data)
	if parsedQuery != sql {
		t.Fatalf("Received incorrect query, want: %v, got: %v", sql, parsedQuery)
	}

	prepare, result := MockPrepareData(t)
	sConn.PrepareData = make(map[uint32]*PrepareData)
	sConn.PrepareData[prepare.StatementID] = prepare

	// write the response to the client
	if err := sConn.writePrepare(result.Fields, prepare); err != nil {
		t.Fatalf("sConn.writePrepare failed: %v", err)
	}

	resp, err := cConn.ReadPacket()
	if err != nil {
		t.Fatalf("cConn.ReadPacket failed: %v", err)
	}
	if uint32(resp[1]) != prepare.StatementID {
		t.Fatalf("Received incorrect Statement ID, want: %v, got: %v", prepare.StatementID, resp[1])
	}
}

func TestComStmtPrepareUpdStmt(t *testing.T) {
	listener, sConn, cConn := createSocketPair(t)
	defer func() {
		listener.Close()
		sConn.Close()
		cConn.Close()
	}()

	sql := "UPDATE test SET __bit = ?, __tinyInt = ?, __tinyIntU = ?, __smallInt = ?, __smallIntU = ?, __mediumInt = ?, __mediumIntU = ?, __int = ?, __intU = ?, __bigInt = ?, __bigIntU = ?, __decimal = ?, __float = ?, __double = ?, __date = ?, __datetime = ?, __timestamp = ?, __time = ?, __year = ?, __char = ?, __varchar = ?, __binary = ?, __varbinary = ?, __tinyblob = ?, __tinytext = ?, __blob = ?, __text = ?, __enum = ?, __set = ? WHERE __id = 0"
	mockData := MockQueryPackets(t, sql)

	err := cConn.writePacket(mockData)
	require.NoError(t, err, "writePacket failed")

	data, err := sConn.ReadPacket()
	require.NoError(t, err, "sConn.ReadPacket - ComPrepare failed")

	parsedQuery := sConn.parseComPrepare(data)
	require.Equal(t, sql, parsedQuery, "Received incorrect query")

	paramsCount := uint16(29)
	prepare := &PrepareData{
		StatementID: 1,
		PrepareStmt: sql,
		ParamsCount: paramsCount,
	}
	sConn.PrepareData = make(map[uint32]*PrepareData)
	sConn.PrepareData[prepare.StatementID] = prepare

	// write the response to the client
	err = sConn.writePrepare(nil, prepare)
	require.NoError(t, err, "sConn.writePrepare failed")

	resp, err := cConn.ReadPacket()
	require.NoError(t, err, "cConn.ReadPacket failed")
	require.EqualValues(t, prepare.StatementID, resp[1], "Received incorrect Statement ID")

	for i := uint16(0); i < paramsCount; i++ {
		resp, err := cConn.ReadPacket()
		require.NoError(t, err, "cConn.ReadPacket failed")
		require.EqualValues(t, 0xfd, resp[17], "Received incorrect Statement ID")
	}
}

func TestComStmtSendLongData(t *testing.T) {
	listener, sConn, cConn := createSocketPair(t)
	defer func() {
		listener.Close()
		sConn.Close()
		cConn.Close()
	}()

	prepare, result := MockPrepareData(t)
	cConn.PrepareData = make(map[uint32]*PrepareData)
	cConn.PrepareData[prepare.StatementID] = prepare
	if err := cConn.writePrepare(result.Fields, prepare); err != nil {
		t.Fatalf("writePrepare failed: %v", err)
	}

	// Since there's no writeComStmtSendLongData, we'll write a prepareStmt and check if we can read the StatementID
	data, err := sConn.ReadPacket()
	if err != nil || len(data) == 0 {
		t.Fatalf("sConn.ReadPacket - ComStmtClose failed: %v %v", data, err)
	}
	stmtID, paramID, chunkData, ok := sConn.parseComStmtSendLongData(data)
	if !ok {
		t.Fatalf("parseComStmtSendLongData failed")
	}
	if paramID != 1 {
		t.Fatalf("Received incorrect ParamID, want %v, got %v:", paramID, 1)
	}
	if stmtID != prepare.StatementID {
		t.Fatalf("Received incorrect value, want: %v, got: %v", uint32(data[1]), prepare.StatementID)
	}
	// Check length of chunkData, Since its a subset of `data` and compare with it after we subtract the number of bytes that was read from it.
	// sizeof(uint32) + sizeof(uint16) + 1 = 7
	if len(chunkData) != len(data)-7 {
		t.Fatalf("Received bad chunkData")
	}
}

func TestComStmtExecute(t *testing.T) {
	listener, sConn, cConn := createSocketPair(t)
	defer func() {
		listener.Close()
		sConn.Close()
		cConn.Close()
	}()

	prepare, _ := MockPrepareData(t)
	cConn.PrepareData = make(map[uint32]*PrepareData)
	cConn.PrepareData[prepare.StatementID] = prepare

	// This is simulated packets for `select * from test_table where id = ?`
	data := []byte{23, 18, 0, 0, 0, 128, 1, 0, 0, 0, 0, 1, 1, 128, 1}

	stmtID, _, err := sConn.parseComStmtExecute(cConn.PrepareData, data)
	if err != nil {
		t.Fatalf("parseComStmtExeute failed: %v", err)
	}
	if stmtID != 18 {
		t.Fatalf("Parsed incorrect values")
	}
}

func TestComStmtExecuteUpdStmt(t *testing.T) {
	listener, sConn, cConn := createSocketPair(t)
	defer func() {
		listener.Close()
		sConn.Close()
		cConn.Close()
	}()

	prepareDataMap := map[uint32]*PrepareData{
		1: {
			StatementID: 1,
			ParamsCount: 29,
			ParamsType:  make([]int32, 29),
			BindVars:    map[string]*types.BindVariable{},
		}}

	// This is simulated packets for update query
	data := []byte{
		0x29, 0x01, 0x00, 0x00, 0x17, 0x01, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x01, 0x10, 0x00, 0x01, 0x00, 0x01, 0x80, 0x02, 0x00, 0x02, 0x80, 0x03, 0x00, 0x03,
		0x80, 0x03, 0x00, 0x03, 0x80, 0x08, 0x00, 0x08, 0x80, 0x00, 0x00, 0x04, 0x00, 0x05, 0x00, 0x0a,
		0x00, 0x0c, 0x00, 0x07, 0x00, 0x0b, 0x00, 0x0d, 0x80, 0xfe, 0x00, 0xfe, 0x00, 0xfc, 0x00, 0xfc,
		0x00, 0xfc, 0x00, 0xfe, 0x00, 0xfc, 0x00, 0xfe, 0x00, 0xfe, 0x00, 0xfe, 0x00, 0x08, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0xaa, 0xe0, 0x80, 0xff, 0x00, 0x80, 0xff, 0xff, 0x00, 0x00, 0x80, 0xff,
		0xff, 0xff, 0xff, 0x00, 0x00, 0x00, 0x00, 0x80, 0xff, 0xff, 0xff, 0xff, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x80, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x15, 0x31, 0x32, 0x33,
		0x34, 0x35, 0x36, 0x37, 0x38, 0x39, 0x30, 0x2e, 0x30, 0x31, 0x32, 0x33, 0x34, 0x35, 0x36, 0x37,
		0x38, 0x39, 0xd0, 0x0f, 0x49, 0x40, 0x44, 0x17, 0x41, 0x54, 0xfb, 0x21, 0x09, 0x40, 0x04, 0xe0,
		0x07, 0x08, 0x08, 0x0b, 0xe0, 0x07, 0x08, 0x08, 0x11, 0x19, 0x3b, 0x00, 0x00, 0x00, 0x00, 0x0b,
		0xe0, 0x07, 0x08, 0x08, 0x11, 0x19, 0x3b, 0x00, 0x00, 0x00, 0x00, 0x0c, 0x01, 0x08, 0x00, 0x00,
		0x00, 0x07, 0x3b, 0x3b, 0x00, 0x00, 0x00, 0x00, 0x04, 0x31, 0x39, 0x39, 0x39, 0x08, 0x31, 0x32,
		0x33, 0x34, 0x35, 0x36, 0x37, 0x38, 0x0c, 0xe9, 0x9f, 0xa9, 0xe5, 0x86, 0xac, 0xe7, 0x9c, 0x9f,
		0xe8, 0xb5, 0x9e, 0x08, 0x31, 0x32, 0x33, 0x34, 0x35, 0x36, 0x37, 0x38, 0x08, 0x31, 0x32, 0x33,
		0x34, 0x35, 0x36, 0x37, 0x38, 0x08, 0x31, 0x32, 0x33, 0x34, 0x35, 0x36, 0x37, 0x38, 0x0c, 0xe9,
		0x9f, 0xa9, 0xe5, 0x86, 0xac, 0xe7, 0x9c, 0x9f, 0xe8, 0xb5, 0x9e, 0x08, 0x31, 0x32, 0x33, 0x34,
		0x35, 0x36, 0x37, 0x38, 0x0c, 0xe9, 0x9f, 0xa9, 0xe5, 0x86, 0xac, 0xe7, 0x9c, 0x9f, 0xe8, 0xb5,
		0x9e, 0x03, 0x66, 0x6f, 0x6f, 0x07, 0x66, 0x6f, 0x6f, 0x2c, 0x62, 0x61, 0x72}

	stmtID, _, err := sConn.parseComStmtExecute(prepareDataMap, data[4:]) // first 4 are header
	require.NoError(t, err)
	require.EqualValues(t, 1, stmtID)

	prepData := prepareDataMap[stmtID]
	assert.EqualValues(t, types.Bit, prepData.ParamsType[0], "got: %s", types.MySqlType(prepData.ParamsType[0]))
	assert.EqualValues(t, types.Int8, prepData.ParamsType[1], "got: %s", types.MySqlType(prepData.ParamsType[1]))
	assert.EqualValues(t, types.Int8, prepData.ParamsType[2], "got: %s", types.MySqlType(prepData.ParamsType[2]))
	assert.EqualValues(t, types.Int16, prepData.ParamsType[3], "got: %s", types.MySqlType(prepData.ParamsType[3]))
	assert.EqualValues(t, types.Int16, prepData.ParamsType[4], "got: %s", types.MySqlType(prepData.ParamsType[4]))
	assert.EqualValues(t, types.Int32, prepData.ParamsType[5], "got: %s", types.MySqlType(prepData.ParamsType[5]))
	assert.EqualValues(t, types.Int32, prepData.ParamsType[6], "got: %s", types.MySqlType(prepData.ParamsType[6]))
	assert.EqualValues(t, types.Int32, prepData.ParamsType[7], "got: %s", types.MySqlType(prepData.ParamsType[7]))
	assert.EqualValues(t, types.Int32, prepData.ParamsType[8], "got: %s", types.MySqlType(prepData.ParamsType[8]))
	assert.EqualValues(t, types.Int64, prepData.ParamsType[9], "got: %s", types.MySqlType(prepData.ParamsType[9]))
	assert.EqualValues(t, types.Int64, prepData.ParamsType[10], "got: %s", types.MySqlType(prepData.ParamsType[10]))
	assert.EqualValues(t, types.Decimal, prepData.ParamsType[11], "got: %s", types.MySqlType(prepData.ParamsType[11]))
	assert.EqualValues(t, types.Float32, prepData.ParamsType[12], "got: %s", types.MySqlType(prepData.ParamsType[12]))
	assert.EqualValues(t, types.Float64, prepData.ParamsType[13], "got: %s", types.MySqlType(prepData.ParamsType[13]))
	assert.EqualValues(t, types.Date, prepData.ParamsType[14], "got: %s", types.MySqlType(prepData.ParamsType[14]))
	assert.EqualValues(t, types.Datetime, prepData.ParamsType[15], "got: %s", types.MySqlType(prepData.ParamsType[15]))
	assert.EqualValues(t, types.Timestamp, prepData.ParamsType[16], "got: %s", types.MySqlType(prepData.ParamsType[16]))
	assert.EqualValues(t, types.Time, prepData.ParamsType[17], "got: %s", types.MySqlType(prepData.ParamsType[17]))

	// this is year but in binary it is changed to varbinary
	assert.EqualValues(t, types.VarBinary, prepData.ParamsType[18], "got: %s", types.MySqlType(prepData.ParamsType[18]))

	assert.EqualValues(t, types.Char, prepData.ParamsType[19], "got: %s", types.MySqlType(prepData.ParamsType[19]))
	assert.EqualValues(t, types.Char, prepData.ParamsType[20], "got: %s", types.MySqlType(prepData.ParamsType[20]))
	assert.EqualValues(t, types.Text, prepData.ParamsType[21], "got: %s", types.MySqlType(prepData.ParamsType[21]))
	assert.EqualValues(t, types.Text, prepData.ParamsType[22], "got: %s", types.MySqlType(prepData.ParamsType[22]))
	assert.EqualValues(t, types.Text, prepData.ParamsType[23], "got: %s", types.MySqlType(prepData.ParamsType[23]))
	assert.EqualValues(t, types.Char, prepData.ParamsType[24], "got: %s", types.MySqlType(prepData.ParamsType[24]))
	assert.EqualValues(t, types.Text, prepData.ParamsType[25], "got: %s", types.MySqlType(prepData.ParamsType[25]))
	assert.EqualValues(t, types.Char, prepData.ParamsType[26], "got: %s", types.MySqlType(prepData.ParamsType[26]))
	assert.EqualValues(t, types.Char, prepData.ParamsType[27], "got: %s", types.MySqlType(prepData.ParamsType[27]))
	assert.EqualValues(t, types.Char, prepData.ParamsType[28], "got: %s", types.MySqlType(prepData.ParamsType[28]))
}

func TestComStmtClose(t *testing.T) {
	listener, sConn, cConn := createSocketPair(t)
	defer func() {
		listener.Close()
		sConn.Close()
		cConn.Close()
	}()

	prepare, result := MockPrepareData(t)
	cConn.PrepareData = make(map[uint32]*PrepareData)
	cConn.PrepareData[prepare.StatementID] = prepare
	if err := cConn.writePrepare(result.Fields, prepare); err != nil {
		t.Fatalf("writePrepare failed: %v", err)
	}

	// Since there's no writeComStmtClose, we'll write a prepareStmt and check if we can read the StatementID
	data, err := sConn.ReadPacket()
	if err != nil || len(data) == 0 {
		t.Fatalf("sConn.ReadPacket - ComStmtClose failed: %v %v", data, err)
	}
	stmtID, ok := sConn.parseComStmtClose(data)
	if !ok {
		t.Fatalf("parseComStmtClose failed")
	}
	if stmtID != prepare.StatementID {
		t.Fatalf("Received incorrect value, want: %v, got: %v", uint32(data[1]), prepare.StatementID)
	}
}

func TestQueries(t *testing.T) {
	listener, sConn, cConn := createSocketPair(t)
	defer func() {
		listener.Close()
		sConn.Close()
		cConn.Close()
	}()

	// Smallest result
	checkQuery(t, "tiny", sConn, cConn, &types.Result{})

	// Typical Insert result
	checkQuery(t, "insert", sConn, cConn, &types.Result{
		RowsAffected: 0x8010203040506070,
		InsertID:     0x0102030405060708,
	})

	// Typical Select with TYPE_AND_NAME.
	// One value is also NULL.
	checkQuery(t, "type and name", sConn, cConn, &types.Result{
		Fields: []*types.Field{
			{
				Name: "id",
				Type: types.Int32,
			},
			{
				Name: "name",
				Type: types.VarChar,
			},
		},
		Rows: [][]types.Value{
			{
				types.MakeTrusted(types.Int32, []byte("10")),
				types.MakeTrusted(types.VarChar, []byte("nice name")),
			},
			{
				types.MakeTrusted(types.Int32, []byte("20")),
				types.NULL,
			},
		},
		RowsAffected: 2,
	})

	// Typical Select with TYPE_AND_NAME.
	// All types are represented.
	// One row has all NULL values.
	checkQuery(t, "all types", sConn, cConn, &types.Result{
		Fields: []*types.Field{
			{Name: "Type_INT8     ", Type: types.Int8},
			{Name: "Type_UINT8    ", Type: types.Uint8},
			{Name: "Type_INT16    ", Type: types.Int16},
			{Name: "Type_UINT16   ", Type: types.Uint16},
			{Name: "Type_INT24    ", Type: types.Int24},
			{Name: "Type_UINT24   ", Type: types.Uint24},
			{Name: "Type_INT32    ", Type: types.Int32},
			{Name: "Type_UINT32   ", Type: types.Uint32},
			{Name: "Type_INT64    ", Type: types.Int64},
			{Name: "Type_UINT64   ", Type: types.Uint64},
			{Name: "Type_FLOAT32  ", Type: types.Float32},
			{Name: "Type_FLOAT64  ", Type: types.Float64},
			{Name: "Type_TIMESTAMP", Type: types.Timestamp},
			{Name: "Type_DATE     ", Type: types.Date},
			{Name: "Type_TIME     ", Type: types.Time},
			{Name: "Type_DATETIME ", Type: types.Datetime},
			{Name: "Type_YEAR     ", Type: types.Year},
			{Name: "Type_DECIMAL  ", Type: types.Decimal},
			{Name: "Type_TEXT     ", Type: types.Text},
			{Name: "Type_BLOB     ", Type: types.Blob},
			{Name: "Type_VARCHAR  ", Type: types.VarChar},
			{Name: "Type_VARBINARY", Type: types.VarBinary},
			{Name: "Type_CHAR     ", Type: types.Char},
			{Name: "Type_BINARY   ", Type: types.Binary},
			{Name: "Type_BIT      ", Type: types.Bit},
			{Name: "Type_ENUM     ", Type: types.Enum},
			{Name: "Type_SET      ", Type: types.Set},
			// Skip TUPLE, not possible in Result.
			{Name: "Type_GEOMETRY ", Type: types.Geometry},
			{Name: "Type_JSON     ", Type: types.Json},
		},
		Rows: [][]types.Value{
			{
				types.MakeTrusted(types.Int8, []byte("Type_INT8")),
				types.MakeTrusted(types.Uint8, []byte("Type_UINT8")),
				types.MakeTrusted(types.Int16, []byte("Type_INT16")),
				types.MakeTrusted(types.Uint16, []byte("Type_UINT16")),
				types.MakeTrusted(types.Int24, []byte("Type_INT24")),
				types.MakeTrusted(types.Uint24, []byte("Type_UINT24")),
				types.MakeTrusted(types.Int32, []byte("Type_INT32")),
				types.MakeTrusted(types.Uint32, []byte("Type_UINT32")),
				types.MakeTrusted(types.Int64, []byte("Type_INT64")),
				types.MakeTrusted(types.Uint64, []byte("Type_UINT64")),
				types.MakeTrusted(types.Float32, []byte("Type_FLOAT32")),
				types.MakeTrusted(types.Float64, []byte("Type_FLOAT64")),
				types.MakeTrusted(types.Timestamp, []byte("Type_TIMESTAMP")),
				types.MakeTrusted(types.Date, []byte("Type_DATE")),
				types.MakeTrusted(types.Time, []byte("Type_TIME")),
				types.MakeTrusted(types.Datetime, []byte("Type_DATETIME")),
				types.MakeTrusted(types.Year, []byte("Type_YEAR")),
				types.MakeTrusted(types.Decimal, []byte("Type_DECIMAL")),
				types.MakeTrusted(types.Text, []byte("Type_TEXT")),
				types.MakeTrusted(types.Blob, []byte("Type_BLOB")),
				types.MakeTrusted(types.VarChar, []byte("Type_VARCHAR")),
				types.MakeTrusted(types.VarBinary, []byte("Type_VARBINARY")),
				types.MakeTrusted(types.Char, []byte("Type_CHAR")),
				types.MakeTrusted(types.Binary, []byte("Type_BINARY")),
				types.MakeTrusted(types.Bit, []byte("Type_BIT")),
				types.MakeTrusted(types.Enum, []byte("Type_ENUM")),
				types.MakeTrusted(types.Set, []byte("Type_SET")),
				types.MakeTrusted(types.Geometry, []byte("Type_GEOMETRY")),
				types.MakeTrusted(types.Json, []byte("Type_JSON")),
			},
			{
				types.NULL,
				types.NULL,
				types.NULL,
				types.NULL,
				types.NULL,
				types.NULL,
				types.NULL,
				types.NULL,
				types.NULL,
				types.NULL,
				types.NULL,
				types.NULL,
				types.NULL,
				types.NULL,
				types.NULL,
				types.NULL,
				types.NULL,
				types.NULL,
				types.NULL,
				types.NULL,
				types.NULL,
				types.NULL,
				types.NULL,
				types.NULL,
				types.NULL,
				types.NULL,
				types.NULL,
				types.NULL,
				types.NULL,
			},
		},
		RowsAffected: 2,
	})

	// Typical Select with TYPE_AND_NAME.
	// First value first column is an empty string, so it's encoded as 0.
	checkQuery(t, "first empty string", sConn, cConn, &types.Result{
		Fields: []*types.Field{
			{
				Name: "name",
				Type: types.VarChar,
			},
		},
		Rows: [][]types.Value{
			{
				types.MakeTrusted(types.VarChar, []byte("")),
			},
			{
				types.MakeTrusted(types.VarChar, []byte("nice name")),
			},
		},
		RowsAffected: 2,
	})

	// Typical Select with TYPE_ONLY.
	checkQuery(t, "type only", sConn, cConn, &types.Result{
		Fields: []*types.Field{
			{
				Type: types.Int64,
			},
		},
		Rows: [][]types.Value{
			{
				types.MakeTrusted(types.Int64, []byte("10")),
			},
			{
				types.MakeTrusted(types.Int64, []byte("20")),
			},
		},
		RowsAffected: 2,
	})

	// Typical Select with ALL.
	checkQuery(t, "complete", sConn, cConn, &types.Result{
		Fields: []*types.Field{
			{
				Type:         types.Int64,
				Name:         "cool column name",
				Table:        "table name",
				OrgTable:     "org table",
				Database:     "fine db",
				OrgName:      "crazy org",
				ColumnLength: 0x80020304,
				Charset:      0x1234,
				Decimals:     36,
				Flags: uint32(types.MySqlFlag_NOT_NULL_FLAG |
					types.MySqlFlag_PRI_KEY_FLAG |
					types.MySqlFlag_PART_KEY_FLAG |
					types.MySqlFlag_NUM_FLAG),
			},
		},
		Rows: [][]types.Value{
			{
				types.MakeTrusted(types.Int64, []byte("10")),
			},
			{
				types.MakeTrusted(types.Int64, []byte("20")),
			},
			{
				types.MakeTrusted(types.Int64, []byte("30")),
			},
		},
		RowsAffected: 3,
	})
}

func checkQuery(t *testing.T, query string, sConn, cConn *Conn, result *types.Result) {
	// The protocol depends on the CapabilityClientDeprecateEOF flag.
	// So we want to test both cases.

	sConn.Capabilities = 0
	cConn.Capabilities = 0
	checkQueryInternal(t, query, sConn, cConn, result, true /* wantfields */, true /* allRows */, false /* warnings */)
	checkQueryInternal(t, query, sConn, cConn, result, false /* wantfields */, true /* allRows */, false /* warnings */)
	checkQueryInternal(t, query, sConn, cConn, result, true /* wantfields */, false /* allRows */, false /* warnings */)
	checkQueryInternal(t, query, sConn, cConn, result, false /* wantfields */, false /* allRows */, false /* warnings */)

	checkQueryInternal(t, query, sConn, cConn, result, true /* wantfields */, true /* allRows */, true /* warnings */)

	sConn.Capabilities = CapabilityClientDeprecateEOF
	cConn.Capabilities = CapabilityClientDeprecateEOF
	checkQueryInternal(t, query, sConn, cConn, result, true /* wantfields */, true /* allRows */, false /* warnings */)
	checkQueryInternal(t, query, sConn, cConn, result, false /* wantfields */, true /* allRows */, false /* warnings */)
	checkQueryInternal(t, query, sConn, cConn, result, true /* wantfields */, false /* allRows */, false /* warnings */)
	checkQueryInternal(t, query, sConn, cConn, result, false /* wantfields */, false /* allRows */, false /* warnings */)

	checkQueryInternal(t, query, sConn, cConn, result, true /* wantfields */, true /* allRows */, true /* warnings */)
}

func checkQueryInternal(t *testing.T, query string, sConn, cConn *Conn, result *types.Result, wantfields, allRows, warnings bool) {

	if sConn.Capabilities&CapabilityClientDeprecateEOF > 0 {
		query += " NOEOF"
	} else {
		query += " EOF"
	}
	if wantfields {
		query += " FIELDS"
	} else {
		query += " NOFIELDS"
	}
	if allRows {
		query += " ALL"
	} else {
		query += " PARTIAL"
	}

	var warningCount uint16
	if warnings {
		query += " WARNINGS"
		warningCount = 99
	} else {
		query += " NOWARNINGS"
	}

	var fatalError string
	// Use a go routine to run ExecuteFetch.
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()

		// Test ExecuteFetch.
		maxrows := 10000
		if !allRows {
			// Asking for just one row max. The results that have more will fail.
			maxrows = 1
		}
		got, gotWarnings, err := cConn.ExecuteFetchWithWarningCount(query, maxrows, wantfields)
		if !allRows && len(result.Rows) > 1 {
			if err == nil {
				t.Errorf("ExecuteFetch should have failed but got: %v", got)
			}
			sqlErr, ok := err.(*SQLError)
			if !ok || sqlErr.Number() != ERVitessMaxRowsExceeded {
				t.Errorf("Expected ERVitessMaxRowsExceeded %v, got %v", ERVitessMaxRowsExceeded, sqlErr.Number())
			}
			return
		}
		if err != nil {
			fatalError = fmt.Sprintf("executeFetch failed: %v", err)
			return
		}
		expected := *result
		if !wantfields {
			expected.Fields = nil
		}
		if !got.Equal(&expected) {
			for i, f := range got.Fields {
				if i < len(expected.Fields) && !types.FieldEqual(f, expected.Fields[i]) {
					t.Logf("Got      field(%v) = %v", i, f)
					t.Logf("Expected field(%v) = %v", i, expected.Fields[i])
				}
			}
			fatalError = fmt.Sprintf("ExecuteFetch(wantfields=%v) returned:\n%v\nBut was expecting:\n%v", wantfields, got, expected)
			return
		}

		if gotWarnings != warningCount {
			t.Errorf("ExecuteFetch(%v) expected %v warnings got %v", query, warningCount, gotWarnings)
		}

		// Test ExecuteStreamFetch, build a Result.
		expected = *result
		if err := cConn.ExecuteStreamFetch(query); err != nil {
			fatalError = fmt.Sprintf("ExecuteStreamFetch(%v) failed: %v", query, err)
			return
		}
		got = &types.Result{}
		got.RowsAffected = result.RowsAffected
		got.InsertID = result.InsertID
		got.Fields, err = cConn.Fields()
		if err != nil {
			fatalError = fmt.Sprintf("Fields(%v) failed: %v", query, err)
			return
		}
		if len(got.Fields) == 0 {
			got.Fields = nil
		}
		for {
			row, err := cConn.FetchNext()
			if err != nil {
				fatalError = fmt.Sprintf("FetchNext(%v) failed: %v", query, err)
				return
			}
			if row == nil {
				// Done.
				break
			}
			got.Rows = append(got.Rows, row)
		}
		cConn.CloseResult()

		if !got.Equal(&expected) {
			for i, f := range got.Fields {
				if i < len(expected.Fields) && !types.FieldEqual(f, expected.Fields[i]) {
					t.Logf("========== Got      field(%v) = %v", i, f)
					t.Logf("========== Expected field(%v) = %v", i, expected.Fields[i])
				}
			}
			for i, row := range got.Rows {
				if i < len(expected.Rows) && !reflect.DeepEqual(row, expected.Rows[i]) {
					t.Logf("========== Got      row(%v) = %v", i, RowString(row))
					t.Logf("========== Expected row(%v) = %v", i, RowString(expected.Rows[i]))
				}
			}
			t.Errorf("\nExecuteStreamFetch(%v) returned:\n%+v\nBut was expecting:\n%+v\n", query, got, &expected)
		}
	}()

	// The other side gets the request, and sends the result.
	// Twice, once for ExecuteFetch, once for ExecuteStreamFetch.
	count := 2
	if !allRows && len(result.Rows) > 1 {
		// short-circuit one test, the go routine returned and didn't
		// do the streaming query.
		count--
	}

	handler := testHandler{
		result:   result,
		warnings: warningCount,
	}

	for i := 0; i < count; i++ {
		kontinue := sConn.handleNextCommand(&handler)
		if !kontinue {
			t.Fatalf("error handling command: %d", i)
		}
	}

	wg.Wait()

	if fatalError != "" {
		t.Fatalf(fatalError)
	}
}

//nolint
func writeResult(conn *Conn, result *types.Result) error {
	if len(result.Fields) == 0 {
		return conn.writeOKPacket(&PacketOK{
			affectedRows: result.RowsAffected,
			lastInsertID: result.InsertID,
			statusFlags:  conn.StatusFlags,
			warnings:     0,
		})
	}
	if err := conn.writeFields(result); err != nil {
		return err
	}
	if err := conn.writeRows(result); err != nil {
		return err
	}
	return conn.writeEndResult(false, 0, 0, 0)
}

func RowString(row []types.Value) string {
	l := len(row)
	result := fmt.Sprintf("%v values:", l)
	for _, val := range row {
		result += fmt.Sprintf(" %v", val)
	}
	return result
}
