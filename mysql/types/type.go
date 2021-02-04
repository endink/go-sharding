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

import "fmt"

// MySqlTypedefines the various supported data types in bind vars
// and query results.
type MySqlType int32

const (
	// Null_type specifies a NULL type.
	Null MySqlType = 0
	// INT8 specifies a TINYINT type.
	// Properties: 1, IsNumber.
	Int8 MySqlType = 257
	// UINT8 specifies a TINYINT UNSIGNED type.
	// Properties: 2, IsNumber, IsUnsigned.
	Uint8 MySqlType = 770
	// INT16 specifies a SMALLINT type.
	// Properties: 3, IsNumber.
	Int16 MySqlType = 259
	// UINT16 specifies a SMALLINT UNSIGNED type.
	// Properties: 4, IsNumber, IsUnsigned.
	Uint16 MySqlType = 772
	// INT24 specifies a MEDIUMINT type.
	// Properties: 5, IsNumber.
	Int24 MySqlType = 261
	// UINT24 specifies a MEDIUMINT UNSIGNED type.
	// Properties: 6, IsNumber, IsUnsigned.
	Uint24 MySqlType = 774
	// INT32 specifies a INTEGER type.
	// Properties: 7, IsNumber.
	Int32 MySqlType = 263
	// UINT32 specifies a INTEGER UNSIGNED type.
	// Properties: 8, IsNumber, IsUnsigned.
	Uint32 MySqlType = 776
	// INT64 specifies a BIGINT type.
	// Properties: 9, IsNumber.
	Int64 MySqlType = 265
	// UINT64 specifies a BIGINT UNSIGNED type.
	// Properties: 10, IsNumber, IsUnsigned.
	Uint64 MySqlType = 778
	// FLOAT32 specifies a FLOAT type.
	// Properties: 11, IsFloat.
	Float32 MySqlType = 1035
	// FLOAT64 specifies a DOUBLE or REAL type.
	// Properties: 12, IsFloat.
	Float64 MySqlType = 1036
	// TIMESTAMP specifies a TIMESTAMP type.
	// Properties: 13, IsQuoted.
	Timestamp MySqlType = 2061
	// DATE specifies a DATE type.
	// Properties: 14, IsQuoted.
	Date MySqlType = 2062
	// TIME specifies a TIME type.
	// Properties: 15, IsQuoted.
	Time MySqlType = 2063
	// DATETIME specifies a DATETIME type.
	// Properties: 16, IsQuoted.
	Datetime MySqlType = 2064
	// YEAR specifies a YEAR type.
	// Properties: 17, IsNumber, IsUnsigned.
	Year MySqlType = 785
	// DECIMAL specifies a DECIMAL or NUMERIC type.
	// Properties: 18, None.
	Decimal MySqlType = 18
	// TEXT specifies a TEXT type.
	// Properties: 19, IsQuoted, IsText.
	Text MySqlType = 6163
	// BLOB specifies a BLOB type.
	// Properties: 20, IsQuoted, IsBinary.
	Blob MySqlType = 10260
	// VARCHAR specifies a VARCHAR type.
	// Properties: 21, IsQuoted, IsText.
	VarChar MySqlType = 6165
	// VARBINARY specifies a VARBINARY type.
	// Properties: 22, IsQuoted, IsBinary.
	VarBinary MySqlType = 10262
	// CHAR specifies a CHAR type.
	// Properties: 23, IsQuoted, IsText.
	Char MySqlType = 6167
	// BINARY specifies a BINARY type.
	// Properties: 24, IsQuoted, IsBinary.
	Binary MySqlType = 10264
	// BIT specifies a BIT type.
	// Properties: 25, IsQuoted.
	Bit MySqlType = 2073
	// ENUM specifies an ENUM type.
	// Properties: 26, IsQuoted.
	Enum MySqlType = 2074
	// SET specifies a SET type.
	// Properties: 27, IsQuoted.
	Set MySqlType = 2075
	// TUPLE specifies a tuple. This cannot
	// be returned in a QueryResult, but it can
	// be sent as a bind var.
	// Properties: 28, None.
	Tuple MySqlType = 28
	// GEOMETRY specifies a GEOMETRY type.
	// Properties: 29, IsQuoted.
	Geometry MySqlType = 2077
	// JSON specifies a JSON type.
	// Properties: 30, IsQuoted.
	Json MySqlType = 2078
	// EXPRESSION specifies a SQL expression.
	// This MySqlTypeis for internal use only.
	// Properties: 31, None.
	Expression MySqlType = 31
)

func (t MySqlType) String() string {
	if n, ok := TypeNames[t]; ok {
		return n
	}
	return fmt.Sprintf("KnowType(%d)", int32(t))
}

var TypeNames = map[MySqlType]string{
	0:     "Null_type",
	257:   "INT8",
	770:   "UINT8",
	259:   "INT16",
	772:   "UINT16",
	261:   "INT24",
	774:   "UINT24",
	263:   "INT32",
	776:   "UINT32",
	265:   "INT64",
	778:   "UINT64",
	1035:  "FLOAT32",
	1036:  "FLOAT64",
	2061:  "TIMESTAMP",
	2062:  "DATE",
	2063:  "TIME",
	2064:  "DATETIME",
	785:   "YEAR",
	18:    "DECIMAL",
	6163:  "TEXT",
	10260: "BLOB",
	6165:  "VARCHAR",
	10262: "VARBINARY",
	6167:  "CHAR",
	10264: "BINARY",
	2073:  "BIT",
	2074:  "ENUM",
	2075:  "SET",
	28:    "TUPLE",
	2077:  "GEOMETRY",
	2078:  "JSON",
	31:    "EXPRESSION",
}

var TypeValues = map[string]int32{
	"Null_type":  0,
	"INT8":       257,
	"UINT8":      770,
	"INT16":      259,
	"UINT16":     772,
	"INT24":      261,
	"UINT24":     774,
	"INT32":      263,
	"UINT32":     776,
	"INT64":      265,
	"UINT64":     778,
	"FLOAT32":    1035,
	"FLOAT64":    1036,
	"TIMESTAMP":  2061,
	"DATE":       2062,
	"TIME":       2063,
	"DATETIME":   2064,
	"YEAR":       785,
	"DECIMAL":    18,
	"TEXT":       6163,
	"BLOB":       10260,
	"VARCHAR":    6165,
	"VARBINARY":  10262,
	"CHAR":       6167,
	"BINARY":     10264,
	"BIT":        2073,
	"ENUM":       2074,
	"SET":        2075,
	"TUPLE":      28,
	"GEOMETRY":   2077,
	"JSON":       2078,
	"EXPRESSION": 31,
}

const (
	mysqlUnsigned = 32
	mysqlBinary   = 128
	mysqlEnum     = 256
	mysqlSet      = 2048
)

var typeToMySQL = map[MySqlType]struct {
	typ   int64
	flags int64
}{
	Int8:      {typ: 1},
	Uint8:     {typ: 1, flags: mysqlUnsigned},
	Int16:     {typ: 2},
	Uint16:    {typ: 2, flags: mysqlUnsigned},
	Int32:     {typ: 3},
	Uint32:    {typ: 3, flags: mysqlUnsigned},
	Float32:   {typ: 4},
	Float64:   {typ: 5},
	Null:      {typ: 6, flags: mysqlBinary},
	Timestamp: {typ: 7},
	Int64:     {typ: 8},
	Uint64:    {typ: 8, flags: mysqlUnsigned},
	Int24:     {typ: 9},
	Uint24:    {typ: 9, flags: mysqlUnsigned},
	Date:      {typ: 10, flags: mysqlBinary},
	Time:      {typ: 11, flags: mysqlBinary},
	Datetime:  {typ: 12, flags: mysqlBinary},
	Year:      {typ: 13, flags: mysqlUnsigned},
	Bit:       {typ: 16, flags: mysqlUnsigned},
	Json:      {typ: 245},
	Decimal:   {typ: 246},
	Text:      {typ: 252},
	Blob:      {typ: 252, flags: mysqlBinary},
	VarChar:   {typ: 253},
	VarBinary: {typ: 253, flags: mysqlBinary},
	Char:      {typ: 254},
	Binary:    {typ: 254, flags: mysqlBinary},
	Enum:      {typ: 254, flags: mysqlEnum},
	Set:       {typ: 254, flags: mysqlSet},
	Geometry:  {typ: 255},
}

// If you add to this map, make sure you add a test case
// in tabletserver/endtoend.
var mysqlToType = map[int64]MySqlType{
	0:   Decimal,
	1:   Int8,
	2:   Int16,
	3:   Int32,
	4:   Float32,
	5:   Float64,
	6:   Null,
	7:   Timestamp,
	8:   Int64,
	9:   Int24,
	10:  Date,
	11:  Time,
	12:  Datetime,
	13:  Year,
	15:  VarChar,
	16:  Bit,
	17:  Timestamp,
	18:  Datetime,
	19:  Time,
	245: Json,
	246: Decimal,
	247: Enum,
	248: Set,
	249: Text,
	250: Text,
	251: Text,
	252: Text,
	253: VarChar,
	254: Char,
	255: Geometry,
}

const (
	flagIsIntegral = int(Flag_ISINTEGRAL)
	flagIsUnsigned = int(Flag_ISUNSIGNED)
	flagIsFloat    = int(Flag_ISFLOAT)
	flagIsQuoted   = int(Flag_ISQUOTED)
	flagIsText     = int(Flag_ISTEXT)
	flagIsBinary   = int(Flag_ISBINARY)
)

// IsIntegral returns true if querypb.Type is an integral
// (signed/unsigned) that can be represented using
// up to 64 binary bits.
// If you have a Value object, use its member function.
func IsIntegral(t MySqlType) bool {
	return int(t)&flagIsIntegral == flagIsIntegral
}

// IsSigned returns true if querypb.Type is a signed integral.
// If you have a Value object, use its member function.
func IsSigned(t MySqlType) bool {
	return int(t)&(flagIsIntegral|flagIsUnsigned) == flagIsIntegral
}

// IsUnsigned returns true if querypb.Type is an unsigned integral.
// Caution: this is not the same as !IsSigned.
// If you have a Value object, use its member function.
func IsUnsigned(t MySqlType) bool {
	return int(t)&(flagIsIntegral|flagIsUnsigned) == flagIsIntegral|flagIsUnsigned
}

// IsFloat returns true is querypb.Type is a floating point.
// If you have a Value object, use its member function.
func IsFloat(t MySqlType) bool {
	return int(t)&flagIsFloat == flagIsFloat
}

// IsQuoted returns true if querypb.Type is a quoted text or binary.
// If you have a Value object, use its member function.
func IsQuoted(t MySqlType) bool {
	return (int(t)&flagIsQuoted == flagIsQuoted) && t != Bit
}

// IsText returns true if querypb.Type is a text.
// If you have a Value object, use its member function.
func IsText(t MySqlType) bool {
	return int(t)&flagIsText == flagIsText
}

// IsBinary returns true if querypb.Type is a binary.
// If you have a Value object, use its member function.
func IsBinary(t MySqlType) bool {
	return int(t)&flagIsBinary == flagIsBinary
}

// IsNumber returns true if the type is any type of number.
func IsNumber(t MySqlType) bool {
	return IsIntegral(t) || IsFloat(t) || t == Decimal
}

// MySQLToType computes the vitess type from mysql type and flags.
func MySQLToType(mysqlType, flags int64) (typ MySqlType, err error) {
	result, ok := mysqlToType[mysqlType]
	if !ok {
		return 0, fmt.Errorf("unsupported type: %d", mysqlType)
	}
	return modifyType(result, flags), nil
}

// TypeToMySQL returns the equivalent mysql type and flag for a vitess type.
func TypeToMySQL(typ MySqlType) (mysqlType, flags int64) {
	val := typeToMySQL[typ]
	return val.typ, val.flags
}

// modifyType modifies the vitess type based on the
// mysql flag. The function checks specific flags based
// on the type. This allows us to ignore stray flags
// that MySQL occasionally sets.
func modifyType(typ MySqlType, flags int64) MySqlType {
	switch typ {
	case Int8:
		if flags&mysqlUnsigned != 0 {
			return Uint8
		}
	case Int16:
		if flags&mysqlUnsigned != 0 {
			return Uint16
		}
	case Int32:
		if flags&mysqlUnsigned != 0 {
			return Uint32
		}
	case Int64:
		if flags&mysqlUnsigned != 0 {
			return Uint64
		}
	case Int24:
		if flags&mysqlUnsigned != 0 {
			return Uint24
		}
	case Text:
		if flags&mysqlBinary != 0 {
			return Blob
		}
	case VarChar:
		if flags&mysqlBinary != 0 {
			return VarBinary
		}
	case Char:
		if flags&mysqlBinary != 0 {
			return Binary
		}
		if flags&mysqlEnum != 0 {
			return Enum
		}
		if flags&mysqlSet != 0 {
			return Set
		}
	case Year:
		if flags&mysqlBinary != 0 {
			return VarBinary
		}
	}
	return typ
}
