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

type MySqlFlag int32

const (
	MySqlFlag_EMPTY                 MySqlFlag = 0
	MySqlFlag_NOT_NULL_FLAG         MySqlFlag = 1
	MySqlFlag_PRI_KEY_FLAG          MySqlFlag = 2
	MySqlFlag_UNIQUE_KEY_FLAG       MySqlFlag = 4
	MySqlFlag_MULTIPLE_KEY_FLAG     MySqlFlag = 8
	MySqlFlag_BLOB_FLAG             MySqlFlag = 16
	MySqlFlag_UNSIGNED_FLAG         MySqlFlag = 32
	MySqlFlag_ZEROFILL_FLAG         MySqlFlag = 64
	MySqlFlag_BINARY_FLAG           MySqlFlag = 128
	MySqlFlag_ENUM_FLAG             MySqlFlag = 256
	MySqlFlag_AUTO_INCREMENT_FLAG   MySqlFlag = 512
	MySqlFlag_TIMESTAMP_FLAG        MySqlFlag = 1024
	MySqlFlag_SET_FLAG              MySqlFlag = 2048
	MySqlFlag_NO_DEFAULT_VALUE_FLAG MySqlFlag = 4096
	MySqlFlag_ON_UPDATE_NOW_FLAG    MySqlFlag = 8192
	MySqlFlag_NUM_FLAG              MySqlFlag = 32768
	MySqlFlag_PART_KEY_FLAG         MySqlFlag = 16384
	MySqlFlag_GROUP_FLAG            MySqlFlag = 32768
	MySqlFlag_UNIQUE_FLAG           MySqlFlag = 65536
	MySqlFlag_BINCMP_FLAG           MySqlFlag = 131072
)

var MySqlFlagValueToName = map[MySqlFlag]string{
	0:     "EMPTY",
	1:     "NOT_NULL_FLAG",
	2:     "PRI_KEY_FLAG",
	4:     "UNIQUE_KEY_FLAG",
	8:     "MULTIPLE_KEY_FLAG",
	16:    "BLOB_FLAG",
	32:    "UNSIGNED_FLAG",
	64:    "ZEROFILL_FLAG",
	128:   "BINARY_FLAG",
	256:   "ENUM_FLAG",
	512:   "AUTO_INCREMENT_FLAG",
	1024:  "TIMESTAMP_FLAG",
	2048:  "SET_FLAG",
	4096:  "NO_DEFAULT_VALUE_FLAG",
	8192:  "ON_UPDATE_NOW_FLAG",
	32768: "NUM_FLAG",
	16384: "PART_KEY_FLAG",
	// Duplicate value: 32768: "GROUP_FLAG",
	65536:  "UNIQUE_FLAG",
	131072: "BINCMP_FLAG",
}

var MySqlFlagNameToValue = map[string]int32{
	"EMPTY":                 0,
	"NOT_NULL_FLAG":         1,
	"PRI_KEY_FLAG":          2,
	"UNIQUE_KEY_FLAG":       4,
	"MULTIPLE_KEY_FLAG":     8,
	"BLOB_FLAG":             16,
	"UNSIGNED_FLAG":         32,
	"ZEROFILL_FLAG":         64,
	"BINARY_FLAG":           128,
	"ENUM_FLAG":             256,
	"AUTO_INCREMENT_FLAG":   512,
	"TIMESTAMP_FLAG":        1024,
	"SET_FLAG":              2048,
	"NO_DEFAULT_VALUE_FLAG": 4096,
	"ON_UPDATE_NOW_FLAG":    8192,
	"NUM_FLAG":              32768,
	"PART_KEY_FLAG":         16384,
	"GROUP_FLAG":            32768,
	"UNIQUE_FLAG":           65536,
	"BINCMP_FLAG":           131072,
}

func (x MySqlFlag) String() string {
	if n, ok := MySqlFlagValueToName[x]; ok {
		return n
	}
	return ""
}

// Flag allows us to qualify types by their common properties.
type Flag int32

const (
	Flag_NONE       Flag = 0
	Flag_ISINTEGRAL Flag = 256
	Flag_ISUNSIGNED Flag = 512
	Flag_ISFLOAT    Flag = 1024
	Flag_ISQUOTED   Flag = 2048
	Flag_ISTEXT     Flag = 4096
	Flag_ISBINARY   Flag = 8192
)

var FlagValueToName = map[Flag]string{
	0:    "NONE",
	256:  "ISINTEGRAL",
	512:  "ISUNSIGNED",
	1024: "ISFLOAT",
	2048: "ISQUOTED",
	4096: "ISTEXT",
	8192: "ISBINARY",
}

var FlagNameToValue = map[string]Flag{
	"NONE":       0,
	"ISINTEGRAL": 256,
	"ISUNSIGNED": 512,
	"ISFLOAT":    1024,
	"ISQUOTED":   2048,
	"ISTEXT":     4096,
	"ISBINARY":   8192,
}

func (x Flag) String() string {
	if n, ok := FlagValueToName[x]; ok {
		return n
	}
	return ""
}
