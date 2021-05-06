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

package parser

import (
	"github.com/endink/go-sharding/mysql/types"
	"strings"
)

// This file contains types that are 'Encodable'.

// Encodable defines the interface for types that can
// be custom-encoded into SQL.
type Encodable interface {
	EncodeSQL(buf *strings.Builder) error
}

// InsertValues is a custom SQL encoder for the values of
// an insert statement.
type InsertValues [][]types.Value

// EncodeSQL performs the SQL encoding for InsertValues.
func (iv InsertValues) EncodeSQL(buf *strings.Builder) error {
	for i, rows := range iv {
		if i != 0 {
			buf.WriteString(", ")
		}
		buf.WriteByte('(')
		for j, bv := range rows {
			if j != 0 {
				buf.WriteString(", ")
			}
			if err := bv.EncodeSQL(buf); err != nil {
				return err
			}
		}
		buf.WriteByte(')')
	}
	return nil
}

// TupleEqualityList is for generating equality constraints
// for tables that have composite primary keys.
type TupleEqualityList struct {
	Columns []string
	Rows    [][]types.Value
}

// EncodeSQL generates the where clause constraints for the tuple
// equality.
func (tpl *TupleEqualityList) EncodeSQL(buf *strings.Builder) error {
	if len(tpl.Columns) == 1 {
		return tpl.encodeAsIn(buf)
	}
	return tpl.encodeAsEquality(buf)
}

func (tpl *TupleEqualityList) encodeAsIn(buf *strings.Builder) error {
	if err := Append(buf, tpl.Columns[0]); err != nil {
		return err
	}
	buf.WriteString(" in (")
	for i, r := range tpl.Rows {
		if i != 0 {
			buf.WriteString(", ")
		}
		if err := r[0].EncodeSQL(buf); err != nil {
			return err
		}
	}
	return buf.WriteByte(')')
}

func (tpl *TupleEqualityList) encodeAsEquality(buf *strings.Builder) error {
	for i, r := range tpl.Rows {
		if i != 0 {
			buf.WriteString(" or ")
		}
		buf.WriteString("(")
		for j, c := range tpl.Columns {
			if j != 0 {
				buf.WriteString(" and ")
			}
			if err := Append(buf, c); err != nil {
				return err
			}
			buf.WriteString(" = ")
			if err := r[j].EncodeSQL(buf); err != nil {
				return err
			}
		}
		buf.WriteByte(')')
	}
	return nil
}

// Append appends the SQLNode to the buffer.
func Append(buf *strings.Builder, column string) error {
	_, err := buf.WriteString(column)
	return err
}
