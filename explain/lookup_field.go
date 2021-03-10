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

package explain

import (
	"fmt"
	"github.com/pingcap/errors"
	"github.com/pingcap/parser/ast"
)

type FieldLookup interface {
	addField(index int, field *ast.SelectField, isAttached bool) error
	addFieldWitName(index int, name string, isAttached bool) error

	Fields() []*FieldIndex
	FindByName(fieldName string) int
}

type fieldLookup struct {
	fields     []*FieldIndex
	fieldNames map[string]uint8
}

func (a *fieldLookup) FindByName(fieldName string) int {
	i, ok := a.fieldNames[fieldName]
	if ok {
		return int(i)
	} else {
		return -1
	}
}

func newFieldLookup() *fieldLookup {
	return &fieldLookup{
		fieldNames: make(map[string]uint8),
	}
}

func (a *fieldLookup) Fields() []*FieldIndex {
	return a.fields
}

func (a *fieldLookup) addFieldWitName(index int, name string, isAttached bool) error {
	if name != "" {
		if index > 255 || index < 0 {
			return errors.New("field index out of range, at most 256 fields are allowed")
		}

		f := &FieldIndex{
			index,
			isAttached,
		}
		a.fieldNames[name] = uint8(index)
		a.fields = append(a.fields, f)
	}
	return nil
}

func (a *fieldLookup) addField(index int, field *ast.SelectField, isAttached bool) error {
	var name string
	if field.AsName.L != "" {
		name = field.AsName.L
	}
	if name == "" {
		switch expr := field.Expr.(type) {
		case *ast.ColumnNameExpr:
			name = expr.Name.Name.L
		case *ast.AggregateFuncExpr:
			name = fmt.Sprintf("F:%s", expr.F)
		}
	}

	return a.addFieldWitName(index, name, isAttached)
}
