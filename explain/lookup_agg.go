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
	"github.com/XiaoMi/Gaea/core"
	"github.com/pingcap/parser/ast"
	"strings"
)

type AggLookup interface {
	visit(index int, field *ast.SelectField) error
	Fields() []*AggField
}

type aggLookup struct {
	fields []*AggField
}

func newAggLookup() *aggLookup {
	return &aggLookup{}
}

func (a *aggLookup) Fields() []*AggField {
	return a.fields
}

func (a *aggLookup) visit(index int, field *ast.SelectField) error {
	switch agg := field.Expr.(type) {
	case *ast.AggregateFuncExpr:
		aggName := strings.ToLower(agg.F)
		if ty, ok := core.ParseAggType(aggName); ok {
			return fmt.Errorf("aggregate function type is not support: %s", agg.F)
		} else {
			aggField := &AggField{
				Index:   index,
				AggType: ty,
			}
			a.fields = append(a.fields, aggField)
		}
	}
	return nil
}
