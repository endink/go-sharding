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

package strategy

import (
	"errors"
	"fmt"
	"github.com/endink/go-sharding/core"
	"github.com/endink/go-sharding/core/script"
	"strings"
)

const (
	ShardingColumnPropertyName = "sharding-columns"
	ExpressionPropertyName     = "Expression"
)

type InlineBuilder struct {
	ShardingColumns string `yaml:"sharding-columns"`
	Expression      string `yaml:"expression"`
}

func (i *InlineBuilder) Build() (*Inline, error) {
	columns, err := i.loadShardingColumns()
	if err != nil {
		return nil, err
	}

	expr, err := i.loadExpression(columns)
	if err != nil {
		return nil, err
	}
	return &Inline{
		Columns:    columns,
		Expression: expr,
	}, nil
}

func (i *InlineBuilder) loadShardingColumns() ([]string, error) {
	if i.ShardingColumns == "" {
		return nil, fmt.Errorf("configuration property '%s' missed for inline strategy", ShardingColumnPropertyName)
	}

	columns, err := i.parseColumnsExpression(i.ShardingColumns)
	if err != nil {
		return nil, err
	}

	if len(columns) == 0 {
		return nil, fmt.Errorf("invalid configuration property '%s' for inline strategy, columns can not be parsed", ShardingColumnPropertyName)
	}
	return columns, nil
}

func (i *InlineBuilder) loadExpression(shardingColumns []string) (script.InlineExpression, error) {
	if i.Expression == "" {
		return nil, fmt.Errorf("configuration property '%s' missed for inline strategy", ExpressionPropertyName)
	}

	vars := make([]*script.Variable, len(shardingColumns))
	for i, s := range shardingColumns {
		vars[i] = &script.Variable{
			Name:  s,
			Value: nil,
		}
	}

	expr, err := script.NewInlineExpression(i.Expression, vars...)
	if err != nil {
		mainInfo := fmt.Sprintf("invalid configuration property '%s' for inline strategy", ExpressionPropertyName)
		return nil, errors.New(fmt.Sprint(mainInfo, core.LineSeparator, err))
	}

	return expr, nil
}

func (i *InlineBuilder) parseColumnsExpression(columnsExpr string) ([]string, error) {
	columns := strings.Split(columnsExpr, ",")
	columns = core.DistinctSliceAndTrim(columns)
	if len(columns) == 0 {
		return nil, fmt.Errorf("invalid configuration property '%s' for inline strategy, have no columns can be parsed", ShardingColumnPropertyName)
	}

	for _, col := range columns {
		if err := core.ValidateIdentifier(col); err != nil {
			return nil, fmt.Errorf("invalid configuration property '%s' for inline strategy, invalid column name '%s'", ShardingColumnPropertyName, col)
		}
	}
	return columns, nil
}
