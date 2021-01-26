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
	"github.com/XiaoMi/Gaea/core"
	"github.com/XiaoMi/Gaea/core/script"
)

var _ core.ShardingStrategy = &Inline{}

type Inline struct {
	Columns    []string
	Expression script.InlineExpression
}

func (i *Inline) GetShardingColumns() []string {
	return i.Columns
}

func (i *Inline) IsScalarValueSupported() bool {
	return true
}

func (i *Inline) IsRangeValueSupported() bool {
	return false
}

func (i *Inline) Shard(sources []string, values *core.ShardingValues) ([]string, error) {
	if len(i.Columns) == 1 {
		column := i.Columns[0]
		if columnValues, ok := values.ScalarValues[column]; ok {
			return i.shardSingleColumn(column, columnValues)
		} else {
			return sources, nil
		}
	}

	return i.shardComplex(sources, values)
}

//单一列值时简单处理
func (i *Inline) shardSingleColumn(column string, columnValues []interface{}) ([]string, error) {
	v := &script.Variable{
		Name: column,
	}
	tables := make([]string, 0, len(columnValues))
	for _, value := range columnValues {
		v.Value = value
		if table, e := i.Expression.FlatScalar(v); e != nil {
			return nil, e
		} else {
			tables = append(tables, table)
		}
	}
	return core.DistinctSliceAndTrim(tables), nil
}

//多个列值的情况执行复杂计算
func (i *Inline) shardComplex(sources []string, values *core.ShardingValues) ([]string, error) {
	//二位数组，取所有列的数，第一维度为列，第二维度为值
	columnsValues := make([][]interface{}, 0, len(i.Columns))

	valueCount := len(i.Columns)

	for _, column := range i.Columns {
		v, _ := values.ScalarValues[column]
		if len(v) == 0 {
			//定义的列其中一个未找到值返回全部分片
			return sources, nil
		}
		columnsValues = append(columnsValues, v)
		valueCount = valueCount * len(v)
	}

	result := make([]string, 0, valueCount)
	vars := make([]*script.Variable, len(i.Columns))

	//得到列值的笛卡尔积
	p := core.Permute(columnsValues)

	for _, varValues := range p {
		for idx := 0; idx < len(i.Columns); idx++ {
			var v = vars[idx]
			if v == nil { //不必每次都分配内存
				v = &script.Variable{Name: i.Columns[idx]}
				vars[idx] = v
			}
			cv := varValues[idx]
			vars[idx].Value = cv
		}
		table, e := i.Expression.FlatScalar(vars...)
		if e != nil {
			return nil, e
		}
		result = append(result, table)
	}

	return core.DistinctSliceAndTrim(result), nil
}
