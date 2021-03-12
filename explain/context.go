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
	"github.com/XiaoMi/Gaea/logging"
)

var Logger = logging.GetLogger("explain")

var _ Context = &context{}

type Context interface {
	TableLookup() TableLookup
	AggLookup() AggLookup
	GroupByLookup() FieldLookup
	OrderByLookup() FieldLookup
	FieldLookup() FieldLookup
	LimitLookup() LimitLookup
	ContainsFullShardColumn() bool //是否包含需要全分片查询的列
}

type context struct {
	tableLookup      TableLookup
	aggLookup        AggLookup
	groupByLookup    FieldLookup
	orderByLookup    FieldLookup
	fieldLookup      FieldLookup
	limitLookup      LimitLookup
	conditionColumns FieldLookup
	fullShardColumn  bool
}

func NewContext() *context {
	return &context{
		tableLookup:   newTableLookup(),
		aggLookup:     newAggLookup(),
		groupByLookup: newFieldLookup(),
		orderByLookup: newFieldLookup(),
		fieldLookup:   newFieldLookup(),
		limitLookup:   newLimitLookup(),
	}
}

func (c *context) ContainsFullShardColumn() bool {
	return c.fullShardColumn
}

func (c *context) LimitLookup() LimitLookup {
	return c.limitLookup
}

func (c *context) OrderByLookup() FieldLookup {
	return c.orderByLookup
}

func (c *context) GroupByLookup() FieldLookup {
	return c.groupByLookup
}

func (c *context) FieldLookup() FieldLookup {
	return c.fieldLookup
}

func (c *context) TableLookup() TableLookup {
	return c.tableLookup
}

func (c *context) AggLookup() AggLookup {
	return c.aggLookup
}
