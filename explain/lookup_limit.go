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
	"github.com/pingcap/parser/ast"
	driver "github.com/pingcap/tidb/types/parser_driver"
)

const noneCount = int64(-1)

type LimitLookup interface {
	Count() int64
	Offset() int64
	HasLimit() bool
	HasOffset() bool
	setLimit(limit *ast.Limit)
}

type limitLookup struct {
	count  int64
	offset int64
}

func (l *limitLookup) HasLimit() bool {
	return l.count >= 0
}

func (l *limitLookup) HasOffset() bool {
	return l.offset > 0
}

func (l *limitLookup) Count() int64 {
	return l.count
}

func (l *limitLookup) Offset() int64 {
	return l.offset
}

func (l *limitLookup) setLimit(limit *ast.Limit) {
	l.count = noneCount
	l.offset = noneCount

	if limit.Count != nil {
		l.count = limit.Count.(*driver.ValueExpr).GetInt64()
	}
	if limit.Offset != nil {
		l.offset = limit.Offset.(*driver.ValueExpr).GetInt64()
	}
}

func newLimitLookup() LimitLookup {
	return &limitLookup{
		count:  noneCount,
		offset: noneCount,
	}
}
