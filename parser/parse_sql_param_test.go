/*
 *
 *  * Copyright 2021. Go-Sharding Author All Rights Reserved.
 *  *
 *  *  Licensed under the Apache License, Version 2.0 (the "License");
 *  *  you may not use this file except in compliance with the License.
 *  *  You may obtain a copy of the License at
 *  *
 *  *      http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  *  Unless required by applicable law or agreed to in writing, software
 *  *  distributed under the License is distributed on an "AS IS" BASIS,
 *  *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  *  See the License for the specific language governing permissions and
 *  *  limitations under the License.
 *  *
 *  *  File author: Anders Xiao
 *
 */

package parser

import (
	"github.com/XiaoMi/Gaea/testkit"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestParseStmtParam(t *testing.T) {
	sql := "select a, b, c from d where id = ? and name = ? and v in (?,?,?,?)"

	stmt := testkit.ParseSelect(sql, t)
	count, err := parseNodeParam(stmt)
	assert.Nil(t, err)
	assert.Equal(t, 6, count)
}
