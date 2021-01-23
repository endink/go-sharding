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
	"github.com/pingcap/parser/opcode"
	driver "github.com/pingcap/tidb/types/parser_driver"
)

//将算数运算符转换为可以理解的 value 值( scalar 或者 range )
func (s *SqlExplain) explainValue(op opcode.Op, valueExpr *driver.ValueExpr) (interface{}, error) {
	value, e := getValueFromExpr(valueExpr)
	if e != nil {
		return nil, e
	}
	switch op {
	case opcode.EQ:
		return value, nil
	case opcode.GT:
		rng, err := core.NewRangeOpen(value, nil)
		if err != nil {
			return nil, err
		}
		return rng, nil
	case opcode.GE:
		rng, err := core.NewRangeClose(value, nil)
		if err != nil {
			return nil, err
		}
		return rng, nil
	case opcode.LT:
		rng, err := core.NewRangeOpen(nil, value)
		if err != nil {
			return nil, err
		}
		return rng, nil
	case opcode.LE:
		rng, err := core.NewRangeClose(nil, value)
		if err != nil {
			return nil, err
		}
		return rng, nil
	}
	return nil, fmt.Errorf("explain value fault, known opcode: %s", op.String())
}
