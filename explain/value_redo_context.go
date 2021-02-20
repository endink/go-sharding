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

package explain

import (
	"github.com/XiaoMi/Gaea/core"
	"github.com/emirpasic/gods/stacks/arraystack"
)

type valueRedoContext struct {
	logicStack *arraystack.Stack
	valueStack *arraystack.Stack
}

func newValueRedoContext() *valueRedoContext {
	ctx := &valueRedoContext{
		logicStack: arraystack.New(),
		valueStack: arraystack.New(),
	}
	ctx.beginValueGroup()
	return ctx
}

func (vrc *valueRedoContext) currentValueScope() *valueScope {
	current, _ := vrc.valueStack.Peek()
	return current.(*valueScope)
}

func (vrc *valueRedoContext) pushLogic(logic core.BinaryLogic) {
	vrc.logicStack.Push(logic)
}

func (vrc *valueRedoContext) popLogic() {
	vrc.logicStack.Pop()
}

func (vrc *valueRedoContext) currentLogic() core.BinaryLogic {
	v, ok := vrc.logicStack.Peek()
	if !ok {
		return core.LogicAnd
	}
	return v.(core.BinaryLogic)
}

func (vrc *valueRedoContext) beginValueGroup() {
	ns := newValueScope(vrc.currentLogic())
	vrc.valueStack.Push(ns)
}

func (vrc *valueRedoContext) endValueGroup() error {
	if vrc.valueStack.Size() >= 2 {
		ns := vrc.currentValueScope()
		vrc.valueStack.Pop()
		pre := vrc.currentValueScope()
		for table, builder := range ns.builders {
			if err := pre.table(table).Merge(builder, ns.logic); err != nil {
				return err
			}
		}
	}
	return nil
}

func (vrc *valueRedoContext) pushOrValueGroupWithLogic(table string, column string, logic core.BinaryLogic, values ...interface{}) error {
	vrc.beginValueGroup()
	for _, v := range values {
		if err := vrc.pushValueWitLogic(table, column, v, logic); err != nil {
			return err
		}
	}
	return vrc.endValueGroup()
}

func (vrc *valueRedoContext) pushValueWitLogic(table string, column string, value interface{}, logic core.BinaryLogic) error {
	var err error
	if rg, ok := value.(core.Range); ok {
		err = vrc.pushRange(table, column, rg, logic)
	} else {
		vrc.pushScalar(table, column, value, logic)
	}
	return err
}

func (vrc *valueRedoContext) PushValue(table string, column string, value interface{}) error {
	return vrc.pushValueWitLogic(table, column, value, vrc.currentLogic())
}

func (vrc *valueRedoContext) pushScalar(table string, column string, value interface{}, logic core.BinaryLogic) {
	scope := vrc.currentValueScope()
	b := scope.table(table)
	switch logic {
	case core.LogicOr:
		b.OrValue(column, value)
	case core.LogicAnd:
		b.AndValue(column, value)
	}
}

func (vrc *valueRedoContext) pushRange(table string, column string, value core.Range, logic core.BinaryLogic) error {
	scope := vrc.currentValueScope()
	b := scope.table(table)
	switch logic {
	case core.LogicOr:
		return b.OrRange(column, value)
	case core.LogicAnd:
		return b.AndRange(column, value)
	}
	return nil
}
