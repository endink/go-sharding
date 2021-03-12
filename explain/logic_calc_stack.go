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
	"github.com/XiaoMi/Gaea/core"
	"github.com/emirpasic/gods/stacks/arraystack"
)

type brackets int

const (
	bracketsStart brackets = iota
	bracketsEnd
)

//logic stack, priority support
type LogicPriorityStack struct {
	valueStack *arraystack.Stack
	logicStack *arraystack.Stack
	processor  func(logic core.BinaryLogic, value interface{})
}

func newLogicPriorityStack(processor func(logic core.BinaryLogic, value interface{})) *LogicPriorityStack {
	return &LogicPriorityStack{
		valueStack: arraystack.New(),
		logicStack: arraystack.New(),
		processor:  processor,
	}
}

func (ls *LogicPriorityStack) PushValue(v interface{}) {
	if ls.current().logicStack.Size() > 0 {
		l, ok := ls.current().logicStack.Peek()
		if ok {
			logic := l.(core.BinaryLogic)
			switch logic {
			case core.LogicAnd:
				if value, hasValue := ls.valueStack.Pop(); hasValue {
					ls.current().processor(core.LogicAnd, value)
					ls.current().processor(core.LogicAnd, v)
				}
			case core.LogicOr:
				ls.current().valueStack.Push(v)
			}
		}
	}
}

func (ls *LogicPriorityStack) current() *LogicPriorityStack {
	if v, ok := ls.valueStack.Peek(); ok {
		if lgStack, isLgStack := v.(*LogicPriorityStack); isLgStack {
			return lgStack
		}
	}
	return ls
}

func (ls *LogicPriorityStack) PushLogic(logic core.BinaryLogic) {
	ls.logicStack.Push(logic)
}

func (ls *LogicPriorityStack) PushBracketsStart() {
	ls.current().valueStack.Push(newLogicPriorityStack(ls.processor))
}

func (ls *LogicPriorityStack) PushBracketsEnd() {
	if v, ok := ls.valueStack.Peek(); ok {
		if lgStack, isLgStack := v.(*LogicPriorityStack); isLgStack {
			lgStack.Calc()
		}
	}
}

func (ls *LogicPriorityStack) Calc() {
	for !ls.valueStack.Empty() {
		v, _ := ls.valueStack.Pop()
		ls.processor(core.LogicOr, v)
	}
}
