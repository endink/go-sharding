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

package script

import (
	"fmt"
	"github.com/d5/tengo/v2"
)

type Compiler interface {
	AddVariable(name string, value interface{}) error
	Compile() (CompiledScript, error)
}

type scriptParser struct {
	script *tengo.Script
}

func (s *scriptParser) Compile() (CompiledScript, error) {
	c, err := s.script.Compile()
	if err != nil {
		return nil, err
	}
	return &tengoScript{
		compiled: c,
	}, nil
}

func (s *scriptParser) AddVariable(name string, value interface{}) error {
	if tv, err := tengo.FromInterface(value); err != nil {
		return fmt.Errorf("bad format value for script, variable name: %s, %s", name, err)
	} else {
		if err = s.script.Add(name, tv); err != nil {
			return fmt.Errorf("add variable '%s' to compile fault, %s", name, err)
		}
	}
	return nil
}

func NewScriptParser(script string) (Compiler, error) {
	content := fmt.Sprintf("_r:=%s", script)
	bytes := []byte(content)
	s := tengo.NewScript(bytes)
	if err := s.Add("range", RangeFunction); err != nil {
		return nil, err
	} else {
		return &scriptParser{
			script: tengo.NewScript(bytes),
		}, nil
	}
}
