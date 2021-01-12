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

package script

import (
	"github.com/XiaoMi/Gaea/core"
	"github.com/stretchr/testify/assert"
	"strconv"
	"strings"
	"testing"
)

func TestRange1Function(t *testing.T) {
	script := "range(1,10)"
	s := runTestScript(script, t)

	assert.Equal(t, 10, len(s), "result for script fault: %s%s", core.LineSeparator, strings.Join(s, ", "))
}

func TestRange2Function(t *testing.T) {
	script := "range(5,10)"
	s := runTestScript(script, t)

	assert.Equal(t, 6, len(s), "result for script fault: %s%s", core.LineSeparator, strings.Join(s, ", "))
}

func TestArray(t *testing.T) {
	script := "[2,3,5,7]"
	s := runTestScript(script, t)

	assert.Equal(t, 4, len(s), "result for script fault: %s%s", core.LineSeparator, strings.Join(s, ", "))
}

func TestVar(t *testing.T) {
	script := "a+b"
	vars := map[string]interface{}{
		"a": 3,
		"b": 4,
	}
	s := runTestScriptVar(script, vars, t)

	assert.Equal(t, 1, len(s), "result for script fault: %s%s", core.LineSeparator, strings.Join(s, ", "))

	v, _ := strconv.Atoi(s[0])
	assert.Equal(t, 7, v)
}

func runTestScript(script string, t *testing.T) []string {
	return runTestScriptVar(script, nil, t)
}

func runTestScriptVar(script string, vars map[string]interface{}, t *testing.T) []string {
	s := compileTestScriptVar(script, vars, t)
	r, err := s.Run()
	assert.Nil(t, err, "run script fault:", script)
	return r
}

func compileTestScript(script string, t *testing.T) CompiledScript {
	return compileTestScriptVar(script, nil, t)
}

func compileTestScriptVar(script string, vars map[string]interface{}, t *testing.T) CompiledScript {
	c, err := NewScriptParser(script)
	assert.Nil(t, err, "fault to compile: %s", script)

	if vars != nil {
		for name, value := range vars {
			err = c.Var(name, value)
			assert.Nil(t, err, "add var to script fault: %s", script)
		}
	}

	s, err := c.Compile()
	assert.Nil(t, err, "compile script fault:", script)
	return s
}
