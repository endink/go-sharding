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
	"errors"
	"github.com/XiaoMi/Gaea/core"
	"strings"
)

type inlineSegment struct {
	rawScript string
	prefix    string
	script    CompiledScript
}

type splitContext struct {
	prefix    *strings.Builder
	rawScript *strings.Builder
	variables map[string]interface{}
	segments  []*inlineSegment
}

func (seg inlineSegment) isBlank() bool {
	return strings.TrimSpace(seg.prefix) == "" && strings.TrimSpace(seg.rawScript) == ""
}

func splitInlineExpression(exp string) ([]*inlineSegment, error) {
	isScript := false
	scriptStart := false
	expLen := len(exp)
	includeSplitter := false

	syntaxError := func(message string, index int) error {
		var sb = core.NewStringBuilder()
		sb.WriteLine("inline expression syntax error")
		sb.WriteLine(message)
		sb.WriteLineF("expression: %s", exp)
		sb.WriteLineF("char index: %d", index)
		return errors.New(sb.String())
	}

	context := &splitContext{
		prefix:    &strings.Builder{},
		rawScript: &strings.Builder{},
		variables: make(map[string]interface{}),
	}

	prefix := context.prefix
	rawScript := context.rawScript
	var err error

	for i, c := range exp {
		char := byte(c)
		switch char {
		case '$':
			if !isScript {
				if i < (expLen-1) && '{' == exp[i+1] {
					isScript = true
					scriptStart = true
				} else {
					return nil, syntaxError("'{' symbol is missing after the symbol '$'", i)
				}
			} else {
				return nil, syntaxError("should not appear symbol '$'", i)
			}
		case '{':
			if isScript {
				if scriptStart {
					scriptStart = false
				} else {
					rawScript.WriteByte(char)
				}
			} else {
				prefix.WriteByte(char)
			}
		case '.':
			if i == 0 || i == (expLen-1) {
				return nil, syntaxError("should not appear symbol '.' at beginning and end of the inline expression", i)
			}
			if isScript {
				rawScript.WriteByte(char)
			} else {
				if includeSplitter {
					return nil, syntaxError("should not appear symbol '.'", i)
				} else {
					includeSplitter = true
				}
				prefix.WriteByte(char)
			}
		case '}':
			if isScript {
				isScript = false
				if err = flushSegment(context); err != nil {
					return nil, syntaxError(err.Error(), i)
				}
			} else {
				return nil, syntaxError("should not appear symbol '}'", i)
			}
		default:
			if isScript {
				rawScript.WriteByte(char)
			} else {
				prefix.WriteByte(char)
			}
		}

	}

	if err = flushSegment(context); err != nil {
		return nil, syntaxError(err.Error(), expLen)
	}
	return context.segments, nil
}

func flushSegment(context *splitContext) error {
	seg := &inlineSegment{
		prefix:    context.prefix.String(),
		rawScript: context.rawScript.String(),
	}
	if !seg.isBlank() {
		trim := strings.TrimSpace(seg.rawScript)
		if trim != "" {
			if s, err := ParseScriptVar(trim, context.variables); err != nil {
				return err
			} else {
				seg.script = s
			}
		}
		context.segments = append(context.segments, seg)
	}

	context.prefix.Reset()
	context.rawScript.Reset()
	return nil
}
