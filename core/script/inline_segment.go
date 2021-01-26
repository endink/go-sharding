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

type SegmentValidator func(string) error

type inlineSegmentGroup struct {
	segments []*inlineSegment
}

func (group *inlineSegmentGroup) clone() *inlineSegmentGroup {
	if group.segments == nil {
		return &inlineSegmentGroup{}
	}
	segs := make([]*inlineSegment, len(group.segments))
	for i, s := range group.segments {
		segs[i] = s
	}
	return &inlineSegmentGroup{
		segments: segs,
	}
}

type inlineSegment struct {
	rawScript string
	prefix    string
	script    CompiledScript
}

type splitContext struct {
	prefix    *strings.Builder
	rawScript *strings.Builder
	variables []*Variable
	segments  []*inlineSegment
}

func (seg *inlineSegment) clone() *inlineSegment {
	return &inlineSegment{
		rawScript: seg.rawScript,
		prefix:    seg.prefix,
		script:    seg.script.Clone(),
	}
}

func (seg *inlineSegment) isBlank() bool {
	return strings.TrimSpace(seg.prefix) == "" && strings.TrimSpace(seg.rawScript) == ""
}

func splitSegments(exp string, validator SegmentValidator, variables ...*Variable) ([]*inlineSegmentGroup, error) {
	isScript := false
	scriptStart := false
	expLen := len(exp)
	includeSplitter := false

	groups := make([]*inlineSegmentGroup, 0)

	syntaxError := func(message string, index int) error {
		var sb = core.NewStringBuilder()
		sb.WriteLine("inline expression syntax error")
		sb.WriteLine(message)
		sb.WriteLineF("expression: %s", exp)
		if index < 0 {
			sb.WriteLineF("char index: %d", index)
		}
		return errors.New(sb.String())
	}

	context := &splitContext{
		prefix:    &strings.Builder{},
		rawScript: &strings.Builder{},
		variables: variables,
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
				if err = context.flushSegment(validator); err != nil {
					return nil, syntaxError(err.Error(), i)
				}
			} else {
				prefix.WriteByte(char)
			}
		case ',':
			if !isScript {
				if g, err := context.flushGroup(validator); err != nil {
					return nil, syntaxError(err.Error(), i)
				} else {
					groups = append(groups, g)
				}
			} else {
				rawScript.WriteByte(char)
			}
		default:
			if isScript {
				rawScript.WriteByte(char)
			} else {
				prefix.WriteByte(char)
			}
		}

	}

	if isScript {
		return nil, syntaxError("symbol '}' used to end the script are missing", -1)
	}

	if g, err := context.flushGroup(validator); err != nil {
		return nil, syntaxError(err.Error(), expLen)
	} else {
		groups = append(groups, g)
	}
	return groups, nil
}

func (context *splitContext) flushGroup(validator SegmentValidator) (*inlineSegmentGroup, error) {
	if err := context.flushSegment(validator); err != nil {
		return nil, err
	} else {
		g := &inlineSegmentGroup{
			segments: context.segments,
		}
		context.segments = nil
		return g, nil
	}
}

func (context *splitContext) flushSegment(validator SegmentValidator) error {
	seg := &inlineSegment{
		prefix:    strings.TrimSpace(context.prefix.String()),
		rawScript: strings.TrimSpace(context.rawScript.String()),
	}
	if !seg.isBlank() {
		trim := strings.TrimSpace(seg.rawScript)
		if trim != "" {
			if s, err := ParseScript(trim, context.variables...); err != nil {
				return err
			} else {
				seg.script = s
			}
		}
		if validator != nil {
			if e := validator(seg.prefix); e != nil {
				return e
			}
		}
		context.segments = append(context.segments, seg)
	}

	context.prefix.Reset()
	context.rawScript.Reset()
	return nil
}
