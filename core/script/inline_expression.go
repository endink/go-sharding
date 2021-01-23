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
)

var _ InlineExpression = &inlineExpr{}

type InlineExpression interface {
	Flat(variables ...*Variable) ([]string, error)
	FlatScalar(variables ...*Variable) (string, error)
	Clone() InlineExpression
	RawExpresion() string
}

type inlineExpr struct {
	expression string
	segments   []*inlineSegmentGroup
	varsNames  []string
}

func (i *inlineExpr) Clone() InlineExpression {
	var groups []*inlineSegmentGroup
	if len(i.segments) > 0 {
		groups = make([]*inlineSegmentGroup, len(i.segments))
		for idx, segment := range i.segments {
			groups[idx] = segment
		}
	}
	return &inlineExpr{
		expression: i.expression,
		segments:   groups,
		varsNames:  i.varsNames,
	}
}

func (i *inlineExpr) RawExpresion() string {
	return i.expression
}

func (i *inlineExpr) FlatScalar(variables ...*Variable) (string, error) {
	if list, err := i.Flat(variables...); err != nil {
		return "", err
	} else {
		for _, s := range list {
			return s, nil
		}
	}
	return "", nil
}

func (i *inlineExpr) Flat(variables ...*Variable) ([]string, error) {
	set := make(map[string]struct{})
	var list []string

	for _, g := range i.segments {
		var current []string
		for _, s := range g.segments {
			if s.script != nil {
				for _, va := range variables {
					if err := s.script.SetVar(va.Name, va.Value); err != nil {
						return nil, i.wrapExecuteError(err, variables...)
					}
				}
				if l, err := s.script.ExecuteList(); err != nil {
					return nil, i.wrapExecuteError(err, variables...)
				} else {
					segStrings := flatFill(s.prefix, l)
					current = outJoin(current, segStrings)
				}
			} else {
				if s.prefix != "" {
					current = append(current, s.prefix)
				}
			}

		}
		for _, c := range current {
			if _, ok := set[c]; !ok {
				set[c] = struct{}{}
				list = append(list, c)
			}

		}
	}
	if list == nil {
		list = make([]string, 0)
	}
	return list, nil
}

func varsArray(vars []*Variable) []interface{} {
	r := make([]interface{}, len(vars))
	for i, variable := range vars {
		r[i] = variable
	}
	return r
}

func (i *inlineExpr) wrapExecuteError(e error, vars ...*Variable) error {
	sb := core.NewStringBuilder()
	sb.WriteLine("inline sharding fault.")
	sb.WriteLine("Script: ", i.expression)
	sb.Write("Variables: ")
	if len(vars) > 0 {
		sb.WriteJoin(", ", varsArray(vars)...)
	} else {
		sb.Write("<none>")
	}
	sb.WriteLine()
	sb.WriteLine("Error:")
	sb.Write(e.Error())

	return errors.New(sb.String())
}

func NewInlineExpression(expression string, variables ...*Variable) (InlineExpression, error) {
	var names []string
	if len(variables) > 0 {
		names = make([]string, len(variables))
		for i, variable := range variables {
			names[i] = variable.Name
		}
	}

	expr := &inlineExpr{expression: expression, varsNames: names}

	if segments, err := splitSegments(expression, variables...); err != nil {
		return nil, err
	} else {
		expr.segments = segments
	}
	return expr, nil
}
