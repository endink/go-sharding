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

type InlineExpression interface {
	Flat(variables ...*Variable) ([]string, error)
	FlatScalar(variables ...*Variable) (string, error)
}

type inlineExpr struct {
	expression string
	segments   []*inlineSegmentGroup
	vars       []*Variable
}

func (i *inlineExpr) FlatScalar(variables ...*Variable) (string, error) {
	if list, err := i.Flat(variables...); err != nil {
		return "", nil
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
						return nil, err
					}
				}
				if list, err := s.script.ExecuteList(); err != nil {
					return nil, err
				} else {
					segStrings := flatFill(s.prefix, list)
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

func NewInlineExpression(expression string, variables ...*Variable) (InlineExpression, error) {
	expr := &inlineExpr{expression: expression, vars: variables}

	if segments, err := splitSegments(expression, variables...); err != nil {
		return nil, err
	} else {
		expr.segments = segments
	}
	return expr, nil
}
