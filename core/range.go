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

package core

import (
	"errors"
	"fmt"
	"github.com/XiaoMi/Gaea/core/comparison"
	"reflect"
)

type RangeAction string

const (
	RangeActionContains  RangeAction = "Contains"
	RangeActionIntersect RangeAction = "Intersect"
	RangeActionUnion     RangeAction = "Union"
)

type Range interface {
	fmt.Stringer
	LowerBound() interface{}
	UpperBound() interface{}
	HasLower() bool
	HasUpper() bool
	Contains(value interface{}) (bool, error)
	Intersect(value Range) (Range, error)
	HasIntersection(v Range) (bool, error)
}

var (
	ErrRangeBoundTypeNotSame     = errors.New("different types of boundary values cannot create range")
	ErrRangeInvalidBound         = errors.New("the lower bound of the range cannot be greater than the upper bound")
	ErrRangeBoundTypeUnsupported = errors.New("boundary value types for the range are not supported")
)

type defaultRange struct {
	Lower interface{}
	Upper interface{}
	HasL  bool
	HasU  bool
	kind  reflect.Kind
}

func NewRange(min interface{}, max interface{}) (Range, error) {
	r := &defaultRange{}

	if min == nil {
		r.HasL = false
	} else if !comparison.IsCompareSupported(min) {
		return nil, ErrRangeBoundTypeUnsupported
	} else {
		r.HasL = true
		r.Lower = min
		r.kind = reflect.TypeOf(min).Kind()
	}

	if max == nil {
		r.HasU = false
	} else {
		if r.HasL {
			maxKind := reflect.TypeOf(max).Kind()
			if maxKind != r.kind {
				return nil, ErrRangeBoundTypeNotSame
			}
		} else if !comparison.IsCompareSupported(max) {
			return nil, ErrRangeBoundTypeUnsupported
		}
		r.HasU = true
		r.Upper = max
	}

	if r.HasL && r.HasU {
		if c, _ := comparison.Compare(r.Lower, r.Upper); c > 0 {
			return nil, ErrRangeInvalidBound
		}
	}

	return r, nil
}

func (d *defaultRange) LowerBound() interface{} {
	return d.Lower
}

func (d *defaultRange) UpperBound() interface{} {
	return d.Upper
}

func (d *defaultRange) HasLower() bool {
	return d.HasL
}

func (d *defaultRange) HasUpper() bool {
	return d.HasU
}

func (d *defaultRange) Contains(value interface{}) (bool, error) {
	var outMin, outMax bool
	if d.HasL {
		r, err := comparison.Compare(d.Lower, value)
		if err != nil {
			return false, err
		}
		outMin = r > 0
	}

	if d.HasU {
		r, err := comparison.Compare(d.Upper, value)
		if err != nil {
			return false, err
		}
		outMax = r < 0
	}

	return !outMin && !outMax, nil
}

func (d *defaultRange) HasIntersection(v Range) (bool, error) {
	if v == nil {
		return false, errors.New("the range used to intersect cannot be nil")
	}

	if (!v.HasLower() && !v.HasUpper()) || (!d.HasLower() && !d.HasUpper()) {
		return true, nil
	}
	first, second, err := sortByLower(d, v)
	if err != nil {
		return false, err
	}

	if first.HasUpper() && second.HasLower() {
		if r, err := comparison.Compare(first.UpperBound(), second.LowerBound()); err != nil {
			return false, err
		} else {
			return r >= 0, nil
		}
	}

	return true, nil
}

func sortByLower(v Range, d Range) (Range, Range, error) {
	var first, second Range

	if v.HasLower() {
		if !d.HasLower() {
			first = d
			second = v
		} else {
			r, err := comparison.Compare(v.LowerBound(), d.LowerBound())
			if err != nil {
				return nil, nil, err
			}
			if r < 0 {
				first = v
				second = d
			} else {
				first = d
				second = v
			}
		}
	} else {
		first = v
		second = d
	}
	return first, second, nil
}

func (d *defaultRange) Intersect(v Range) (Range, error) {
	if has, err := d.HasIntersection(v); err != nil {
		return nil, err
	} else if !has {
		return nil, nil
	}

	newRange := &defaultRange{}

	if d.HasL && v.HasLower() {
		r, err := comparison.Max(d.Lower, v.LowerBound())
		if err != nil {
			return nil, err
		}
		newRange.Lower = r
		newRange.HasL = true
	} else if !v.HasLower() && d.HasL {
		newRange.Lower = v.LowerBound()
		newRange.HasL = true
	} else if v.HasLower() && !d.HasL {
		newRange.Lower = v.LowerBound()
		newRange.HasL = true
	}

	if d.HasU && v.HasUpper() {
		r, err := comparison.Min(d.Upper, v.UpperBound())
		if err != nil {
			return nil, err
		}
		newRange.Upper = r
		newRange.HasU = true
	} else if !v.HasUpper() && d.HasU {
		newRange.Upper = d.Upper
		newRange.HasU = true
	} else if v.HasUpper() && !d.HasU {
		newRange.Upper = v.UpperBound()
		newRange.HasU = true
	}

	return newRange, nil
}

func (d *defaultRange) String() string {
	var min, max string
	if d.HasL {
		min = fmt.Sprint(d.Lower)
	}
	if d.HasU {
		max = fmt.Sprint(d.Upper)
	}
	return fmt.Sprintf("%s..%s", min, max)
}
