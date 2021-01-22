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

var _ Range = &defaultRange{}

const (
	RangeActionContains  RangeAction = "ContainsValue"
	RangeActionIntersect RangeAction = "Intersect"
	RangeActionUnion     RangeAction = "Union"
)

type Range interface {
	fmt.Stringer
	LowerBound() interface{}
	UpperBound() interface{}
	HasLower() bool
	HasUpper() bool
	ContainsValue(value interface{}) (bool, error)
	Contains(value Range) (bool, error)
	Intersect(value Range) (Range, error)
	HasIntersection(v Range) (bool, error)
	Union(value Range) (Range, error)
	ValueKind() reflect.Kind
	Equals(value interface{}) bool
}

var (
	ErrRangeBoundTypeNotSame       = errors.New("different types of boundary values cannot create range")
	ErrRangeInvalidBound           = errors.New("the lower bound of the range cannot be greater than the upper bound")
	ErrRangeBoundTypeUnsupported   = errors.New("boundary value types for the range are not supported")
	ErrNilRangeOperationNotAllowed = errors.New("the range used for the operation cannot be empty")
)

type defaultRange struct {
	Lower interface{}
	Upper interface{}
	HasL  bool
	HasU  bool
	kind  reflect.Kind
}

func NewAtLeastRange(min interface{}) Range {
	r, _ := NewRange(min, nil)
	return r
}

func NewAtMostRange(max interface{}) Range {
	r, _ := NewRange(nil, max)
	return r
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

func (d *defaultRange) Equals(v interface{}) bool {
	if v == nil {
		return false
	}
	if value, ok := v.(Range); !ok {
		return false
	} else {
		return d.ValueKind() == value.ValueKind() &&
			d.HasLower() == value.HasLower() &&
			d.HasUpper() == value.HasUpper() &&
			d.LowerBound() == value.LowerBound() &&
			d.UpperBound() == value.UpperBound()
	}
}

func (d *defaultRange) LowerBound() interface{} {
	if !d.HasL {
		return nil
	}
	return d.Lower
}

func (d *defaultRange) UpperBound() interface{} {
	if !d.HasU {
		return nil
	}
	return d.Upper
}

func (d *defaultRange) HasLower() bool {
	return d.HasL
}

func (d *defaultRange) HasUpper() bool {
	return d.HasU
}

func (d *defaultRange) Contains(value Range) (bool, error) {
	if value == nil {
		return false, ErrNilRangeOperationNotAllowed
	}
	if !d.HasL && !d.HasU {
		return true, nil
	}

	var lowerCompared, UpperCompared int
	if d.HasLower() {
		if !value.HasLower() {
			return false, nil
		}

		if r, err := comparison.Compare(d.LowerBound(), value.LowerBound()); err != nil {
			return false, err
		} else {
			lowerCompared = r
		}
	}

	if d.HasUpper() {
		if !value.HasUpper() {
			return false, nil
		}

		if r, err := comparison.Compare(d.UpperBound(), value.UpperBound()); err != nil {
			return false, err
		} else {
			UpperCompared = r
		}
	}

	return lowerCompared <= 0 && UpperCompared >= 0, nil
}

func (d *defaultRange) ContainsValue(value interface{}) (bool, error) {
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
		return false, ErrNilRangeOperationNotAllowed
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

	if !d.HasLower() {
		first = d
		second = v
	} else if !v.HasLower() {
		first = v
		second = d
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
	return first, second, nil
}

func (d *defaultRange) Union(v Range) (Range, error) {
	if has, err := d.HasIntersection(v); err != nil {
		return nil, err
	} else if !has {
		return nil, nil
	}

	var err error
	var lower interface{} = nil
	var upper interface{} = nil

	if d.HasLower() && v.HasLower() {
		lower, err = comparison.Min(d.LowerBound(), v.LowerBound())
		if err != nil {
			return nil, err
		}
	}

	if d.HasUpper() && v.HasUpper() {
		upper, err = comparison.Max(d.UpperBound(), v.UpperBound())
		if err != nil {
			return nil, err
		}
	}

	newRange, err := NewRange(lower, upper)
	if err != nil {
		return nil, err
	}

	r := newRange.(*defaultRange)
	if d.kind != reflect.Invalid {
		r.kind = d.kind
	} else if v.ValueKind() != reflect.Invalid {
		r.kind = v.ValueKind()
	}

	return r, nil
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

	if d.kind != reflect.Invalid {
		newRange.kind = d.kind
	} else if v.ValueKind() != reflect.Invalid {
		newRange.kind = v.ValueKind()
	}

	return newRange, nil
}

func (d *defaultRange) ValueKind() reflect.Kind {
	return d.kind
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
