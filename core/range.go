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
	IsUpperClosed() bool
	IsLowerClosed() bool
}

var (
	ErrRangeBoundTypeNotSame       = errors.New("different types of boundary values cannot create range")
	ErrRangeInvalidBound           = errors.New("the lower bound of the range cannot be greater than the upper bound")
	ErrRangeBoundTypeUnsupported   = errors.New("boundary value types for the range are not supported")
	ErrNilRangeOperationNotAllowed = errors.New("the range used for the operation cannot be empty")
)

type defaultRange struct {
	Lower  interface{}
	Upper  interface{}
	HasL   bool
	HasU   bool
	kind   reflect.Kind
	CloseL bool
	CloseU bool
}

func NewRangeCloseOpen(min interface{}, max interface{}) (Range, error) {
	return NewRange(min, max, true, false)
}

func NewRangeOpenClose(min interface{}, max interface{}) (Range, error) {
	return NewRange(min, max, false, true)
}

func NewRangeOpen(min interface{}, max interface{}) (Range, error) {
	return NewRange(min, max, false, false)
}

func NewRangeClose(min interface{}, max interface{}) (Range, error) {
	return NewRange(min, max, true, true)
}

func NewRange(min interface{}, max interface{}, closeLower bool, closeUppper bool) (Range, error) {
	r := &defaultRange{
		CloseL: closeLower,
		CloseU: closeUppper,
	}

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

func (d *defaultRange) IsUpperClosed() bool {
	return d.CloseU
}

func (d *defaultRange) IsLowerClosed() bool {
	return d.CloseL
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
			d.UpperBound() == value.UpperBound() &&
			(!d.HasLower() || d.IsLowerClosed() == value.IsLowerClosed()) &&
			(!d.HasUpper() || d.IsUpperClosed() == value.IsUpperClosed())
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
	if d.HasLower() {
		r, err := comparison.Compare(d.Lower, value)
		if err != nil {
			return false, err
		}
		if (d.IsLowerClosed() && r > 0) || (!d.IsLowerClosed() && r >= 0) {
			return false, nil
		}
	}

	if d.HasUpper() {
		r, err := comparison.Compare(d.Upper, value)
		if err != nil {
			return false, err
		}
		if (d.IsUpperClosed() && r < 0) || (!d.IsUpperClosed() && r <= 0) {
			return false, nil
		}
	}

	return true, nil
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
		firstValue := first.UpperBound()
		secondValue := second.LowerBound()
		if r, err := comparison.Compare(firstValue, secondValue); err != nil {
			return false, err
		} else {
			switch r {
			case 0:
				//一开一闭临界时就有交集
				return first.IsUpperClosed() || second.IsLowerClosed(), nil
			default:
				return r >= 0, nil
			}
		}
	}

	return true, nil
}

func sortByUpper(v Range, d Range) (Range, Range, error) {
	var first, second Range

	if !d.HasUpper() {
		first = v
		second = d
	} else if !v.HasUpper() {
		first = d
		second = v
	} else {
		r, err := comparison.Compare(v.UpperBound(), d.UpperBound())
		if err != nil {
			return nil, nil, err
		}
		switch r {
		case 0:
			if v.IsUpperClosed() {
				first = d
				second = v
			} else {
				first = v
				second = d
			}
		default:
			if r < 0 {
				first = v
				second = d
			} else {
				first = d
				second = v
			}
		}

	}
	return first, second, nil
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
		switch r {
		case 0:
			if v.IsLowerClosed() {
				first = v
				second = d
			} else {
				first = d
				second = v
			}
		default:
			if r < 0 {
				first = v
				second = d
			} else {
				first = d
				second = v
			}
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
	var closeLower, closeUpper bool

	if d.HasLower() && v.HasLower() {
		first, _, e := sortByLower(d, v)
		if e != nil {
			return nil, e
		}
		lower = first.LowerBound()
		closeLower = first.IsLowerClosed()
	}

	if d.HasUpper() && v.HasUpper() {
		_, second, e := sortByUpper(d, v)
		if e != nil {
			return nil, e
		}
		upper = second.UpperBound()
		closeUpper = second.IsUpperClosed()
	}

	newRange, err := NewRange(lower, upper, closeLower, closeUpper)
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

	first, second, err := sortByLower(d, v)
	if err != nil {
		return nil, err
	}
	newRange.CloseL = second.IsLowerClosed()
	newRange.Lower = second.LowerBound()
	newRange.HasL = second.HasLower()

	if !first.HasUpper() { //完全覆盖的情况
		newRange.CloseU = second.IsUpperClosed()
		newRange.Upper = second.UpperBound()
		newRange.HasU = second.HasUpper()
	} else if !second.HasUpper() { //不应该出现这种情况
		newRange.CloseU = first.IsUpperClosed()
		newRange.Upper = first.UpperBound()
		newRange.HasU = first.HasUpper()
	} else {
		c, e := comparison.Compare(first.UpperBound(), second.UpperBound())
		if e != nil {
			return nil, e
		}
		switch c {
		case 0:
			if !first.IsUpperClosed() {
				newRange.CloseU = first.IsUpperClosed()
				newRange.HasU = first.HasUpper()
				newRange.Upper = first.UpperBound()
			} else {
				newRange.CloseU = second.IsUpperClosed()
				newRange.HasU = second.HasUpper()
				newRange.Upper = second.UpperBound()
			}
		case 1:
			newRange.CloseU = second.IsUpperClosed()
			newRange.HasU = second.HasUpper()
			newRange.Upper = second.UpperBound()
		case -1:
			newRange.CloseU = first.IsUpperClosed()
			newRange.HasU = first.HasUpper()
			newRange.Upper = first.UpperBound()
		}
	}

	if d.kind != reflect.Invalid {
		newRange.kind = d.kind
	} else if v.ValueKind() != reflect.Invalid {
		newRange.kind = v.ValueKind()
	}

	if newRange.HasLower() && newRange.HasUpper() && newRange.LowerBound() == newRange.UpperBound() {
		newRange.CloseL = true
		newRange.CloseU = true
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
	lowerStr := "["
	upperStr := "]"

	if !d.IsLowerClosed() {
		lowerStr = "("
	}
	if !d.IsUpperClosed() {
		upperStr = ")"
	}
	return fmt.Sprint(lowerStr, min, ", ", max, upperStr)
}
