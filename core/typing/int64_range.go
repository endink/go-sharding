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

package typing

import (
	"github.com/XiaoMi/Gaea/core"
)

type Int64Range struct {
	Lower int64
	Upper int64
	HasL  bool
	HasU  bool
}

func (i *Int64Range) Contains(value interface{}) (bool, error) {
	if v, ok := value.(int64); ok {
		outMin := i.HasL && i.Lower > v
		outMax := i.HasU && i.Upper < v
		return !outMin && !outMax, nil
	}
	return false, InvalidRangeValueType(RangeActionContains, value, i)
}

func (i *Int64Range) Intersect(value core.Range) (core.Range, error) {
	if v, ok := value.(*Int64Range); ok {
		newRange := &Int64Range{}

		if i.HasL && v.HasL {
			newRange.Lower = core.MaxInt64(i.Lower, v.Lower)
			newRange.HasL = true
		} else if !v.HasL && i.HasL {
			newRange.Lower = i.Lower
			newRange.HasL = true
		} else if v.HasL && !i.HasL {
			newRange.Lower = v.Lower
			newRange.HasL = true
		}

		if i.HasU && v.HasU {
			newRange.Lower = core.MinInt64(i.Lower, v.Lower)
			newRange.HasU = true
		} else if !v.HasU && i.HasU {
			newRange.Upper = i.Upper
			newRange.HasU = true
		} else if v.HasU && !i.HasU {
			newRange.Upper = v.Upper
			newRange.HasU = true
		}

		if newRange.HasL && newRange.HasU && newRange.Upper < newRange.Lower {
			return nil, nil
		}

		return newRange, nil
	}
	return nil, InvalidRangeValueType(RangeActionIntersect, value, i)
}

func (i *Int64Range) LowerBound() interface{} {
	return i.Lower
}

func (i *Int64Range) UpperBound() interface{} {
	return i.Upper
}

func (i *Int64Range) HasLower() bool {
	return i.HasL
}

func (i *Int64Range) HasUpper() bool {
	return i.HasU
}
