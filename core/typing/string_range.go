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

type StringRange struct {
	Lower string
	Upper string
	HasL  bool
	HasU  bool
}

func (i *StringRange) Contains(value interface{}) (bool, error) {
	if v, ok := value.(string); ok {
		outMin := i.HasL && i.Lower > v
		outMax := i.HasU && i.Upper < v
		return !outMin && !outMax, nil
	}
	return false, InvalidRangeValueType(RangeActionContains, value, i)
}

func (i *StringRange) LowerBound() interface{} {
	return i.Lower
}

func (i *StringRange) UpperBound() interface{} {
	return i.Upper
}

func (i *StringRange) HasLower() bool {
	return i.HasL
}

func (i *StringRange) HasUpper() bool {
	return i.HasU
}
