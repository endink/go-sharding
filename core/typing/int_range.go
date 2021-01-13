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

type IntRange struct {
	Lower int
	Upper int
	HasL  bool
	HasU  bool
}

func (i *IntRange) LowerBound() interface{} {
	return i.Lower
}

func (i *IntRange) UpperBound() interface{} {
	return i.Upper
}

func (i *IntRange) HasLower() bool {
	return i.HasL
}

func (i *IntRange) HasUpper() bool {
	return i.HasU
}
