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
	"errors"
	"fmt"
	"github.com/XiaoMi/Gaea/core"
	"reflect"
)

type RangeAction string

const (
	RangeActionContains  RangeAction = "Contains"
	RangeActionIntersect RangeAction = "Intersect"
	RangeActionUnion     RangeAction = "Union"
)

func InvalidRangeValueType(action RangeAction, value interface{}, p core.Range) error {
	return errors.New(fmt.Sprint(
		"range invoke with invalid type",
		core.LineSeparator,
		"action: ", action,
		core.LineSeparator,
		"range type:", reflect.TypeOf(p).Name(),
		core.LineSeparator,
		"value type: ", reflect.TypeOf(value).Name()))
}
