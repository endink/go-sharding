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

package rewriting

import (
	"errors"
	"github.com/XiaoMi/Gaea/explain"
)

func newLimit(context explain.Context) (int64, error) {
	if context.LimitLookup().HasLimit() {
		return 0, errors.New("there is none limit in plain context")
	}

	newCount := context.LimitLookup().Count()
	if context.LimitLookup().Offset() > 0 {
		newCount += context.LimitLookup().Offset()
	}

	return newCount, nil
}
