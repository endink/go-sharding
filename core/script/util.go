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
	"fmt"
	"github.com/endink/go-sharding/core"
)

func outJoin(prefix []string, suffix []string) []string {
	if len(prefix) == 0 {
		return suffix
	}
	bucket := make(map[string]struct{})
	for _, p := range prefix {
		for _, v := range suffix {
			name := fmt.Sprint(p, v)
			if name != "" {
				bucket[name] = core.Nothing
			}
		}
	}
	r := make([]string, 0, len(bucket))
	for key, _ := range bucket {
		r = append(r, key)
	}
	return r
}

func flatFill(prefix string, suffix ...string) []string {
	if prefix == "" {
		return suffix
	}

	bucket := make(map[string]struct{})
	for _, v := range suffix {
		name := fmt.Sprint(prefix, v)
		if name != "" {
			bucket[name] = core.Nothing
		}
	}
	r := make([]string, 0, len(bucket))
	for key, _ := range bucket {
		r = append(r, key)
	}
	return r
}
