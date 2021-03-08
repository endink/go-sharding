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

package testkit

import (
	"fmt"
	"github.com/XiaoMi/Gaea/core/comparison"
	"github.com/emirpasic/gods/utils"
	"github.com/stretchr/testify/assert"
)

type equatable interface {
	Equals(v interface{}) bool
}

func ErrorDifferentInfo(excepted interface{}, actual interface{}) string {
	sb := newStringBuilder()
	sb.WriteLine("item not same")

	sb.WriteLine("excepted: ")
	sb.WriteLine(fmt.Sprintf("%v", excepted))
	sb.WriteLine()

	sb.WriteLine("actual: ")
	sb.WriteLine(fmt.Sprintf("%v", actual))
	sb.WriteLine()
	sb.WriteLine()

	return sb.String()
}

func errorDifferent(excepted []interface{}, actual []interface{}) string {
	sb := newStringBuilder()
	sb.WriteLine("array not same")

	sb.Write("excepted: ")
	utils.Sort(excepted, func(a, b interface{}) int {
		i, _ := comparison.Compare(a, b)
		return i
	})
	writeArray(sb, excepted)
	sb.WriteLine()

	sb.Write("actual: ")
	utils.Sort(actual, func(a, b interface{}) int {
		i, _ := comparison.Compare(a, b)
		return i
	})
	writeArray(sb, actual)
	sb.WriteLine()

	return sb.String()
}

func writeArray(sb *stringBuilder, excepted []interface{}) {
	if len(excepted) > 0 {
		for i, e := range excepted {
			if i == (len(excepted) - 1) {
				sb.Write(fmt.Sprint(e))
			} else {
				sb.Write(fmt.Sprint(e) + ", ")
			}
		}
	} else {
		sb.Write("<empty array>")
	}
}

func AssertEmptyArray(t assert.TestingT, actual []interface{}, msgAndArgs ...interface{}) bool {
	return AssertArrayEquals(t, nil, actual, msgAndArgs...)
}

func AssertStrArrayEquals(t assert.TestingT, excepted []string, actual []string, msgAndArgs ...interface{}) bool {
	return AssertArrayEquals(t, convertStrArray(excepted), convertStrArray(actual), msgAndArgs...)
}

func AssertArrayEquals(t assert.TestingT, excepted []interface{}, actual []interface{}, msgAndArgs ...interface{}) bool {
	if excepted == nil && actual == nil {
		return true
	}

	if len(excepted) != len(actual) {
		msg := errorDifferent(excepted, actual)
		return assert.Fail(t, msg, msgAndArgs)
	}
	var diff bool
	for _, r := range excepted {
		if !arrayContains(actual, r) {
			diff = true
			break
		}
	}

	if diff {
		msg := errorDifferent(excepted, actual)
		return assert.Fail(t, msg, msgAndArgs)
	}

	return true
}

func convertStrArray(values []string) []interface{} {
	r := make([]interface{}, len(values))

	for i, value := range values {
		r[i] = value
	}
	return r
}

func arrayContains(ranges []interface{}, value interface{}) bool {
	for _, r := range ranges {
		if r == value {
			return true
		}
		if eq, ok := value.(equatable); ok && eq.Equals(r) {
			return true
		}
	}
	return false
}
