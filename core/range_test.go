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
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
)

func assertHasIntersection(t assert.TestingT, r1 Range, r2 Range, has bool) {
	i, err := r1.HasIntersection(r2)
	noErr := assert.Nil(t, err, fmt.Sprintf("can not intersect with %s and %s", r1, r2))
	if noErr {
		if has {
			assert.True(t, i, fmt.Sprintf("%s and %s should has intersection", r1, r2))
		} else {
			assert.False(t, i, fmt.Sprintf("%s and %s should has no intersection", r1, r2))
		}
	}
}

func testNewRangeWithValue(t *testing.T, min interface{}, max interface{}, hasError bool) {
	r, err := NewRangeClose(min, max)

	if hasError {
		assert.Error(t, err)
	} else {

		if ok := assert.Nil(t, err); !ok {
			return
		}

		if ok := assert.True(t, r.HasLower()); !ok {
			return
		}

		if ok := assert.True(t, r.HasUpper()); !ok {
			return
		}

		r, err = NewRangeClose(nil, max)
		if ok := assert.Nil(t, err); !ok {
			return
		}

		if ok := assert.False(t, r.HasLower()); !ok {
			return
		}
		if ok := assert.True(t, r.HasUpper()); !ok {
			return
		}

		r, err = NewRangeClose(min, nil)
		if ok := assert.Nil(t, err); !ok {
			return
		}

		if ok := assert.True(t, r.HasLower()); !ok {
			return
		}
		assert.False(t, r.HasUpper())
	}
}

func TestNewRange(t *testing.T) {
	testNewRangeWithValue(t, -100, 12323, false)
	testNewRangeWithValue(t, -3.333, 5.33333, false)
	testNewRangeWithValue(t, "a", "z", false)
	testNewRangeWithValue(t, 3, 3, false)
	testNewRangeWithValue(t, 100, 3, true)
	testNewRangeWithValue(t, 3.00001, 3, true)
	testNewRangeWithValue(t, "b", "a", true)
	testNewRangeWithValue(t, "a", "a", false)
}

func testContainsWithValue(t *testing.T, min interface{}, max interface{}, value interface{}, contains bool) {
	r, err := NewRangeClose(min, max)
	assert.Nil(t, err)

	c, err := r.ContainsValue(value)
	assert.Nil(t, err)

	if contains {
		assert.True(t, c, fmt.Sprintf("%v should be in %s", value, r))
	} else {
		assert.False(t, c, fmt.Sprintf("%v should not in %s", value, r))
	}
}

func TestContainsValue(t *testing.T) {
	testContainsWithValue(t, -100, 100, 99, true)
	testContainsWithValue(t, -100, 100, 101, false)
	testContainsWithValue(t, -100, 100, -101, false)

	testContainsWithValue(t, 3.3, 5.5, 4.4, true)
	testContainsWithValue(t, 3.3, 5.5, 3.29, false)
	testContainsWithValue(t, 3.3, 5.5, 5.51, false)

	testContainsWithValue(t, "d", "h", "e", true)
	testContainsWithValue(t, "d", "h", "i", false)
	testContainsWithValue(t, "d", "h", "c", false)
}

func TestHasIntersection(t *testing.T) {
	var r1, r2 Range

	r1, _ = NewRangeClose(100, 200)
	r2, _ = NewRangeClose(20, 30)
	assertHasIntersection(t, r1, r2, false)

	r1, _ = NewRangeClose(100, 200)
	r2, _ = NewRangeClose(20, nil)
	assertHasIntersection(t, r1, r2, true)

	r1, _ = NewRangeClose(nil, 200)
	r2, _ = NewRangeClose(20, nil)
	assertHasIntersection(t, r1, r2, true)

	r1, _ = NewRangeClose(100, nil)
	r2, _ = NewRangeClose(20, nil)
	assertHasIntersection(t, r1, r2, true)

	r1, _ = NewRangeClose(nil, nil)
	r2, _ = NewRangeClose(nil, nil)
	assertHasIntersection(t, r1, r2, true)

	r1, _ = NewRangeClose(nil, 30)
	r2, _ = NewRangeClose(nil, 31)
	assertHasIntersection(t, r1, r2, true)

	r1, _ = NewRangeClose(nil, 30)
	r2, _ = NewRangeClose(50, nil)
	assertHasIntersection(t, r1, r2, false)

	r1, _ = NewRangeClose(nil, 60)
	r2, _ = NewRangeClose(50, nil)
	assertHasIntersection(t, r1, r2, true)

}

func TestIntRangeIntersect(t *testing.T) {
	var r1, r2 Range

	r1, _ = NewRangeClose(100, 200)
	r2, _ = NewRangeClose(20, 30)
	testIntersectWithValue(t, r1, r2, nil, nil)

	r1, _ = NewRangeClose(100, 200)
	r2, _ = NewRangeClose(20, 150)
	testIntersectWithValue(t, r1, r2, 100, 150)

	r1, _ = NewRangeClose(100, 1000)
	r2, _ = NewRangeClose(101, 150)
	testIntersectWithValue(t, r1, r2, 101, 150)

	r1, _ = NewRangeClose(nil, 1000)
	r2, _ = NewRangeClose(101, nil)
	testIntersectWithValue(t, r1, r2, 101, 1000)

	r1, _ = NewRangeClose(20, 30)
	r2, _ = NewRangeClose(31, 40)
	testIntersectWithValue(t, r1, r2, nil, nil)

	r1, _ = NewRangeClose(20, 30)
	r2, _ = NewRangeClose(30, 40)
	testIntersectWithValue(t, r1, r2, 30, 30)

	r1, _ = NewRangeClose(nil, nil)
	r2, _ = NewRangeClose(30, 40)
	testIntersectWithValue(t, r1, r2, 30, 40)

	r1, _ = NewRangeClose(nil, 50)
	r2, _ = NewRangeClose(30, nil)
	testIntersectWithValue(t, r1, r2, 30, 50)

	r1, _ = NewRangeClose(nil, 30)
	r2, _ = NewRangeClose(45, nil)
	testIntersectWithValue(t, r1, r2, nil, nil)

}

func TestIntRangeUnion(t *testing.T) {
	var r1, r2 Range

	r1, _ = NewRangeClose(100, 200)
	r2, _ = NewRangeClose(20, 30)
	testUnionWithValue(t, r1, r2, nil, nil, false)

	r1, _ = NewRangeClose(100, 200)
	r2, _ = NewRangeClose(20, 150)
	testUnionWithValue(t, r1, r2, 20, 200, true)

	r1, _ = NewRangeClose(100, 1000)
	r2, _ = NewRangeClose(101, 150)
	testUnionWithValue(t, r1, r2, 100, 1000, true)

	r1, _ = NewRangeClose(nil, 1000)
	r2, _ = NewRangeClose(101, nil)
	testUnionWithValue(t, r1, r2, nil, nil, true)

	r1, _ = NewRangeClose(20, 30)
	r2, _ = NewRangeClose(31, 40)
	testUnionWithValue(t, r1, r2, nil, nil, false)

	r1, _ = NewRangeClose(20, 30)
	r2, _ = NewRangeClose(30, 40)
	testUnionWithValue(t, r1, r2, 20, 40, true)

	r1, _ = NewRangeClose(nil, nil)
	r2, _ = NewRangeClose(30, 40)
	testUnionWithValue(t, r1, r2, nil, nil, true)

	r1, _ = NewRangeClose(nil, 50)
	r2, _ = NewRangeClose(30, nil)
	testUnionWithValue(t, r1, r2, nil, nil, true)

	r1, _ = NewRangeClose(nil, 30)
	r2, _ = NewRangeClose(45, nil)
	testUnionWithValue(t, r1, r2, nil, nil, false)

}

func TestFloatRangeIntersect(t *testing.T) {
	var r1, r2 Range

	r1, _ = NewRangeClose(100.1, 200.2)
	r2, _ = NewRangeClose(20.1, 30.1)
	testIntersectWithValue(t, r1, r2, nil, nil)

	r1, _ = NewRangeClose(100.3, 200.4)
	r2, _ = NewRangeClose(20.3, 150.8)
	testIntersectWithValue(t, r1, r2, 100.3, 150.8)

	r1, _ = NewRangeClose(100.1, 1000.989)
	r2, _ = NewRangeClose(101.33, 150.44)
	testIntersectWithValue(t, r1, r2, 101.33, 150.44)

	r1, _ = NewRangeClose(nil, 1000.997)
	r2, _ = NewRangeClose(101.998, nil)
	testIntersectWithValue(t, r1, r2, 101.998, 1000.997)

	r1, _ = NewRangeClose(20.1, 30.2)
	r2, _ = NewRangeClose(31.3, 40.4)
	testIntersectWithValue(t, r1, r2, nil, nil)

	r1, _ = NewRangeClose(20.3123, 30.333)
	r2, _ = NewRangeClose(30.333, 40.333)
	testIntersectWithValue(t, r1, r2, 30.333, 30.333)

}

func assertRange(t *testing.T, r1 Range, r2 Range, excepted Range, actual Range, opCode string) {
	if (excepted == nil) && !assert.Nil(t, actual, fmt.Sprintf("%s %s %s result should be nil range", r1, opCode, r2)) {
		return
	}

	if excepted != nil {
		if !assert.NotNil(t, actual, fmt.Sprintf("%s %s %s result should not be nil range", r1, opCode, r2)) {
			return
		}
		if excepted.HasLower() && !assert.True(t, actual.HasLower(), fmt.Sprintf("%s %s %s result should has lower bound", r1, opCode, r2)) {
			return
		}

		if excepted.HasUpper() && !assert.True(t, actual.HasUpper(), fmt.Sprintf("%s %s %s result should has upper bound", r1, opCode, r2)) {
			return
		}

		rr, _ := NewRange(excepted.LowerBound(), excepted.UpperBound(), excepted.IsLowerClosed(), excepted.IsUpperClosed())
		if assert.NotNil(t, actual, fmt.Sprintf("%s %s %s result should not be nil", r1, opCode, r2)) {
			assert.True(t, rr.Equals(actual),
				fmt.Sprintf("%s %s %s should be %s, actual is %s", r1, opCode, r2, rr, actual))
		}
	}
}

func testIntersectWithValue(t *testing.T, r1 Range, r2 Range, resultMin, resultMax interface{}) {
	r, err := r1.Intersect(r2)
	assert.Nil(t, err)

	if (resultMax != nil || resultMin != nil) && !assert.NotNil(t, r, fmt.Sprintf("%s & %s result should not be nil range", r1, r2)) {
		return
	}

	if resultMin != nil && !assert.True(t, r.HasLower(), fmt.Sprintf("%s & %s result should has lower bound", r1, r2)) {
		return
	}

	if resultMax != nil && !assert.True(t, r.HasUpper(), fmt.Sprintf("%s & %s result should has upper bound", r1, r2)) {
		return
	}

	if resultMin == nil && resultMax == nil {
		assert.Nil(t, r, fmt.Sprintf("%s & %s result should be nil range", r1, r2))
	} else {
		rr, _ := NewRangeClose(resultMin, resultMax)
		if assert.NotNil(t, r, fmt.Sprintf("%s & %s result should not be nil", r1, r2)) {
			assert.True(t, r.LowerBound() == resultMin && r.UpperBound() == resultMax,
				fmt.Sprintf("%s & %s should be %s, actual is %s", r1, r2, rr, r))
		}
	}
}

func testIntersect(t *testing.T, r1 Range, r2 Range, excepted Range) {
	r, err := r1.Intersect(r2)
	assert.Nil(t, err)

	assertRange(t, r1, r2, excepted, r, "&")
}

func testUnion(t *testing.T, r1 Range, r2 Range, excepted Range) {
	r, err := r1.Union(r2)
	assert.Nil(t, err)

	assertRange(t, r1, r2, excepted, r, "|")
}

func testUnionWithValue(t *testing.T, r1 Range, r2 Range, resultMin, resultMax interface{}, hasResult bool) {
	r, err := r1.Union(r2)
	assert.Nil(t, err)

	if (resultMax != nil || resultMin != nil) && !assert.NotNil(t, r, fmt.Sprintf("%s | %s result should not be nil range", r1, r2)) {
		return
	}

	if resultMin != nil && !assert.True(t, r.HasLower(), fmt.Sprintf("%s | %s result should has lower bound", r1, r2)) {
		return
	}

	if resultMax != nil && !assert.True(t, r.HasUpper(), fmt.Sprintf("%s | %s result should has upper bound", r1, r2)) {
		return
	}

	if resultMin == nil && resultMax == nil {
		if !hasResult {
			assert.Nil(t, r, fmt.Sprintf("%s | %s result should be nil range", r1, r2))
		} else {
			assert.True(t, !r.HasUpper() && !r.HasLower(), fmt.Sprintf("%s | %s result should be *~*", r1, r2))
		}
	} else {
		rr, _ := NewRangeClose(resultMin, resultMax)
		if assert.NotNil(t, r, fmt.Sprintf("%s | %s result should be nil", r1, r2)) {
			assert.True(t, r.LowerBound() == resultMin && r.UpperBound() == resultMax,
				fmt.Sprintf("%s | %s should be %s, actual is %s", r1, r2, rr, r))
		}
	}
}

func TestContainsValueComplex(t *testing.T) {
	testOpenCloseContains(t, createRangeOpenClose(1, 10), 1, false)
	testOpenCloseContains(t, createRangeOpen(1, 10), 1, false)
	testOpenCloseContains(t, createRangeCloseOpen(1, 10), 1, true)
	testOpenCloseContains(t, createRangeOpenClose(1, 10), 10, true)
	testOpenCloseContains(t, createRangeOpen(1, 10), 10, false)
	testOpenCloseContains(t, createRangeCloseOpen(1, 10), 10, false)
}

func TestInsectComplex(t *testing.T) {
	r1 := createRangeCloseOpen(1, 3)
	r2 := createRangeOpenClose(3, 100)
	testIntersect(t, r1, r2, nil)

	r1 = createRangeOpenClose(1, 3)
	r2 = createRangeOpenClose(3, 100)
	testIntersect(t, r1, r2, createRange(3, 3))

	r1 = createRangeOpenClose(1, 3)
	r2 = createRangeOpen(3, 100)
	testIntersect(t, r1, r2, createRange(3, 3))

	r1 = createRangeOpen(1, 5)
	r2 = createRangeOpen(3, 100)
	testIntersect(t, r1, r2, createRangeOpen(3, 5))

	r1 = createRange(1, 5)
	r2 = createRangeOpen(3, 100)
	testIntersect(t, r1, r2, createRangeOpenClose(3, 5))

	r1 = createRange(1, 100)
	r2 = createRangeOpen(1, 100)
	testIntersect(t, r1, r2, createRangeOpen(1, 100))

	r1 = createRange(1, 100)
	r2 = createRangeCloseOpen(1, 100)
	testIntersect(t, r1, r2, createRangeCloseOpen(1, 100))

	r1 = createRange(1, 100)
	r2 = createRangeOpenClose(1, 100)
	testIntersect(t, r1, r2, createRangeOpenClose(1, 100))

	r1 = createRangeOpen(1, 200)
	r2 = createRange(1, 100)
	testIntersect(t, r1, r2, createRangeOpenClose(1, 100))

	r1 = createRangeOpenClose(1, 100)
	r2 = createRangeCloseOpen(1, 100)
	testIntersect(t, r1, r2, createRangeOpen(1, 100))

	r1 = createRangeOpenClose(1, 100)
	r2 = createRangeCloseOpen(50, 100)
	testIntersect(t, r1, r2, createRangeCloseOpen(50, 100))

}

func TestUnionComplex(t *testing.T) {
	r1 := createRangeCloseOpen(1, 3)
	r2 := createRangeOpenClose(3, 100)
	testUnion(t, r1, r2, nil)

	r1 = createRangeOpenClose(1, 3)
	r2 = createRangeOpenClose(3, 100)
	testUnion(t, r1, r2, createRangeOpenClose(1, 100))

	r1 = createRangeOpenClose(1, 3)
	r2 = createRangeOpen(3, 100)
	testUnion(t, r1, r2, createRangeOpen(1, 100))

	r1 = createRangeOpen(1, 5)
	r2 = createRangeOpen(3, 100)
	testUnion(t, r1, r2, createRangeOpen(1, 100))

	r1 = createRange(1, 5)
	r2 = createRangeOpen(3, 100)
	testUnion(t, r1, r2, createRangeCloseOpen(1, 100))

	r1 = createRange(1, 100)
	r2 = createRangeOpen(1, 100)
	testUnion(t, r1, r2, createRange(1, 100))

	r1 = createRange(1, 100)
	r2 = createRangeCloseOpen(1, 100)
	testUnion(t, r1, r2, createRange(1, 100))

	r1 = createRange(1, 100)
	r2 = createRangeOpenClose(1, 100)
	testUnion(t, r1, r2, createRange(1, 100))

	r1 = createRangeOpen(1, 200)
	r2 = createRange(1, 100)
	testUnion(t, r1, r2, createRangeCloseOpen(1, 200))

	r1 = createRangeOpenClose(1, 100)
	r2 = createRangeCloseOpen(1, 100)
	testUnion(t, r1, r2, createRange(1, 100))

	r1 = createRangeOpenClose(1, 100)
	r2 = createRangeCloseOpen(50, 100)
	testUnion(t, r1, r2, createRangeOpenClose(1, 100))
}

func testOpenCloseContains(t *testing.T, r Range, value interface{}, contains bool) bool {
	c, err := r.ContainsValue(value)
	assert.Nil(t, err)
	if contains {
		return assert.True(t, c, fmt.Errorf("%s should contains %s", r, value))
	} else {
		return assert.False(t, c, fmt.Errorf("%s should not contains %s", r, value))
	}
}