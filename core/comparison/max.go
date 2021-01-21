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

package comparison

import "math"

func MaxInt(x, y int) int {
	if x < y {
		return y
	}
	return x
}

func MaxInt8(x, y int8) int8 {
	if x < y {
		return y
	}
	return x
}

func MaxInt16(x, y int16) int16 {
	if x < y {
		return y
	}
	return x
}

func MaxInt32(x, y int32) int32 {
	if x < y {
		return y
	}
	return x
}

func MaxInt64(x, y int64) int64 {
	if x < y {
		return y
	}
	return x
}

func MaxUInt(x, y uint) uint {
	if x < y {
		return y
	}
	return x
}

func MaxUInt8(x, y uint8) uint8 {
	if x < y {
		return y
	}
	return x
}

func MaxUInt16(x, y uint16) uint16 {
	if x < y {
		return y
	}
	return x
}

func MaxUInt32(x, y uint32) uint32 {
	if x < y {
		return y
	}
	return x
}

func MaxUInt64(x, y uint64) uint64 {
	if x < y {
		return y
	}
	return x
}

func MaxFloat32(x, y float32) float32 {
	if x < y {
		return y
	}
	return x
}

func MaxFloat64(x, y float64) float64 {
	return math.Max(x, y)
}

func MaxByte(x, y byte) byte {
	if x < y {
		return y
	}
	return x
}

func MaxString(x, y string) string {
	if x < y {
		return y
	}
	return x
}
