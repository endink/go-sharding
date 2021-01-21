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

func CompareInt64(x, y int64) int {
	if x < y {
		return -1
	} else if x == y {
		return 0
	}

	return 1
}

func CompareInt(x, y int) int {
	if x < y {
		return -1
	} else if x == y {
		return 0
	}

	return 1
}

func CompareInt32(x, y int32) int {
	if x < y {
		return -1
	} else if x == y {
		return 0
	}

	return 1
}

func CompareInt16(x, y int16) int {
	if x < y {
		return -1
	} else if x == y {
		return 0
	}

	return 1
}

func CompareInt8(x, y int8) int {
	if x < y {
		return -1
	} else if x == y {
		return 0
	}

	return 1
}

func CompareByte(x, y byte) int {
	if x < y {
		return -1
	} else if x == y {
		return 0
	}

	return 1
}

func CompareFloat32(x, y float32) int {
	if x < y {
		return -1
	} else if x == y {
		return 0
	}

	return 1
}

func CompareFloat64(x, y float64) int {
	if x < y {
		return -1
	} else if x == y {
		return 0
	}

	return 1
}

func CompareUInt64(x, y uint64) int {
	if x < y {
		return -1
	} else if x == y {
		return 0
	}

	return 1
}

func CompareUInt(x, y uint) int {
	if x < y {
		return -1
	} else if x == y {
		return 0
	}

	return 1
}

func CompareUInt32(x, y uint32) int {
	if x < y {
		return -1
	} else if x == y {
		return 0
	}

	return 1
}

func CompareUInt16(x, y uint16) int {
	if x < y {
		return -1
	} else if x == y {
		return 0
	}

	return 1
}

func CompareUInt8(x, y uint8) int {
	if x < y {
		return -1
	} else if x == y {
		return 0
	}

	return 1
}
