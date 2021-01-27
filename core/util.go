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

package core

import (
	"fmt"
	"github.com/XiaoMi/Gaea/core/comparison"
	"os"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

var LineSeparator string = "\n"

var Nothing = struct{}{}

// BoolIndex rolled array switch mark
type BoolIndex struct {
	index int32
}

// Set set index value
func (b *BoolIndex) Set(index bool) {
	if index {
		atomic.StoreInt32(&b.index, 1)
	} else {
		atomic.StoreInt32(&b.index, 0)
	}
}

// Get return current, next, current bool value
func (b *BoolIndex) Get() (int32, int32, bool) {
	index := atomic.LoadInt32(&b.index)
	if index == 1 {
		return 1, 0, true
	}
	return 0, 1, false
}

// ItoString interface to string
func ItoString(a interface{}) (bool, string) {
	switch a.(type) {
	case nil:
		return false, "NULL"
	case []byte:
		return true, string(a.([]byte))
	default:
		return false, fmt.Sprintf("%v", a)
	}
}

// Int2TimeDuration convert int to Time.Duration
func Int2TimeDuration(t int) (time.Duration, error) {
	tmp := strconv.Itoa(t)
	tmp = tmp + "s"
	idleTimeout, err := time.ParseDuration(tmp)
	if err != nil {
		return 0, err

	}
	return idleTimeout, nil
}

func IsWindows() bool {
	sysType := runtime.GOOS

	return strings.ToLower(sysType) == "windows"

}

func FolderExists(name string) bool {
	info, err := os.Lstat(name)
	if err != nil {
		if os.IsNotExist(err) {
			return false
		}
	}
	return info.IsDir()
}

func FileExists(name string) bool {
	info, err := os.Lstat(name)
	if err != nil {
		if os.IsNotExist(err) {
			return false
		}
	}
	return !info.IsDir()
}

func IfBlank(value string, blankValue string) string {
	if strings.TrimSpace(value) == "" {
		return blankValue
	}
	return value
}

func IfBlankAndTrim(value string, blankValue string) string {
	v := strings.TrimSpace(value)
	if v == "" {
		return blankValue
	}
	return v
}

func StringSliceEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}

	if (a == nil) != (b == nil) {
		return false
	}

	b = b[:len(a)]
	for i, v := range a {
		if v != b[i] {
			return false
		}
	}

	return true
}

func DistinctSliceAndTrim(slice []string) []string {
	result := make([]string, 0, len(slice))
	temp := map[string]struct{}{}
	for _, item := range slice {
		trim := strings.TrimSpace(item)
		if trim != "" {
			if _, ok := temp[trim]; !ok {
				temp[item] = struct{}{}
				result = append(result, trim)
			}
		}
	}
	return result
}

var identityRegex *regexp.Regexp
var identityRegexOnce sync.Once

//验证由字母数字下划线组成的标识符，且必须以字母开头
func ValidateIdentifier(identifier string) error {
	identityRegexOnce.Do(func() {
		identityRegex, _ = regexp.Compile(`^[A-Za-z]+[A-Za-z0-9_-]*$`)
	})
	if !identityRegex.MatchString(identifier) {
		return fmt.Errorf("identifier must starts with a letter and letters, numbers, underline(_), minus(-) are allowed, given value: %s", identifier)
	}
	return nil
}

func TrimAndLower(v string) string {
	return strings.ToLower(strings.TrimSpace(v))
}

func TrimAndLowerArray(array []string) []string {
	result := make([]string, len(array))
	for i, s := range array {
		result[i] = TrimAndLower(s)
	}
	return result
}

//求并集
func Union(slice1, slice2 []string) []string {
	m := make(map[string]int)
	for _, v := range slice1 {
		m[v]++
	}

	for _, v := range slice2 {
		times, _ := m[v]
		if times == 0 {
			slice1 = append(slice1, v)
		}
	}
	return slice1
}

//求交集
func Intersect(slice1, slice2 []string) []string {
	m := make(map[string]struct{})
	nothing := struct{}{}
	nn := make([]string, 0, comparison.MinInt(len(slice1), len(slice2)))
	for _, v := range slice1 {
		m[v] = nothing
	}

	for _, v := range slice2 {
		_, ok := m[v]
		if ok {
			nn = append(nn, v)
		}
	}
	return nn
}

//求差集 slice1-并集
func Difference(slice1, slice2 []string) []string {
	m := make(map[string]int)
	nn := make([]string, 0)
	inter := Intersect(slice1, slice2)
	for _, v := range inter {
		m[v]++
	}

	for _, value := range slice1 {
		times, _ := m[value]
		if times == 0 {
			nn = append(nn, value)
		}
	}
	return nn
}

func reduce(slices [][]interface{}, out *[][]interface{}, idx int, idxes []int) {
	if idx < len(slices) {
		for i := 0; i < len(slices[idx]); i++ {
			idxes[idx] = i
			reduce(slices, out, idx+1, idxes)
		}
	} else {
		var cm []interface{}
		for i := 0; i < len(slices); i++ {
			cm = append(cm, slices[i][idxes[i]])
		}
		*out = append(*out, cm)
	}
}

// 笛卡尔积算法 多个数组的排列组合
// 例:{{1,2,3}, {4,5}}, 输出{{1,4}, {1,5}, {2,4}, {2,5}, {3,4}, {3,5}}
func Permute(slices [][]interface{}) [][]interface{} {
	var result [][]interface{}
	idxes := make([]int, len(slices))
	reduce(slices, &result, 0, idxes)
	return result
}

func reduceString(slices [][]string, out *[][]string, idx int, idxes []int) {
	if idx < len(slices) {
		for i := 0; i < len(slices[idx]); i++ {
			idxes[idx] = i
			reduceString(slices, out, idx+1, idxes)
		}
	} else {
		var cm []string
		for i := 0; i < len(slices); i++ {
			cm = append(cm, slices[i][idxes[i]])
		}
		*out = append(*out, cm)
	}
}

// 笛卡尔积算法 多个数组的排列组合
// 例:{{1,2,3}, {4,5}}, 输出{{1,4}, {1,5}, {2,4}, {2,5}, {3,4}, {3,5}}
func PermuteString(slices [][]string) [][]string {
	var result [][]string
	idxes := make([]int, len(slices))
	reduceString(slices, &result, 0, idxes)
	return result
}
