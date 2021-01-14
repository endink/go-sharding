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
