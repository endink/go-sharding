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

package util

import (
	"bytes"
	"encoding/json"
)

func JsonClone(dest interface{}, source interface{}) error {
	data, err := json.Marshal(source)
	if err != nil {
		return err
	}
	return json.Unmarshal(data, dest)
}

func CheckJsonEqual(a interface{}, b interface{}) (bool, error) {
	ja, err := json.Marshal(a)
	if err != nil {
		return false, err
	}
	jb, err := json.Marshal(b)
	if err != nil {
		return false, err
	}

	return bytes.Equal(ja, jb), nil

}

func JsonEqual(a interface{}, b interface{}) bool {
	ja, err := json.Marshal(a)
	if err != nil {
		println(err.Error())
		return false
	}
	jb, err := json.Marshal(b)
	if err != nil {
		println(err.Error())
		return false
	}
	return bytes.Equal(ja, jb)

}
