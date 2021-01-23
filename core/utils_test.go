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
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestPermute(t *testing.T) {
	array1 := []interface{}{1, 3, 5}
	array2 := []interface{}{2, 4, 6}
	array3 := []interface{}{7, 8, 9}

	list := [][]interface{}{array1, array2, array3}

	result := Permute(list)

	assert.Equal(t, 27, len(result))

	for _, innerArray := range result {
		assert.Equal(t, 3, len(innerArray))
	}
}
