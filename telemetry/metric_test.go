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

package telemetry

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestBuildMetricName(t *testing.T) {
	var name string
	name = BuildMetricName("a_")
	assert.Equal(t, "a", name)

	name = BuildMetricName("_-a._")
	assert.Equal(t, "a", name)

	name = BuildMetricName("db", "A")
	assert.Equal(t, "db_a", name)

	name = BuildMetricName("db", "AbcEdf")
	assert.Equal(t, "db_abc_edf", name)

	name = BuildMetricName("db", "...AbcEdf...")
	assert.Equal(t, "db_abc_edf", name)
}
