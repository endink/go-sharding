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

package config

import (
	"github.com/XiaoMi/Gaea/core"
	"github.com/XiaoMi/Gaea/logging"
	"os"
	"path/filepath"
)

var logger = logging.GetLogger("config")

func DefaultConfigFileLocations() []string {
	files := make(map[string]bool, 3)
	if !core.IsWindows() {
		files["/etc/go-sharding/config.yaml"] = false
		files["/etc/go-sharding/config.yml"] = false
	}
	dir, err := os.Getwd()
	if err == nil {
		files[filepath.Join(dir, "config.yaml")] = false
	} else {
		files["config.yaml"] = false
	}

	result := make([]string, len(files))
	i := 0
	for k, _ := range files {
		result[i] = k
		i++
	}
	return result
}
