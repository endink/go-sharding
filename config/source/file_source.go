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

package source

import (
	cnf "go.uber.org/config"
	"io/ioutil"
)

const FileConfigProvider = "file"

// File source provider for configuration
type FileSource struct {
	value  cnf.Value
	loaded bool
}

func (c *FileSource) GetName() string {
	return FileConfigProvider
}

func (c *FileSource) Load(config cnf.Provider) (cnf.Value, error) {
	c.value = config.Get(cnf.Root)
	c.loaded = true

	return c.value, nil
}

// Close do nothing
func (c *FileSource) Close() error {
	return nil
}

// Read read file data
func (c *FileSource) Read(file string) ([]byte, error) {
	value, err := ioutil.ReadFile(file)
	if err != nil {
		return nil, err
	}
	return value, nil
}

// List list path, return slice of all files
func (c *FileSource) List(path string) ([]string, error) {
	r := make([]string, 0)
	files, err := ioutil.ReadDir(path)
	if err != nil {
		return r, err
	}

	for _, f := range files {
		r = append(r, f.Name())
	}

	return r, nil
}
