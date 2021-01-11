// Copyright 2019 The Gaea Authors. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package source

import (
	"errors"
	"github.com/XiaoMi/Gaea/config"
	"github.com/XiaoMi/Gaea/logging"
	cnf "go.uber.org/config"
	"io/ioutil"
	"os"
	"strings"
	"time"
)

const (
	defaultFilePath = "./etc/file"
)

// File source provider for configuration
type fileSource struct {
}

func (c *fileSource) GetName() string {
	return config.FileProvider
}

func (c *fileSource) OnLoad(config cnf.Provider) error {
	config.Get(cnf.Root)
}

// New constructor of etcdSource
func NewFileSource() (config.Source, error) {

}

// Close do nothing
func (c *fileSource) Close() error {
	return nil
}

// Create do nothing
func (c *fileSource) Create(path string, data []byte) error {
	return nil
}

// Update do nothing
func (c *fileSource) Update(path string, data []byte) error {
	return nil
}

// UpdateWithTTL update path with data and ttl
func (c *fileSource) UpdateWithTTL(path string, data []byte, ttl time.Duration) error {
	return nil
}

// Delete delete path
func (c *fileSource) Delete(path string) error {
	return nil
}

// Read read file data
func (c *fileSource) Read(file string) ([]byte, error) {
	value, err := ioutil.ReadFile(file)
	if err != nil {
		return nil, err
	}
	return value, nil
}

// List list path, return slice of all files
func (c *fileSource) List(path string) ([]string, error) {
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
