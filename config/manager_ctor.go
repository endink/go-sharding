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
	"fmt"
	"github.com/endink/go-sharding/core"
	"github.com/endink/go-sharding/core/provider"
	"go.uber.org/config"
	"strings"
)

func NewManager() (Manager, error) {
	var sources []config.YAMLOption

	files := DefaultConfigFileLocations()

	var sb = core.NewStringBuilder()
	sb.WriteLine()
	sb.WriteLine("Search configuration locations:")
	for _, f := range files {
		if core.FileExists(f) {
			sources = append(sources, config.File(f))
			sb.WriteLine("[Found]:", f)
		} else {
			sb.WriteLine("[Not Found]:", f)
		}
	}

	var yaml *config.YAML
	if len(sources) > 0 {
		var err error
		sources = append(sources, config.Permissive())
		yaml, err = config.NewYAML(sources...)
		if err != nil {
			logger.Warn("Build boot config file fault.", core.LineSeparator, err)
		}
	}

	return NewManagerFromYAML(yaml)
}

func NewManagerFromYAML(yaml *config.YAML) (Manager, error) {
	bootCnf := &cnfManager{
		Provider: "file",
	}
	err := yaml.Get("config").Populate(bootCnf)
	if err != nil {
		return nil, err
	}
	bootCnf.Provider = core.IfBlankAndTrim(bootCnf.Provider, "file")

	var found = false
	if p, ok := provider.DefaultRegistry().TryLoad(provider.ConfigSource, bootCnf.Provider); ok {
		if s, ok := p.(Source); ok {
			bootCnf.Source = s
			found = true
		}
	}

	if !found {
		return nil, fmt.Errorf("config source provider named '%s' was not found", bootCnf.Provider)
	}

	if v, err := bootCnf.Source.Load(yaml); err != nil {
		return nil, err
	} else {
		bootCnf.current = &v
	}
	if err = bootCnf.initialize(); err != nil {
		return nil, err
	}

	return bootCnf, nil
}

func NewManagerFromString(ymlContent string) (Manager, error) {
	r := strings.NewReader(ymlContent)
	opt := config.Source(r)
	permissive := config.Permissive()
	yml, err := config.NewYAML(opt, permissive)
	if err != nil {
		return nil, err
	}

	return NewManagerFromYAML(yml)
}
