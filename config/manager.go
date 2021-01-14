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
	"errors"
	"fmt"
	"github.com/XiaoMi/Gaea/config/internal"
	"github.com/XiaoMi/Gaea/core"
	"github.com/XiaoMi/Gaea/core/provider"
	"github.com/XiaoMi/Gaea/core/script"
	"go.uber.org/config"
	"strings"
	"sync"
)

const shardingTablesConfigPath = "rule.tables"
const dataSourcesConfigPath = "sources"
const defaultDataSourcesConfigPath = "default-source"

const noneStrategyName = "none"

type Manager interface {
	GetSettings() *core.Settings
}

type cnfManager struct {
	Provider string
	Source   Source
	current  *config.Value

	settings *core.Settings
	lock     sync.Mutex
}

func (mgr *cnfManager) GetSettings() *core.Settings {
	if mgr.settings == nil {
		mgr.lock.Lock()
		defer mgr.lock.Unlock()
		if mgr.settings == nil {
			s := core.NewSettings()
			err := mgr.populateSettings(s)
			if err != nil {
				logger.Error("populate config fault", core.LineSeparator, err)
			}
			mgr.settings = s
		}
	}
	return mgr.settings
}

func (mgr *cnfManager) populateSettings(settings *core.Settings) error {
	//解析物理数据库地址
	err := mgr.buildDataSource(settings)
	if err != nil {
		return err
	}

	tables := make(map[string]*internal.TableSettings)
	err = mgr.current.Get(shardingTablesConfigPath).Populate(tables)
	if err != nil {
		return err
	}

	shardingTables := make(map[string]*core.ShardingTable, len(tables))
	set := make(map[string]struct{})

	for n, t := range tables {
		if _, ok := set[n]; !ok {
			set[n] = struct{}{}
			if st, err := mgr.buildShardingTable(n, t); err != nil {
				return err
			} else {
				shardingTables[n] = st
			}
			continue
		}
		return errors.New(fmt.Sprint("duplex table config: ", n))
	}

	settings.ShardingRule = &core.ShardingRule{
		Tables: shardingTables,
	}

	return nil
}

func (mgr *cnfManager) buildDataSource(settings *core.Settings) error {
	err := mgr.current.Get(dataSourcesConfigPath).Populate(settings.DataSources)
	if err != nil {
		return err
	}

	for name, ds := range settings.DataSources {
		ds.Endpoint = strings.TrimSpace(ds.Endpoint)
		if strings.TrimSpace(ds.Endpoint) == "" {
			return fmt.Errorf("enpoint configuration missed in source '%s'", name)
		}

		ds.Username = strings.TrimSpace(ds.Username)
		if strings.TrimSpace(ds.Username) == "" {
			return fmt.Errorf("username configuration missed in source '%s'", name)
		}
	}

	source := mgr.current.Get(defaultDataSourcesConfigPath).String()
	if _, ok := settings.DataSources[source]; !ok {
		return fmt.Errorf("default source '%s' is not configured in sources", source)
	}

	settings.DefaultDataSource = source
	return nil
}

func (mgr *cnfManager) buildShardingTable(name string, settings *internal.TableSettings) (*core.ShardingTable, error) {
	var err error
	//验证配置格式
	if err = validateShardingTableConfig(name, settings); err != nil {
		return nil, err
	}

	sd := &core.ShardingTable{}
	sd.Name = name
	//加载分库策略
	sd.DatabaseStrategy, err = mgr.buildShardingStrategy(name, internal.DbStrategyProperty, settings.DbStrategy)
	if err != nil {
		return nil, err
	}
	//加载分表策略
	sd.TableStrategy, err = mgr.buildShardingStrategy(name, internal.TableStrategyProperty, settings.TableStrategy)
	if err != nil {
		return nil, err
	}

	resources, err := buildDbResource(settings.Resources)
	if err != nil {
		return nil, err
	}
	sd.Resources = resources
	return sd, nil
}

func (mgr *cnfManager) buildShardingStrategy(tableName string, propertyName string, settings interface{}) (core.ShardingStrategy, error) {
	name := getStrategyName(settings)

	p, ok := provider.DefaultRegistry().TryLoad(provider.StrategyFactory, name)
	if !ok {
		return nil, fmt.Errorf("strategy factory named '%s' is not registered", name)
	}
	f, ok := p.(core.ShardingStrategyFactory)
	if !ok {
		return nil, fmt.Errorf("provider named '%s' is not ShardingStrategyFactory", name)
	}
	props := make(map[string]string)
	if name != noneStrategyName {
		configPath := fmt.Sprint(shardingTablesConfigPath, ".", tableName, ".", propertyName, ".", name)
		if err := mgr.current.Get(configPath).Populate(props); err != nil {
			return nil, err
		}
	}
	if strategy, err := f.CreateStrategy(props); err != nil {
		return nil, errors.New(fmt.Sprint("create sharding strategy fault.", core.LineSeparator, err))
	} else {
		return strategy, nil
	}
}

func getStrategyName(strategy interface{}) string {
	if s, ok := strategy.(string); ok {
		return strings.TrimSpace(s)
	}
	m := strategy.(map[interface{}]interface{})
	for key, _ := range m {
		return fmt.Sprint(key)
	}
	return ""
}

func validateShardingTableConfig(tableName string, settings *internal.TableSettings) error {
	if err := validateStrategyConfig(tableName, internal.DbStrategyProperty, settings.DbStrategy); err != nil {
		return err
	}

	if err := validateStrategyConfig(tableName, internal.TableStrategyProperty, settings.TableStrategy); err != nil {
		return err
	}
	return nil
}

func validateStrategyConfig(tableName string, propertyName string, settings interface{}) error {

	if settings == nil {
		return fmt.Errorf("config property '%s' missed or null", propertyName)
	}

	if s, ok := settings.(string); ok {
		if s == noneStrategyName {
			return nil
		}
	}

	if m, ok := settings.(map[interface{}]interface{}); ok {
		if len(m) > 1 {
			return fmt.Errorf("more than one sharding strategy configured for table: %s", propertyName)
		}
		return nil
	}

	return fmt.Errorf("table '%s' has bad config value for %s ( value: %s )", tableName, propertyName, fmt.Sprint(settings))
}

func buildDbResource(dbNodesExpression string) (map[string][]string, error) {
	expr := strings.TrimSpace(dbNodesExpression)
	if expr == "" {
		return make(map[string][]string, 0), nil
	}

	inline, err := script.NewInlineExpression(expr)
	if err != nil {
		return nil, errors.New(fmt.Sprint("bad database node expression: ", expr, core.LineSeparator, err))
	}

	ns, err := inline.Flat()
	if err != nil {
		return nil, errors.New(fmt.Sprint("bad database node expression: ", expr, core.LineSeparator, err))
	}

	nodes := make(map[string][]string, len(ns))
	for _, name := range ns {
		schemaAndTable := strings.Split(name, ".")
		if len(schemaAndTable) != 2 {
			return nil, errors.New(fmt.Sprint("bad database node expression: ", expr, ", the separator (.) between schema and table name missed", core.LineSeparator, err))
		}
		schema := schemaAndTable[0]
		table := schemaAndTable[1]
		nodes[schema] = append(nodes[schema], table)
	}
	return nodes, nil
}
