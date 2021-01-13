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

type Manager interface {
	GetSettings() *Settings
}

type cnfManager struct {
	Provider string
	Source   Source
	current  *config.Value

	settings *Settings
	lock     sync.Mutex
}

func (mgr *cnfManager) GetSettings() *Settings {
	if mgr.settings == nil {
		mgr.lock.Lock()
		defer mgr.lock.Unlock()
		if mgr.settings == nil {
			s := NewSettings()
			err := mgr.populateSettings(s)
			if err != nil {
				logger.Error("populate config fault", core.LineSeparator, err)
			}
			mgr.settings = s
		}
	}
	return mgr.settings
}

func (mgr *cnfManager) populateSettings(settings *Settings) error {
	//解析物理数据库地址
	err := mgr.current.Get(dataSourcesConfigPath).Populate(settings.DataSources)
	if err != nil {
		return err
	}

	tables := make(map[string]internal.TableSettings)
	err = mgr.current.Get(shardingTablesConfigPath).Populate(tables)
	if err != nil {
		return err
	}

	shardingTables := make(map[string]*core.ShardingTable, len(tables))
	for n, t := range tables {
		if st, err := mgr.buildShardingTable(n, t); err != nil {
			return err
		} else {
			shardingTables[n] = st
		}
	}

	settings.ShardingRule = &ShardingRule{
		Tables: shardingTables,
	}

	return nil
}

func (mgr *cnfManager) buildShardingTable(name string, settings internal.TableSettings) (*core.ShardingTable, error) {
	var err error
	//验证配置格式
	if err = validateTableSettings(name, settings); err != nil {
		return nil, err
	}

	sd := &core.ShardingTable{}
	sd.Name = name
	//加载分库策略
	sn, props := getFirstKeyValue(settings.DbStrategy)
	sd.DatabaseStrategy, err = mgr.buildShardingStrategy(sn, props)
	if err != nil {
		return nil, err
	}
	//加载分表策略
	sn, props = getFirstKeyValue(settings.TableStrategy)
	sd.TableStrategy, err = mgr.buildShardingStrategy(sn, props)
	if err != nil {
		return nil, err
	}

	nodes, err := buildDbNodes(settings.Resources)
	if err != nil {
		return nil, err
	}
	sd.Resource = nodes
	return sd, nil
}

func (mgr *cnfManager) buildShardingStrategy(name string, props map[string]string) (core.ShardingStrategy, error) {
	p, ok := provider.DefaultRegistry().TryLoad(provider.StrategyFactory, name)
	if !ok {
		return nil, fmt.Errorf("strategy factory named '%s' is not registered", name)
	}
	f, ok := p.(core.ShardingStrategyFactory)
	if !ok {
		return nil, fmt.Errorf("provider named '%s' is not ShardingStrategyFactory", name)
	}
	if strategy, err := f.CreateStrategy(props); err != nil {
		return nil, errors.New(fmt.Sprint("create sharding strategy fault.", core.LineSeparator, err))
	} else {
		return strategy, nil
	}
}

func getFirstKeyValue(strategy map[string]map[string]string) (string, map[string]string) {
	for name, value := range strategy {
		return name, value
	}
	panic(errors.New("map is null"))
}

func validateTableSettings(tableName string, settings internal.TableSettings) error {

	if settings.DbStrategy == nil || len(settings.DbStrategy) == 0 {
		return ErrDbStrategyConfigMissed
	}

	if settings.TableStrategy == nil || len(settings.TableStrategy) == 0 {
		return ErrTableStrategyConfigMissed
	}

	if len(settings.DbStrategy) > 1 {
		return fmt.Errorf("table '%s' configured more than 1 database sharding strategy ( config property: %s )", tableName, internal.DbStrategyProperty)
	}

	if len(settings.TableStrategy) > 1 {
		return fmt.Errorf("table '%s' configured more than 1 table sharding strategy ( config property: %s )", tableName, internal.TableStrategyProperty)
	}
	return nil
}

func buildDbNodes(dbNodesExpression string) ([]*core.DatabaseResource, error) {
	expr := strings.TrimSpace(dbNodesExpression)
	if expr == "" {
		return nil, ErrDataNodeConfigMissed
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
	dbNodes := make([]*core.DatabaseResource, len(nodes))
	index := 0
	for name, tables := range nodes {
		dbNode := &core.DatabaseResource{
			Name:   name,
			Tables: core.DistinctSlice(tables),
		}
		dbNodes[index] = dbNode
		index++
	}
	return dbNodes, nil
}
