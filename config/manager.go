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
	"go.uber.org/config"
	"sync"
)

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
	if err := mgr.current.Populate(settings); err != nil {
		return err
	}

	return nil
}
