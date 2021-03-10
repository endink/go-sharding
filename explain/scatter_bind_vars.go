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

package explain

import "github.com/XiaoMi/Gaea/mysql/types"

type ScatterBindVars struct {
	IsScattered bool
	BindVars    []*ScatterVars
}

func (vars *ScatterBindVars) Get(dataSource string) (map[string]*types.BindVariable, bool) {
	if vars.IsScattered {
		for _, vars := range vars.BindVars {
			if vars.DataSource == dataSource {
				return vars.Vars, true
			}
		}
	}
	return nil, false
}

type ScatterVars struct {
	DataSource string
	Vars       map[string]*types.BindVariable
}
