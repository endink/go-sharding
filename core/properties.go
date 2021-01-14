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

package core

import "go.uber.org/config"

type Properties interface {
	GetValues() map[string]string
	PopulateValue(instance interface{}) error
}

var EmptyProperties Properties = &emptyProperties{}

func NewProperties(value *config.Value) (Properties, error) {
	values := make(map[string]string)
	if err := value.Populate(values); err != nil {
		return nil, err
	}
	return &properties{
		values:   values,
		rawValue: value,
	}, nil
}

type properties struct {
	values   map[string]string
	rawValue *config.Value
}

func (props *properties) GetValues() map[string]string {
	return props.values
}

func (props *properties) PopulateValue(instance interface{}) error {
	return props.rawValue.Populate(instance)
}

type emptyProperties struct {
}

func (props *emptyProperties) GetValues() map[string]string {
	return make(map[string]string, 0)
}

func (props *emptyProperties) PopulateValue(instance interface{}) error {
	return nil
}
