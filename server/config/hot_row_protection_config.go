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

package config

// HotRowProtectionConfig contains the config for hot row protection.
type HotRowProtectionConfig struct {
	// Mode can be disable, dryRun or enable. Default is disable.
	Mode               string `json:"mode,omitempty"`
	MaxQueueSize       int    `json:"maxQueueSize,omitempty"`
	MaxGlobalQueueSize int    `json:"maxGlobalQueueSize,omitempty"`
	MaxConcurrency     int    `json:"maxConcurrency,omitempty"`
}
