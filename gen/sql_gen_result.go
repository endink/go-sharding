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

package gen

import (
	"fmt"
	"github.com/XiaoMi/Gaea/core"
)

type Usage byte

const (
	//使用生成的值
	UsageShard Usage = iota
	//使用原始值
	UsageRaw
)

func (u Usage) String() string {
	switch u {
	case UsageShard:
		return "Shard"
	case UsageRaw:
		return "Raw"
	}
	return "Known"
}

type ScatterCommand struct {
	DataSource string
	SqlCommand string
}

func (s *ScatterCommand) String() string {
	return fmt.Sprint(s.DataSource, ": ", s.SqlCommand)
}

type SqlGenResult struct {
	Commands []*ScatterCommand
	//指示用法，如果为 Raw 使用原始 SQL 执行分片数据库即可
	Usage Usage
}

func (r *SqlGenResult) String() string {
	sb := core.NewStringBuilder()
	sb.WriteLine("Usage: ", r.Usage.String())
	for _, command := range r.Commands {
		sb.WriteLine(command.String())
	}
	return sb.String()
}
