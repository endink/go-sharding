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
	"github.com/XiaoMi/Gaea/core"
	"github.com/XiaoMi/Gaea/mysql/types"
)

type Usage byte

const (
	//使用生成的值
	UsageShard Usage = iota
	//使用原始值
	UsageRaw
	//不可能达成的条件，例如条件冲突
	UsageImpossible
)

func (u Usage) String() string {
	switch u {
	case UsageShard:
		return "Shard"
	case UsageRaw:
		return "Raw"
	case UsageImpossible:
		return "Impossible"
	}
	return "Known"
}

type ScatterCommand struct {
	DataSource string
	SqlCommand string
	Vars       []*types.BindVariable
}

func (s *ScatterCommand) Equals(v interface{}) bool {
	if v == nil {
		return false
	}
	switch cmd := v.(type) {
	case *ScatterCommand:
		return s.DataSource == cmd.DataSource && s.SqlCommand == cmd.SqlCommand && types.BindVarsArrayEquals(s.Vars, cmd.Vars)
	default:
		return false
	}
}

func (s *ScatterCommand) String() string {
	sb := core.NewStringBuilder()
	sb.Write(s.DataSource, ": ", s.SqlCommand)
	vLen := len(s.Vars)
	if vLen > 0 {
		sb.WriteLine()
		sb.Write(vLen, " vars: ")
		for n, v := range s.Vars {
			gv, e := v.GetGolangValue()
			if e == nil {
				sb.Write("p", n, "=", gv)
			} else {
				sb.Write("p", n, "=", v)
			}
			if (n + 1) < vLen {
				sb.Write(", ")
			}
		}
	}
	return sb.String()
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
