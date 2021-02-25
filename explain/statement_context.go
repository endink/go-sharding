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

import (
	"github.com/pingcap/parser/format"
	"io"
)

var _ StatementContext = &statementContext{}

type StatementContext interface {
	io.Writer
	WriteKeyWord(keyWord string)
	WriteString(str string)
	WriteName(name string)
	WritePlain(plainText string)
	WritePlainf(format string, a ...interface{})

	GetContext() Context
	GetFlags() format.RestoreFlags
	GetRuntime() Runtime
	GetRestoreCtx() *format.RestoreCtx
}

type statementContext struct {
	*format.RestoreCtx
	ctx   Context
	rt    Runtime
	flags format.RestoreFlags
}

func (s *statementContext) GetRestoreCtx() *format.RestoreCtx {
	return s.RestoreCtx
}

func NewStatementContext(ctx Context, restoreCtx *format.RestoreCtx, rt Runtime) *statementContext {
	return &statementContext{
		RestoreCtx: restoreCtx,
		ctx:        ctx,
		rt:         rt,
	}
}

func (s *statementContext) Write(p []byte) (n int, err error) {
	return s.In.Write(p)
}

func (s *statementContext) GetContext() Context {
	return s.ctx
}

func (s *statementContext) GetFlags() format.RestoreFlags {
	return s.flags
}

func (s *statementContext) GetRuntime() Runtime {
	return s.rt
}
