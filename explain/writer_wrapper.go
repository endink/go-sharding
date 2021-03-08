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
	"github.com/XiaoMi/Gaea/core"
	"github.com/XiaoMi/Gaea/util"
	"github.com/pingcap/errors"
	"github.com/pingcap/parser/format"
	"io"
)

func wrapWriter(write io.Writer, runtime Runtime, ctx Context) io.Writer {
	rstCtx := &format.RestoreCtx{
		Flags: runtime.GetRestoreFlags(),
		In:    write,
	}
	return NewStatementContext(ctx, rstCtx, runtime)
}

func unwrapWriter(writer io.Writer) (StatementContext, error) {
	ctx, ok := writer.(StatementContext)
	if !ok {
		sb := core.NewStringBuilder()
		sb.WriteLine("writer is not an 'writerWrapper' for restoring")
		sb.WriteLine("writer type: %T", writer)
		sb.WriteLine("caller:")
		sb.WriteLine(util.Stack(4))
		return nil, errors.New(sb.String())
	}
	return ctx, nil
}
