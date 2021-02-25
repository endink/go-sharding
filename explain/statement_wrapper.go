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
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/format"
	"github.com/pingcap/parser/types"
	"io"
)

var _ ast.ExprNode = &statementWrapper{}

type statementWrapper struct {
	formatter StatementFormatter
}

func wrapFormatter(formatter StatementFormatter) ast.ExprNode {
	return &statementWrapper{formatter: formatter}
}

func (s *statementWrapper) Restore(ctx *format.RestoreCtx) error {
	stmtCtx, err := unwrapWriter(ctx.In)
	if err != nil {
		return err
	}
	return s.formatter.Format(stmtCtx)
}

func (s *statementWrapper) SetText(text string) {

}

func (s *statementWrapper) Text() string {
	return s.formatter.Text()
}

func (s *statementWrapper) Accept(v ast.Visitor) (node ast.Node, ok bool) {
	return s, true
}

func (s *statementWrapper) SetType(tp *types.FieldType) {

}

func (s *statementWrapper) GetType() *types.FieldType {
	return s.formatter.GetType()
}

func (s *statementWrapper) SetFlag(flag uint64) {

}

func (s *statementWrapper) GetFlag() uint64 {
	return s.formatter.GetFlag()
}

func (s *statementWrapper) Format(w io.Writer) {

}
