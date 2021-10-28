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

package parser

import (
	"fmt"
	"github.com/pingcap/parser/ast"
	pf "github.com/pingcap/parser/format"
	"strings"
)

// TrackedBuffer is used to rebuild a query from the ast.
// bindLocations keeps track of locations in the buffer that
// use bind variables for efficient future substitutions.
// nodeFormatter is the formatting function the buffer will
// use to format a node. By default(nil), it's FormatNode.
// But you can supply a different formatting function if you
// want to generate a query that's different from the default.
type TrackedBuffer struct {
	*strings.Builder
	bindLocations []bindLocation
	flag          pf.RestoreFlags
}

// NewTrackedBuffer creates a new TrackedBuffer.
func NewTrackedBuffer() *TrackedBuffer {
	return NewTrackedBufferWithFlag(EscapeRestoreFlags)
}

// NewTrackedBufferWithFlag creates a new TrackedBuffer.
func NewTrackedBufferWithFlag(flag pf.RestoreFlags) *TrackedBuffer {
	return &TrackedBuffer{
		Builder: new(strings.Builder),
		flag:    flag,
	}
}

// astPrintf is for internal use by the ast structs
func (buf *TrackedBuffer) astPrintf(format string, values ...interface{}) {
	var argIndex int
	end := len(format)
	fieldnum := 0
	for i := 0; i < end; {
		lasti := i
		for i < end && format[i] != '%' && format[i] != ':' && format[i] != '?' {
			i++
		}

		if i > lasti {
			_, _ = buf.WriteString(format[lasti:i])
		}

		if i >= end {
			break
		}

		switch format[i] {
		case '?':
			buf.WriteArg(fmt.Sprintf(":p%d", argIndex))
			argIndex++
			i++
		case '%':
			offset := buf.procTemplate(format, values, i, fieldnum)
			fieldnum++
			i += offset
		case ':':
			offset := buf.procArg(format, i, end)
			i += offset
		}
	}
}

func (buf *TrackedBuffer) procArg(format string, offset int, formatLen int) int {
	if offset+2 < formatLen && format[offset+1] == ':' && format[offset+2] == ':' { //转义字符
		_ = buf.WriteByte(':')
		return 3
	}
	length := 1
	index := offset + 1
	for index < formatLen && isArgName(rune(format[index]), length == 1) {
		index++
		length++
	}
	if index > offset+1 {
		buf.WriteArg(format[offset:index])
	} else { //没有找到合法的参数
		_, _ = buf.WriteString(format[offset:index])
	}
	return length
}

func isArgName(c rune, includeColon bool) bool {
	return (c >= '0' && c <= '9') || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || c == '_' || c == '-' || (includeColon && c == ':')
}

func (buf *TrackedBuffer) procTemplate(format string, values []interface{}, offset int, fieldnum int) int {
	token := format[offset+1] // skip '%'
	switch token {
	case 'c':
		switch v := values[fieldnum].(type) {
		case byte:
			_ = buf.WriteByte(v)
		case rune:
			_, _ = buf.WriteRune(v)
		default:
			panic(fmt.Sprintf("unexpected TrackedBuffer type %T", v))
		}
	case 's':
		switch v := values[fieldnum].(type) {
		case []byte:
			_, _ = buf.Write(v)
		case string:
			_, _ = buf.WriteString(v)
		default:
			panic(fmt.Sprintf("unexpected TrackedBuffer type %T", v))
		}
	case 'a':
		switch v := values[fieldnum].(type) {
		case string:
			buf.WriteArg(v)
		default:
			panic(fmt.Sprint("'%a' must match the string type, actual type is: ", fmt.Sprintf("%T", v)))
		}
	case 'v':
		value := values[fieldnum]
		if n, ok := value.(ast.Node); ok {
			rsCtx := pf.NewRestoreCtx(buf.flag, buf)
			if err := n.Restore(rsCtx); err != nil {
				panic(err)
			}
		} else {
			content := fmt.Sprintf("%v", value)
			if _, err := buf.WriteString(content); err != nil {
				panic(err)
			}
		}
	default:
		panic("unexpected")
	}
	return 2
}

func (buf *TrackedBuffer) printIf(condition bool, text string) {
	if condition {
		_, _ = buf.WriteString(text)
	}
}

// WriteArg writes a value argument into the buffer along with
// tracking information for future substitutions. arg must contain
// the ":" or "::"
func (buf *TrackedBuffer) WriteArg(argName string) {
	if !strings.HasPrefix(argName, ":") {
		panic("The argument name must begin with a colon (:)")
	}
	buf.bindLocations = append(buf.bindLocations, bindLocation{
		argName: argName,
		offset:  buf.Len(),
		length:  1,
	})
	_, _ = buf.WriteRune('?')
}

// ParsedQuery returns a ParsedQuery that contains bind
// locations for easy substitution.
func (buf *TrackedBuffer) ParsedQuery() *ParsedQuery {
	return &ParsedQuery{Query: buf.String(), bindLocations: buf.bindLocations}
}

// HasBindVars returns true if the parsed query uses bind vars.
func (buf *TrackedBuffer) HasBindVars() bool {
	return len(buf.bindLocations) != 0
}
