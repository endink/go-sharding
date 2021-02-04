/*
Copyright 2019 The Vitess Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package parser

import (
	"fmt"
	"github.com/XiaoMi/Gaea/mysql/types"
	"strings"
)

// ParsedQuery represents a parsed query where
// bind locations are precompued for fast substitutions.
type ParsedQuery struct {
	Query         string
	bindLocations []bindLocation
}

type bindLocation struct {
	argName        string
	offset, length int
}

// GenerateQuery generates a query by substituting the specified
// bindVariables. The extras parameter specifies special parameters
// that can perform custom encoding.
func (pq *ParsedQuery) GenerateQuery(bindVariables map[string]*types.BindVariable, extras map[string]Encodable) (string, error) {
	if len(pq.bindLocations) == 0 {
		return pq.Query, nil
	}
	var buf strings.Builder
	buf.Grow(len(pq.Query))
	if err := pq.append(&buf, bindVariables, extras); err != nil {
		return "", err
	}
	return buf.String(), nil
}

// Append appends the generated query to the provided buffer.
func (pq *ParsedQuery) append(buf *strings.Builder, bindVariables map[string]*types.BindVariable, extras map[string]Encodable) error {
	current := 0
	for _, loc := range pq.bindLocations {
		buf.WriteString(pq.Query[current:loc.offset])
		name := loc.argName
		if encodable, ok := extras[name[1:]]; ok {
			return encodable.EncodeSQL(buf)
		} else {
			supplied, _, err := fetchBindVar(name, bindVariables)
			if err != nil {
				return err
			}
			if err = encodeValue(buf, supplied); err != nil {
				return err
			}
		}
		current = loc.offset + loc.length
	}
	buf.WriteString(pq.Query[current:])
	return nil
}

// encodeValue encodes one bind variable value into the query.
func encodeValue(buf *strings.Builder, value *types.BindVariable) error {
	if value.Type != types.Tuple {
		// Since we already check for TUPLE, we don't expect an error.
		v, err := types.BindVariableToValue(value)
		if err != nil {
			return err
		}
		return v.EncodeSQL(buf)
	}

	// It's a TUPLE.
	if e := buf.WriteByte('('); e != nil {
		return e
	}
	for i, bv := range value.Values {
		if i != 0 {
			buf.WriteString(", ")
		}
		if err := bv.EncodeSQL(buf); err != nil {
			return err
		}
	}
	return buf.WriteByte(')')
}

// fetchBindVar resolves the bind variable by fetching it from bindVariables.
func fetchBindVar(name string, bindVariables map[string]*types.BindVariable) (val *types.BindVariable, isList bool, err error) {
	name = name[1:]
	if name[0] == ':' {
		name = name[1:]
		isList = true
	}
	supplied, ok := bindVariables[name]
	if !ok {
		return nil, false, fmt.Errorf("missing bind var %s", name)
	}

	if isList {
		if supplied.Type != types.Tuple {
			return nil, false, fmt.Errorf("unexpected list arg type (%v) for key %s", supplied.Type, name)
		}
		if len(supplied.Values) == 0 {
			return nil, false, fmt.Errorf("empty list supplied for %s", name)
		}
		return supplied, true, nil
	}

	if supplied.Type == types.Tuple {
		return nil, false, fmt.Errorf("unexpected arg type (TUPLE) for non-list key %s", name)
	}

	return supplied, false, nil
}

// ParseAndBind is a one step sweep that binds variables to an input query, in order of placeholders.
// It is useful when one doesn't have any parser-variables, just bind variables.
// Example:
//   query, err := ParseAndBind("select * from tbl where name=%a", sqltypes.StringBindVariable("it's me"))
func ParseAndBind(in string, binds ...*types.BindVariable) (query string, err error) {
	vars := make([]interface{}, len(binds))
	for i := range binds {
		vars[i] = fmt.Sprintf(":var%d", i)
	}
	parsed := BuildParsedQuery(in, vars...)

	bindVars := map[string]*types.BindVariable{}
	for i := range binds {
		bindVars[fmt.Sprintf("var%d", i)] = binds[i]
	}
	return parsed.GenerateQuery(bindVars, nil)
}
