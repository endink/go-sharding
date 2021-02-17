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

package database

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"github.com/XiaoMi/Gaea/logging"
	"github.com/XiaoMi/Gaea/mysql"
	"github.com/XiaoMi/Gaea/mysql/types"
	"github.com/XiaoMi/Gaea/util"
	"sort"
)

var (
	ErrResourceExhausted = errors.New("resources exhausted")
	ErrHasAborted        = errors.New("operation was aborted")
)

// HandlePanic is part of the UpdateStream interface
func HandlePanic(err *error) {
	if x := recover(); x != nil {
		log.Errorf("Uncaught panic:\n%v\n%s", x, util.Stack(4))
		*err = fmt.Errorf("uncaught panic: %v", x)
	}
}

func RecoverError(logger logging.StandardLogger, ctx context.Context) {
	c := ctx
	if c == nil {
		c = context.TODO()
	}
	if x := recover(); x != nil {
		logger.Errorf("Uncaught panic:\n%v\n%s", x, util.Stack(4))
		DbStats.AddInternalErrors(c, "Panic", 1)
	}
}

func ensureContext(ctx context.Context) context.Context {
	c := ctx
	if c == nil {
		c = context.TODO()
	}
	return c
}

func queryAsString(sql string, bindVariables map[string]*types.BindVariable) string {
	buf := &bytes.Buffer{}
	_, _ = fmt.Fprintf(buf, "Sql: %q", sql)
	_, _ = fmt.Fprintf(buf, ", BindVars: {")
	var keys []string
	for key := range bindVariables {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	var valString string
	for _, key := range keys {
		valString = fmt.Sprintf("%v", bindVariables[key])
		_, _ = fmt.Fprintf(buf, "%s: %q", key, valString)
	}
	_, _ = fmt.Fprintf(buf, "}")
	return buf.String()
}

func NewSqlError(ctx context.Context, sql string, bindVariables map[string]*types.BindVariable, err error) error {
	if err == nil {
		return nil
	}

	callerID := CallerFromContext(ctx).From()

	// If TerseErrors is on, strip the error message returned by MySQL and only
	// keep the error number and sql state.
	// We assume that bind variable have PII, which are included in the MySQL
	// query and come back as part of the error message. Removing the MySQL
	// error helps us avoid leaking PII.
	// There are two exceptions:
	// 1. If no bind vars were specified, it's likely that the query was issued
	// by someone manually. So, we don't suppress the error.
	// 2. FAILED_PRECONDITION errors. These are caused when a failover is in progress.
	// If so, we don't want to suppress the error. This will allow VTGate to
	// detect and perform buffering during failovers.
	var message string
	sqlErr, ok := err.(*mysql.SQLError)
	if ok {
		sqlState := sqlErr.SQLState()
		errnum := sqlErr.Number()
		message = fmt.Sprintf("(errno %d) (sqlstate %s)%s: %s", errnum, sqlState, callerID, queryAsString(sql, bindVariables))
		err = errors.New(message)
	} else {
		message = fmt.Sprintf("%s:%v", callerID, err.Error())
		err = errors.New(message)
	}

	return err
}

func CopyMap(source map[string]string) map[string]string {
	if source == nil {
		return nil
	}
	dest := make(map[string]string, len(source))
	for k, v := range source {
		dest[k] = v
	}
	return dest
}

func CopyArray(source []string) []string {
	if source == nil {
		return nil
	}
	dest := make([]string, len(source))
	for i, v := range source {
		dest[i] = v
	}
	return dest
}
