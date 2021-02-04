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
	"context"
	"fmt"
	"github.com/XiaoMi/Gaea/logging"
	"github.com/XiaoMi/Gaea/util"
	"strconv"
	"strings"
)

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

// TransactionID extracts the original transaction ID from the dtid.
func TransactionID(dtid string) (int64, error) {
	splits := strings.Split(dtid, ":")
	if len(splits) != 3 {
		return 0, fmt.Errorf("invalid parts in dtid: %s", dtid)
	}
	txid, err := strconv.ParseInt(splits[2], 10, 0)
	if err != nil {
		return 0, fmt.Errorf("invalid transaction id in dtid: %s", dtid)
	}
	return txid, nil
}
