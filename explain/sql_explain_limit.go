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

package explain

import (
	"github.com/pingcap/parser/ast"
	driver "github.com/pingcap/tidb/types/parser_driver"
)

func (s *SqlExplain) ExplainLimit(stmt *ast.SelectStmt, rewriter Rewriter) error {
	if stmt.Limit != nil {
		s.currentContext().LimitLookup().setLimit(stmt.Limit)
		result, err := rewriter.RewriteLimit(stmt.Limit, s.currentContext())
		if err != nil {
			return err
		}
		if result.IsRewrote() {
			stmt.Limit = result.GetNewNode()
		}
	}
	return nil
}

func NeedRewriteLimitOrCreateRewrite(stmt *ast.SelectStmt) (bool, int64, int64, *ast.Limit) {
	limit := stmt.Limit
	if limit == nil {
		return false, -1, -1, nil
	}

	count := limit.Count.(*driver.ValueExpr).GetInt64()

	if limit.Offset == nil {
		return false, 0, count, nil
	}

	offset := limit.Offset.(*driver.ValueExpr).GetInt64()

	if offset == 0 {
		return false, 0, count, nil
	}

	newCount := count + offset
	nv := &driver.ValueExpr{}
	nv.SetInt64(newCount)
	newLimit := &ast.Limit{
		Count: nv,
	}
	return true, offset, count, newLimit
}
