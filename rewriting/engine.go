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

package rewriting

import (
	"github.com/XiaoMi/Gaea/explain"
	"github.com/XiaoMi/Gaea/mysql/types"
	"github.com/pingcap/errors"
	"github.com/pingcap/parser/ast"
	"github.com/scylladb/go-set/strset"
)

var _ explain.Rewriter = &Engine{}

type Engine struct {
	bindVariableRewriterList []bindVariableRewriter
}

func NewEngine() *Engine {
	return &Engine{}
}

func (engine *Engine) RewriteBindVariables(bindVars map[string]*types.BindVariable) (explain.RewriteBindVarsResult, error) {
	if len(engine.bindVariableRewriterList) == 0 {
		return explain.NoneRewriteBindVarsResult, nil
	}

	rewroteNames := strset.New()
	scatterNameSet := make(map[string]*strset.Set)

	isRewrote := false
	for _, rw := range engine.bindVariableRewriterList {
		r, err := rw.rewriteBindVars(bindVars)
		if err != nil {
			return nil, err
		}
		isRewrote = isRewrote || r.IsRewrote()
		if r.IsRewrote() {
			rewroteNames.Add(r.RewroteVariables()...)
			for n, v := range r.ScatterVariables() {
				set, ok := scatterNameSet[n]
				if !ok {
					set = strset.New()
					scatterNameSet[n] = set
				}
				set.Add(v...)
			}
		}
	}
	if !isRewrote {
		return explain.NoneRewriteBindVarsResult, nil
	}

	scatterNames := make(map[string][]string, len(scatterNameSet))
	for name, set := range scatterNameSet {
		scatterNames[name] = set.List()
	}

	return explain.ResultFromScatterVars(rewroteNames.List(), scatterNames), nil
}

func (engine *Engine) RewriteTable(tableName *ast.TableName, explainContext explain.Context) (explain.RewriteFormattedResult, error) {
	sd, ok, fe := explain.FindShardingTableByTable(tableName, explainContext, "")
	if fe != nil {
		return nil, fe
	}
	if ok {
		if writer, err := NewTableNameWriter(tableName); err == nil {
			return explain.ResultFromFormatter(writer, sd.Name, ""), nil
		} else {
			return nil, err
		}
	}
	return explain.NoneRewriteFormattedResult, nil
}

func (engine *Engine) RewriteField(columnName *ast.ColumnNameExpr, explainContext explain.Context) (explain.RewriteFormattedResult, error) {
	sd, ok, err := explain.FindShardingTableByColumn(columnName, explainContext, false)
	if err != nil {
		return nil, err
	}
	if ok {
		if writer, e := NewColumnNameWriter(columnName, sd.Name); e == nil {
			return explain.ResultFromFormatter(writer, sd.Name, explain.GetColumn(columnName.Name)), nil
		} else {
			return nil, e
		}
	}
	return explain.NoneRewriteFormattedResult, nil
}

func (engine *Engine) RewriteColumn(columnName *ast.ColumnNameExpr, explainContext explain.Context) (explain.RewriteFormattedResult, error) {
	sd, ok, err := explain.FindShardingTableByColumn(columnName, explainContext, true)
	if err != nil {
		return nil, err
	}
	if ok {
		if writer, e := NewColumnNameWriter(columnName, sd.Name); e == nil {
			return explain.ResultFromFormatter(writer, sd.Name, explain.GetColumn(columnName.Name)), nil
		} else {
			return nil, e
		}
	}
	return explain.NoneRewriteFormattedResult, nil
}

func (engine *Engine) RewritePatterIn(patternIn *ast.PatternInExpr, explainContext explain.Context) (explain.RewriteFormattedResult, error) {
	columnNameExpr, ok := patternIn.Expr.(*ast.ColumnNameExpr)
	if !ok {
		return nil, errors.New("pattern in statement required ColumnNameExpr")
	}
	sd, ok, err := explain.FindShardingTableByColumn(columnNameExpr, explainContext, true)
	if err != nil {
		return nil, err
	}
	if ok {
		if writer, e := NewPatternInWriter(patternIn, sd); e == nil {
			engine.bindVariableRewriterList = append(engine.bindVariableRewriterList, writer)
			return explain.ResultFromFormatter(writer, sd.Name, explain.GetColumn(columnNameExpr.Name)), nil
		} else {
			return nil, e
		}
	}
	return explain.NoneRewriteFormattedResult, nil
}

func (engine *Engine) RewriteBetween(between *ast.BetweenExpr, explainContext explain.Context) (explain.RewriteFormattedResult, error) {
	columnNameExpr, ok := between.Expr.(*ast.ColumnNameExpr)
	if !ok {
		return nil, errors.New("between and statement required ColumnNameExpr")
	}
	sd, ok, err := explain.FindShardingTableByColumn(columnNameExpr, explainContext, true)
	if err != nil {
		return nil, err
	}
	if ok {
		if writer, e := NewBetweenWriter(between, sd); e == nil {
			return explain.ResultFromFormatter(writer, sd.Name, explain.GetColumn(columnNameExpr.Name)), nil
		} else {
			return nil, e
		}
	}
	return explain.NoneRewriteFormattedResult, nil
}

func (engine *Engine) RewriteLimit(limit *ast.Limit, explainContext explain.Context) (explain.RewriteLimitResult, error) {
	lookup := explainContext.LimitLookup()
	if lookup.HasLimit() && lookup.HasOffset() {
		count, err := newLimit(explainContext)
		if err != nil {
			return nil, err
		}
		return explain.ResultFromLimit(count), nil
	}
	return explain.NoneRewriteLimitResult, nil
}
