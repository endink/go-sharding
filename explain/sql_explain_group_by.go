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
	"fmt"
	"github.com/pingcap/parser/ast"
)

func (s *SqlExplain) ExplainGroupBy(stmt *ast.SelectStmt, rewriter Rewriter) error {

	if stmt.GroupBy == nil {
		return nil
	}

	//groupByFields, err := s.rewriteByItems(stmt.GroupBy.Items, rewriter)
	//if err != nil {
	//	return fmt.Errorf("get group by fields error: %v", err)
	//}
	groupByLookup := s.CurrentContext().GroupByLookup()
	return s.attachByItems(stmt, stmt.GroupBy.Items, groupByLookup, rewriter)
}

func (s *SqlExplain) attachByItems(stmt *ast.SelectStmt, byItems []*ast.ByItem, lookup FieldLookup, rewriter Rewriter) error {
	index := len(stmt.Fields.Fields)
	for _, item := range byItems {
		if columnExpr, ok := item.Expr.(*ast.ColumnNameExpr); ok {
			return fmt.Errorf("ByItem.Expr is not a ColumnNameExpr")
		} else {
			fieldName := GetColumn(columnExpr.Name)
			if s.CurrentContext().FieldLookup().FindByName(fieldName) < 0 {
				field, e := s.newFieldFromByItem(item, rewriter)
				if e != nil {
					return e
				}
				//附加到查询结果列
				stmt.Fields.Fields = append(stmt.Fields.Fields, field)
				if _, isColumnExpr := field.Expr.(*ast.ColumnNameExpr); isColumnExpr {
					e = s.CurrentContext().FieldLookup().addField(index, field)
					if e != nil {
						return e
					}
				}

				e = lookup.addField(index, field)
				if e != nil {
					return e
				}
				index++
			}
		}
	}
	return nil
}

//func (s *SqlExplain) rewriteByItems(items []*ast.ByItem, rewriter Rewriter) ([]*ast.SelectField, error) {
//	var ret []*ast.SelectField
//	for _, item := range items {
//		selectField, err := s.newFieldFromByItem(item, rewriter)
//		if err != nil {
//			return nil, err
//		}
//		ret = append(ret, selectField)
//	}
//	return ret, nil
//}

func (s *SqlExplain) newFieldFromByItem(item *ast.ByItem, rewriter Rewriter) (*ast.SelectField, error) {
	// 特殊处理DATABASE()这种情况
	if _, ok := item.Expr.(*ast.FuncCallExpr); ok {
		//if funcExpr.FnName.L == "database" {
		//	ret := &ast.SelectField{
		//		Expr: item.Expr,
		//	}
		//	return ret, nil
		//}
		return nil, fmt.Errorf("unsupport group by use function")
	}

	columnExpr, ok := item.Expr.(*ast.ColumnNameExpr)
	if !ok {
		return nil, fmt.Errorf("ByItem.Expr is not a ColumnNameExpr")
	}

	result, err := rewriter.RewriteField(columnExpr, s.CurrentContext())
	if err != nil {
		return nil, err
	}

	if result.IsRewrote() {
		item.Expr = result.GetNewNode()
	}

	ret := &ast.SelectField{
		Expr: item.Expr,
	}
	return ret, nil
}
