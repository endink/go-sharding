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

package plan

import (
	"fmt"
	"github.com/XiaoMi/Gaea/core"
	"github.com/XiaoMi/Gaea/routing"
	"github.com/pingcap/parser/ast"
)

// TableAliasStmtInfo 使用到表别名, 且依赖表别名做路由计算的StmtNode, 目前包括UPDATE, SELECT
// INSERT也可以使用表别名, 但是由于只存在一个表, 可以直接去掉, 因此不需要.
type TableAliasStmtInfo struct {
	*StmtInfo
	tableAlias  map[string]string // key = table alias, value = table
	routeResult RouteResult
}

// NewTableAliasStmtInfo means table alias StmtInfo
func NewTableAliasStmtInfo(sql string, ctx *routing.Context) *TableAliasStmtInfo {
	return &TableAliasStmtInfo{
		StmtInfo:    NewStmtInfo(sql, ctx),
		tableAlias:  make(map[string]string),
		routeResult: NewRouteResult(),
	}
}

func (t *TableAliasStmtInfo) GetRouteResult() RouteResult {
	return t.routeResult
}

// NeedCreateTableNameDecorator check if TableName with alias needs decorate
// SELECT语句可能带有表别名, 需要记录表别名
func (t *TableAliasStmtInfo) AddTableWithAlias(table *ast.TableName, alias string) (*core.ShardingTable, bool, error) {
	st, ok, err := t.AddTable(table)
	if err != nil {
		return nil, false, err
	}

	if alias != "" {
		if err := t.setTableAlias(table.Name.L, alias); err != nil {
			return nil, false, fmt.Errorf("set table alias error: %v", err)
		}
	}

	return st, ok, nil
}

func (t *TableAliasStmtInfo) setTableAlias(table, alias string) error {
	// if not set, set without check
	originTable, ok := t.tableAlias[alias]
	if !ok {
		t.tableAlias[alias] = table
		return nil
	}

	if originTable != table {
		return fmt.Errorf("table alias is set but not match, table: %s, originTable: %s", table, originTable)
	}

	// already set, return
	return nil
}

func (t *TableAliasStmtInfo) getAliasTable(alias string) (string, bool) {
	table, ok := t.tableAlias[alias]
	return table, ok
}

// NeedCreatePatternInExprDecorator check if PatternInExpr needs decoration
func (t *TableAliasStmtInfo) NeedCreatePatternInExprDecorator(n *ast.PatternInExpr) (*ShardingTableRecord, bool, error) {
	if n.Sel != nil {
		return nil, false, fmt.Errorf("TableName does not support Sel in sharding")
	}

	// 如果不是ColumnNameExpr, 则不做任何路由计算和装饰, 直接返回
	columnNameExpr, ok := n.Expr.(*ast.ColumnNameExpr)
	if !ok {
		return nil, false, nil
	}

	rule, need, err := t.NeedCreateColumnNameExprDecoratorInCondition(columnNameExpr)
	if err != nil {
		return nil, false, fmt.Errorf("check ColumnName error: %v", err)
	}

	return rule, need, err
}

// NeedCreateColumnNameExprDecoratorInField check if ColumnNameExpr in field needs decoration
// 用于Field列表中判断列名是否需要装饰
// 如果db名和表名都不存在, 则不需要装饰
func (t *TableAliasStmtInfo) NeedCreateColumnNameExprDecoratorInField(n *ast.ColumnNameExpr) (*ShardingTableRecord, bool, error) {
	db, table, _ := getColumnInfoFromColumnName(n.Name)
	if db == "" && table == "" {
		return nil, false, nil
	}
	return t.needCreateColumnNameDecorator(n.Name)
}

// NeedCreateColumnNameExprDecoratorInCondition check if ColumnNameExpr in condition needs decoration
// 用于JOIN ON条件或WHERE条件中判断列名是否需要装饰
// 与上面的区别在于, 当只存在列名, 不存在db名和表名时, 还会根据列名去查找对应的条件 (因为装饰之后需要在比较条件中计算路由)
func (t *TableAliasStmtInfo) NeedCreateColumnNameExprDecoratorInCondition(n *ast.ColumnNameExpr) (*ShardingTableRecord, bool, error) {
	return t.needCreateColumnNameDecorator(n.Name)
}

// 是否需要装饰ColumnName, 需要则返回ture
// 在CreateColumnNameDecorator之前调用, 用来检查
// 返回结果bool表示是否需要创建装饰器
func (t *TableAliasStmtInfo) needCreateColumnNameDecorator(n *ast.ColumnName) (*ShardingTableRecord, bool, error) {
	db, table, column := getColumnInfoFromColumnName(n)

	return t.GetSettedRuleFromColumnInfo(db, table, column)
}

// GetSettedRuleFromColumnInfo 用于WHERE条件或JOIN ON条件中, 查找列名对应的路由规则
func (t *TableAliasStmtInfo) GetSettedRuleFromColumnInfo(db, table, column string) (*ShardingTableRecord, bool, error) {
	if db == "" && table == "" {
		return t.getSettedRuleByColumnName(column)
	}

	r, err := t.getSettedRuleFromTable(db, table)
	return r, err == nil, err
}

// 用于WHERE条件或JOIN ON条件中, 只存在列名时, 查找对应的路由规则
func (t *TableAliasStmtInfo) getSettedRuleByColumnName(column string) (*ShardingTableRecord, bool, error) {
	ret := &ShardingTableRecord{}
	for _, r := range t.tableRules {
		if r.HasTableShardingColumn(column) {
			if ret.Sharding == nil {
				ret.Sharding = r
			} else {
				//多次出现分片列
				return nil, false, fmt.Errorf("column %s is ambiguous for sharding", column)
			}
		}
	}
	if ret.Sharding == nil {
		return nil, false, nil
	}
	return ret, true, nil

}

// 获取FROM TABLE列表中的表数据
// 用于FieldList和Where条件中列名的判断
func (t *TableAliasStmtInfo) getSettedRuleFromTable(db, table string) (*ShardingTableRecord, error) {
	err := t.validateDatabase(db)
	if err != nil {
		return nil, err
	}

	aliasTable := &ShardingTableRecord{
		IsAlias: false,
	}
	if rule, ok := t.tableRules[table]; ok {
		aliasTable.Sharding = rule
		return aliasTable, nil
	}

	if originTable, ok := t.getAliasTable(table); ok {
		if rule, ok := t.tableRules[originTable]; ok {
			aliasTable.Sharding = rule
			aliasTable.IsAlias = true
			return aliasTable, nil
		}
	}

	return nil, fmt.Errorf("sharding strategy not found")
}

// RecordSubqueryTableAlias 记录表名位置的子查询的别名, 便于后续处理
// 返回已存在Rule的第一个 (任意一个即可)
// 限制: 子查询中的表对应的路由规则必须与外层查询相关联, 或者为全局表
func (t *TableAliasStmtInfo) RecordSubqueryTableAlias(alias string) (*core.ShardingTable, error) {
	if alias == "" {
		return nil, fmt.Errorf("subquery table alias is nil")
	}

	if len(t.tableRules) == 0 {
		return nil, fmt.Errorf("no explicit table exist except subquery")
	}

	table := "sharding_" + alias
	if err := t.setTableAlias(table, alias); err != nil {
		return nil, fmt.Errorf("set subquery table alias error: %v", err)
	}

	var rule *core.ShardingTable
	for _, r := range t.tableRules {
		rule = r
		break
	}

	t.tableRules[table] = rule
	return rule, nil
}
