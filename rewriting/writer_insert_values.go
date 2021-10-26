package rewriting

import (
	"errors"
	"fmt"
	"github.com/endink/go-sharding/core"
	"github.com/endink/go-sharding/explain"
	"github.com/endink/go-sharding/mysql/types"
	"github.com/pingcap/parser/ast"
	types2 "github.com/pingcap/parser/types"
	driver "github.com/pingcap/tidb/types/parser_driver"
)

var _ explain.StatementFormatter = &InsertValuesWriter{}
var _ preparer = &InsertValuesWriter{}

type InsertValuesWriter struct {
	insertStmt *ast.InsertStmt
	shardingTable    *core.ShardingTable
	shardingColName  string
	shardingColIndex int
	values           [][]ast.ValueExpr
	paramIndexes []int //所有的参数变量的索引，用于后续 diff 移除多余的参数
	valueMappings map[string][][]ast.ValueExpr //key: tableName, value: insert values
}

func NewInsertValuesWriter(
	insertStmt *ast.InsertStmt,
	shardingColName string,
	shardingColIndex int,
	values [][]ast.ExprNode,
	shardingTable *core.ShardingTable) (*InsertValuesWriter, error) {

	valueList := make([][]ast.ValueExpr, len(values))
	for i, list := range values {
		l , err := caseValueExpr(list)
		if err != nil {
			return nil, err
		}
		valueList[i] = l
	}


	ret := &InsertValuesWriter{
		insertStmt: insertStmt,
		shardingColName:  shardingColName,
		shardingColIndex: shardingColIndex,
		values:           valueList,
		shardingTable:    shardingTable,
	}


	return ret, nil
}

func (i *InsertValuesWriter) prepare(bindVariables []*types.BindVariable) error {
	var usedTables []string
	valueMap := make(map[string][][]ast.ValueExpr)
	var params []int

	if len(i.values) > 0 {
		shardingValue := core.ShardingValuesForSingleScalar(i.shardingTable.Name, i.shardingColName)
		for _, valueList := range i.values {
			v := valueList[i.shardingColIndex]

			value, err := explain.GetValueFromExpr(v)
			if err != nil {
				return err
			}
			if value.IsLiteral() {
				if constVal, _ := value.GetValue(nil); constVal == nil {
					return errors.New(fmt.Sprintf("sharding column '%s' value can not be null when use batch insert statement", i.shardingColName))
				}
			}

			var goValue interface{}
			goValue, err = value.GetValue(bindVariables)
			if err != nil {
				return err
			}
			shardingValue.ScalarValues[i.shardingColName][0] = goValue
			tables, e := i.shardingTable.TableStrategy.Shard(i.shardingTable.GetTables(), shardingValue)
			if e != nil {
				return e
			}

			varIndexList := getVarIndexList(valueList)
			params = append(params, varIndexList...)
			for _, t := range tables {
				if _, ok := valueMap[t]; !ok {
					usedTables = append(usedTables, t)
				}
				valueMap[t] = append(valueMap[t], valueList)
			}
		}
	}

	i.valueMappings = valueMap
	i.paramIndexes = params
	return nil
}

func getVarIndexList(values []ast.ValueExpr) []int {
	var indexes []int
	for _, v := range values {
		i := getVarIndex(v)
		if i >= 0 {
			indexes = append(indexes, i)
		}
	}
	return indexes
}

func getVarIndex(v ast.ValueExpr) int {
	argIndex := -1
	switch typedValue := v.(type) {
	case *driver.ParamMarkerExpr:
		argIndex = typedValue.Order
	}
	return argIndex
}

func (i *InsertValuesWriter) Format(ctx explain.StatementContext) error {
	rstCtx := ctx.CreateRestoreCtx()

	table, err := ctx.GetRuntime().GetCurrentTable(i.shardingTable.Name)
	if err != nil {
		return err
	}

	values, ok := i.valueMappings[table]
	if !ok {
		values = i.values
	}

	var usedArgs []int

	ctx.WriteKeyWord("INSERT ")
	ctx.WriteKeyWord("INTO ")
	if err := i.insertStmt.Table.Restore(rstCtx); err != nil {
		return fmt.Errorf("An error occurred while restore InsertStmt.Table, %v", err)
	}
	if i.insertStmt.Columns != nil {
		ctx.WritePlain(" (")
		for i, v := range i.insertStmt.Columns {
			if i != 0 {
				ctx.WritePlain(",")
			}
			if err := v.Restore(rstCtx); err != nil {
				return fmt.Errorf("An error occurred while restore InsertStmt.Columns[%d],%v", i, err)
			}
		}
		ctx.WritePlain(")")
	}

	ctx.WriteKeyWord(" VALUES ")

	for i, list := range values {
		if i != 0 {
			ctx.WritePlain(",")
		}

		ctx.WritePlain("(")

		for vi, v := range  list {
			if vi != 0 {
				ctx.WritePlain(",")
			}
			if err = v.Restore(rstCtx); err != nil {
				return fmt.Errorf("an error occurred while restore PatternInExpr.List[%d], err: %v", i, err)
			}
			if n, ok := TryGetParameterIndex(v); ok {
				usedArgs = append(usedArgs, n)
			}
		}

		ctx.WritePlain(")")
	}


	removed := core.DifferenceInt(i.paramIndexes, usedArgs)
	for _, s := range removed {
		ctx.GetRuntime().RemoveParameter(s)
	}
	return nil
}

func (i *InsertValuesWriter) Text() string {
	return "insert"
}

func (i *InsertValuesWriter) GetFlag() uint64 {
	panic("implement me")
}

func (i *InsertValuesWriter) GetType() *types2.FieldType {
	panic("implement me")
}
