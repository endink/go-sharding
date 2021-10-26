package explain

import (
	"fmt"
	"github.com/pingcap/parser/ast"
	driver "github.com/pingcap/tidb/types/parser_driver"
)

func (s *SqlExplain) explainInsertValues(
	ist *ast.InsertStmt,
	mode insertMode,
	shardingColIndex int,
	col *ast.ColumnName) error {


	sd, _, err := FindShardingTableByColumn(col, s.Context(), true)
	if err != nil {
		return err
	}

	colName := GetColumn(col)

	switch mode {
	case insertSingle:
		valueItem := ist.Setlist[shardingColIndex].Expr
		v, isValue := valueItem.(*driver.ValueExpr)
		if isValue && IsSupportedValue(v) {
			r, err := GetValueFromExpr(v)
			if err != nil {
				return fmt.Errorf("get value expr result failed, %v", err)
			}
			if v == nil {
				return fmt.Errorf("sharding value cannot be null")
			}
			s.pushAndValue(sd.Name, colName, r)
		}
	case insertBatch:
		// not assignment mode
		result, err := s.rewriter.RewriteInsertValues(ist, s.Context())
		if err != nil {
			return err
		}
		var values []ValueReference
		if result.IsRewrote() {
			for _, valueList := range ist.Lists {
				valueItem := valueList[shardingColIndex]
				v, isValue:= CaseValueOrParamExpr(valueItem)
				if isValue {
					r, err := GetValueFromExpr(v)
					if err != nil {
						return fmt.Errorf("get value expr result failed, %v", err)
					}
					if v == nil {
						return fmt.Errorf("sharding value cannot be null")
					}
					values = append(values, r)
				}
			}
			//批量插入比较特殊，没有太好的办法处理，只能完全重写语句
			s.AstNode = wrapFormatter(result.GetFormatter())
		}
		if len(values) > 0{
			s.pushOrValueGroup(result.GetShardingTable(), result.GetColumn(), values...)
		}
	}

	return nil
}