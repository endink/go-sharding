package planner

import (
	"github.com/endink/go-sharding/explain"
	"github.com/endink/go-sharding/parser"
	"github.com/endink/go-sharding/rewriting"
	"github.com/pingcap/parser/ast"
)

func planDelete(del *ast.DeleteStmt, table explain.ShardingTableProvider) (plan *Plan, err error) {
	exp := explain.NewSqlExplain(table)
	if err := exp.ExplainDelete(del, rewriting.NewRewriter()); err != nil {
		return nil, err
	}

	q, err2 := parser.GenerateQuery(del)
	if err2 != nil {
		return nil, err2
	}

	plan = &Plan{
		PlanID: PlanDelete,
		explain: exp,
		FullQuery: q,
		TableName: exp.TableName(),
	}

	if del.Where != nil {
		if q3, e3 := parser.GenerateQuery(del.Where); e3 != nil {
			return nil, e3
		}else {
			plan.Where = q3
		}
	}

	return plan, nil
}
