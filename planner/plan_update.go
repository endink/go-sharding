package planner

import (
	"github.com/endink/go-sharding/explain"
	"github.com/endink/go-sharding/parser"
	"github.com/endink/go-sharding/rewriting"
	"github.com/pingcap/parser/ast"
)

// planUpdate code is almost identical to planeDelete.
func planUpdate(upd *ast.UpdateStmt, table explain.ShardingTableProvider) (plan *Plan, err error) {
	exp := explain.NewSqlExplain(table)
	if err := exp.ExplainUpdate(upd, rewriting.NewRewriter()); err != nil {
		return nil, err
	}

	q, err2 := parser.GenerateQuery(upd)
	if err2 != nil {
		return nil, err2
	}

	plan = &Plan{
		PlanID: PlanUpdate,
		explain: exp,
		FullQuery: q,
	}

	if upd.Where != nil {
		if q3, e3 := parser.GenerateQuery(upd.Where); e3 != nil {
			return nil, e3
		}else {
			plan.Where = q3
		}
	}

	return plan, nil
}
