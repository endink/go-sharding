package planner

import (
	"github.com/endink/go-sharding/explain"
	"github.com/endink/go-sharding/parser"
	"github.com/endink/go-sharding/rewriting"
	"github.com/pingcap/parser/ast"
)

func planInsert(ist *ast.InsertStmt, tables explain.ShardingTableProvider) (*Plan, error) {
	exp := explain.NewSqlExplain(tables)
	if err := exp.ExplainInsert(ist, rewriting.NewRewriter()); err != nil {
		return nil, err
	}

	q, err2 := parser.GenerateQuery(ist)
	if err2 != nil {
		return nil, err2
	}

	plan := &Plan{
		PlanID:     PlanSelect,
		FullQuery: q,
	}
	plan.explain = exp

	return plan, nil
}
