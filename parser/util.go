package parser

import (
	"fmt"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/format"
	"github.com/pingcap/parser/opcode"
	driver "github.com/pingcap/tidb/types/parser_driver"
	"strings"
)

const resultTableNameFlag format.RestoreFlags = 0

var EscapeRestoreFlags = format.RestoreStringSingleQuotes | format.RestoreStringEscapeBackslash | format.RestoreKeyWordUppercase | format.RestoreNameBackQuotes

var lockingFunctions = map[string]interface{}{
	"get_lock":          nil,
	"is_free_lock":      nil,
	"is_used_lock":      nil,
	"release_all_locks": nil,
	"release_lock":      nil,
}

var ImpossibleWhereClause = &ast.BinaryOperationExpr{
	L:  makeConstValue(1),
	R:  makeConstValue(1),
	Op: opcode.NE,
}

func makeConstValue(value int64) *driver.ValueExpr {
	nv := &driver.ValueExpr{}
	nv.SetInt64(value)
	return nv
}

// NodeToStringWithoutQuote get node text
func NodeToStringWithoutQuote(node ast.Node) (string, error) {
	s := &strings.Builder{}
	if err := node.Restore(format.NewRestoreCtx(resultTableNameFlag, s)); err != nil {
		return "", err
	}
	return s.String(), nil
}

//IsLockingFunc returns true for all functions that are used to work with mysql advisory locks
func IsLockingFunc(node *ast.FuncCallExpr) bool {
	_, found := lockingFunctions[node.FnName.L]
	return found
}

// checkForPoolingUnsafeConstructs returns an error if the SQL expression contains
// a call to GET_LOCK(), which is unsafe with server-side connection pooling.
func CheckForPoolingUnsafeConstructs(expr ast.StmtNode) error {

	genError := func(node ast.Node) error {
		return fmt.Errorf("'%s' not allowed without a reserved connections", node.Text())
	}

	return Walk(func(in ast.Node) (kontinue bool, err error) {
		switch node := in.(type) {
		case *ast.SetStmt:
			for _, setExpr := range node.Variables {
				if setExpr.IsSystem || setExpr.Name != ast.SetNames {
					return false, genError(node)
				}
			}
		case *ast.FuncCallExpr:
			if IsLockingFunc(node) {
				return false, genError(node)
			}
		}

		// TODO: This could be smarter about not walking down parts of the AST that can't contain
		// function calls.
		return true, nil
	}, expr)
}

func NewLimit(count int64) *ast.Limit {
	nv := &driver.ValueExpr{}
	nv.SetInt64(count)
	return &ast.Limit{
		Count: nv,
	}
}

// GenerateLimitQuery generates a select query with a limit clause.
func GenerateLimitQuery(selStmt ast.StmtNode, count int64) (*ParsedQuery, error) {
	switch sel := selStmt.(type) {
	case *ast.SelectStmt:
		limit := sel.Limit
		if limit == nil {
			sel.Limit = NewLimit(count)
			defer func() {
				sel.Limit = nil
			}()
		}
	case *ast.UnionStmt:
		// Code is identical to *Select, but this one is a *Union.
		limit := sel.Limit
		if limit == nil {
			sel.Limit = NewLimit(count)
			defer func() {
				sel.Limit = nil
			}()
		}
	}
	sb := &strings.Builder{}
	rctx := &format.RestoreCtx{
		Flags: format.DefaultRestoreFlags,
		In:    sb,
	}

	err := selStmt.Restore(rctx)
	if err != nil {
		return nil, err
	}
	return BuildParsedQuery(sb.String()), nil
}

func WriteNode(node ast.Node, flag format.RestoreFlags) (string, error) {
	var sb = new(strings.Builder)
	ctx := format.NewRestoreCtx(flag, sb)
	err := node.Restore(ctx)
	if err != nil {
		return "", err
	} else {
		return sb.String(), nil
	}
}

// GenerateFieldQuery generates a query to just fetch the field info
// by adding impossible where clauses as needed.
//func GenerateFieldQuery(statement ast.StmtNode) *ParsedQuery {
//	buf := sqlparser.NewTrackedBuffer(sqlparser.FormatImpossibleQuery).WriteNode(statement)
//
//	if buf.HasBindVars() {
//		return nil
//	}
//
//	return buf.ParsedQuery()
//}
