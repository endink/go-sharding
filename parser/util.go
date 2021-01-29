package parser

import (
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/format"
	"strings"
)

const resultTableNameFlag format.RestoreFlags = 0

// NodeToStringWithoutQuote get node text
func NodeToStringWithoutQuote(node ast.Node) (string, error) {
	s := &strings.Builder{}
	if err := node.Restore(format.NewRestoreCtx(resultTableNameFlag, s)); err != nil {
		return "", err
	}
	return s.String(), nil
}
