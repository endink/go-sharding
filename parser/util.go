package parser

import (
	"github.com/pingcap/parser"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/format"
	_ "github.com/pingcap/tidb/types/parser_driver"
	"strings"
)

var _testParser *parser.Parser

func getTesterParser() *parser.Parser {
	if _testParser == nil {
		_testParser = parser.New()
	}
	return _testParser
}

//仅用于测试
func ParseSQL(sql string) (ast.StmtNode, error) {
	n, e := getTesterParser().ParseOneStmt(sql, "", "")
	return n, e
}

const resultTableNameFlag format.RestoreFlags = 0

// NodeToStringWithoutQuote get node text
func NodeToStringWithoutQuote(node ast.Node) (string, error) {
	s := &strings.Builder{}
	if err := node.Restore(format.NewRestoreCtx(resultTableNameFlag, s)); err != nil {
		return "", err
	}
	return s.String(), nil
}
