package parser

import (
	"github.com/pingcap/parser"
	"github.com/pingcap/parser/ast"
	_ "github.com/pingcap/tidb/types/parser_driver"
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
