package sql

import (
	"strings"
	"unicode"
)

const (
	StmtSelect = iota
	StmtStream
	StmtInsert
	StmtReplace
	StmtUpdate
	StmtDelete
	StmtDDL
	StmtBegin
	StmtCommit
	StmtRollback
	StmtSet
	StmtShow
	StmtUse
	StmtOther
	StmtUnknown
	StmtComment
)

// Preview analyzes the beginning of the query using a simpler and faster
// textual comparison to identify the statement type.
func PreviewSql(sql string) int {
	trimmed := StripLeadingComments(sql)

	firstWord := trimmed
	if end := strings.IndexFunc(trimmed, unicode.IsSpace); end != -1 {
		firstWord = trimmed[:end]
	}
	firstWord = strings.TrimLeftFunc(firstWord, func(r rune) bool { return !unicode.IsLetter(r) })
	// Comparison is done in order of priority.
	loweredFirstWord := strings.ToLower(firstWord)
	switch loweredFirstWord {
	case "select":
		return StmtSelect
	case "stream":
		return StmtStream
	case "insert":
		return StmtInsert
	case "replace":
		return StmtReplace
	case "update":
		return StmtUpdate
	case "delete":
		return StmtDelete
	}
	// For the following statements it is not sufficient to rely
	// on loweredFirstWord. This is because they are not statements
	// in the grammar and we are relying on Preview to parse them.
	// For instance, we don't want: "BEGIN JUNK" to be parsed
	// as StmtBegin.
	trimmedNoComments, _ := SplitMarginComments(trimmed)
	switch strings.ToLower(trimmedNoComments) {
	case "begin", "start transaction":
		return StmtBegin
	case "commit":
		return StmtCommit
	case "rollback":
		return StmtRollback
	}
	switch loweredFirstWord {
	case "create", "alter", "rename", "drop", "truncate", "flush":
		return StmtDDL
	case "set":
		return StmtSet
	case "show":
		return StmtShow
	case "use":
		return StmtUse
	case "analyze", "describe", "desc", "explain", "repair", "optimize":
		return StmtOther
	}
	if strings.Index(trimmed, "/*!") == 0 {
		return StmtComment
	}
	return StmtUnknown
}

// StmtType returns the statement type as a string
func GetStmtType(stmtType int) string {
	switch stmtType {
	case StmtSelect:
		return "SELECT"
	case StmtStream:
		return "STREAM"
	case StmtInsert:
		return "INSERT"
	case StmtReplace:
		return "REPLACE"
	case StmtUpdate:
		return "UPDATE"
	case StmtDelete:
		return "DELETE"
	case StmtDDL:
		return "DDL"
	case StmtBegin:
		return "BEGIN"
	case StmtCommit:
		return "COMMIT"
	case StmtRollback:
		return "ROLLBACK"
	case StmtSet:
		return "SET"
	case StmtShow:
		return "SHOW"
	case StmtUse:
		return "USE"
	case StmtOther:
		return "OTHER"
	default:
		return "UNKNOWN"
	}
}
