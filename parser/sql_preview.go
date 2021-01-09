package parser

import (
	"strings"
	"unicode"
)

type StatementType int

const (
	StmtSelect StatementType = iota
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
	StmtExplain
	StmtPriv
	StmtSavepoint
	StmtRelease
	StmtSRollback
)

// Preview analyzes the beginning of the query using a simpler and faster
// textual comparison to identify the statement type.
func PreviewSql(sql string) StatementType {
	trimmed := StripLeadingComments(sql)

	if strings.Index(trimmed, "/*!") == 0 {
		return StmtComment
	}

	isNotLetter := func(r rune) bool { return !unicode.IsLetter(r) }
	firstWord := strings.TrimLeftFunc(trimmed, isNotLetter)

	if end := strings.IndexFunc(firstWord, unicode.IsSpace); end != -1 {
		firstWord = firstWord[:end]
	}
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
	case "savepoint":
		return StmtSavepoint
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
	case "describe", "desc", "explain":
		return StmtExplain
	case "analyze", "repair", "optimize":
		return StmtOther
	case "grant", "revoke":
		return StmtPriv
	case "release":
		return StmtRelease
	case "rollback":
		return StmtSRollback
	}
	return StmtUnknown
}

func (s StatementType) String() string {
	switch s {
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
	case StmtPriv:
		return "PRIV"
	case StmtExplain:
		return "EXPLAIN"
	case StmtSavepoint:
		return "SAVEPOINT"
	case StmtSRollback:
		return "SAVEPOINT_ROLLBACK"
	case StmtRelease:
		return "RELEASE"
	default:
		return "UNKNOWN"
	}
}

func (s StatementType) CanHandleWithoutPlan() bool {
	switch s {
	case StmtShow, StmtSet, StmtBegin, StmtComment, StmtRollback, StmtUse, StmtPriv, StmtSavepoint, StmtRelease:
		return true
	}
	return false
}
