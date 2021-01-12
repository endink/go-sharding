package script

import (
	"github.com/d5/tengo/v2"
)

type CompiledScript interface {
}

type tengoScript struct {
	compiled *tengo.Compiled
}

func ParseScript(script string, variables map[string]interface{}) (CompiledScript, error) {
	if parser, err := NewScriptParser(script); err != nil {
		return nil, err
	} else {
		if variables != nil {
			for name, value := range variables {
				if err := parser.AddVariable(name, value); err != nil {
					return nil, err
				}
			}
		}
		return parser.Parse()
	}
}
