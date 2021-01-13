package script

import (
	"fmt"
	"github.com/XiaoMi/Gaea/core"
	"github.com/d5/tengo/v2"
	"golang.org/x/tools/go/ssa/interp/testdata/src/errors"
	"reflect"
)

type CompiledScript interface {
	ExecuteList() ([]string, error)
	ExecuteScalar() (string, error)
}

type tengoScript struct {
	raw       *string
	compiled  *tengo.Compiled
	resultVar string
}

func (script *tengoScript) ExecuteScalar() (string, error) {
	if err := script.compiled.Run(); err != nil {
		return "", err
	} else {
		v := script.compiled.Get(script.resultVar)
		golangValue := v.Value()
		kind := reflect.TypeOf(golangValue).Kind()
		switch kind {
		case reflect.Int,
			reflect.Float32,
			reflect.Float64,
			reflect.Int8,
			reflect.Int16,
			reflect.Int32,
			reflect.Int64,
			reflect.Uint,
			reflect.Uint8,
			reflect.Uint16,
			reflect.Uint64,
			reflect.String:
			return fmt.Sprint(golangValue), nil
		default:
			return "", invalidReturnTypeError(*script.raw, v, false)
		}

	}
}

func (script *tengoScript) ExecuteList() ([]string, error) {
	if err := script.compiled.Run(); err != nil {
		return nil, err
	} else {
		v := script.compiled.Get(script.resultVar)
		golangValue := v.Value()
		kind := reflect.TypeOf(golangValue).Kind()
		switch kind {
		case reflect.Array, reflect.Slice:
			return stringArray(golangValue.([]interface{})), nil
		case reflect.Int,
			reflect.Float32,
			reflect.Float64,
			reflect.Int8,
			reflect.Int16,
			reflect.Int32,
			reflect.Int64,
			reflect.Uint,
			reflect.Uint8,
			reflect.Uint16,
			reflect.Uint64,
			reflect.String:
			return []string{fmt.Sprint(golangValue)}, nil
		default:
			return nil, invalidReturnTypeError(*script.raw, v, true)
		}

	}
}

func stringArray(array []interface{}) []string {
	list := make([]string, len(array), len(array))
	for i, v := range array {
		list[i] = fmt.Sprint(v)
	}
	return list
}

func invalidReturnTypeError(raw string, v *tengo.Variable, allowArray bool) error {
	var arrayDesc = ""
	if allowArray {
		arrayDesc = " and array that element is number or string"
	}
	return errors.New(fmt.Sprint(
		"script return invalid type, excepted primitive number or string",
		arrayDesc,
		core.LineSeparator,
		"script: ",
		raw,
		core.LineSeparator,
		"return type:",
		v.ValueType()))
}

func ParseScript(script string) (CompiledScript, error) {
	return ParseScriptVar(script, nil)
}

func ExeScriptScalar(script string, variables map[string]interface{}) (string, error) {
	if s, err := ParseScriptVar(script, variables); err != nil {
		return "", err
	} else {
		if r, err := s.ExecuteScalar(); err != nil {
			return "", err
		} else {
			return r, nil
		}
	}
}

func ExeScriptList(script string, variables map[string]interface{}) ([]string, error) {
	if s, err := ParseScriptVar(script, variables); err != nil {
		return nil, err
	} else {
		if r, err := s.ExecuteList(); err != nil {
			return nil, err
		} else {
			return r, nil
		}
	}
}

func ParseScriptVar(script string, variables map[string]interface{}) (CompiledScript, error) {
	if parser, err := NewScriptParser(script); err != nil {
		return nil, err
	} else {
		if variables != nil {
			for name, value := range variables {
				if err := parser.Var(name, value); err != nil {
					return nil, err
				}
			}
		}
		return parser.Compile()
	}
}
