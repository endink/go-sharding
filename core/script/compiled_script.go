package script

import (
	"fmt"
	"github.com/XiaoMi/Gaea/core"
	"github.com/d5/tengo/v2"
	"golang.org/x/tools/go/ssa/interp/testdata/src/errors"
	"reflect"
)

type CompiledScript interface {
	Run() ([]string, error)
}

type tengoScript struct {
	raw       *string
	compiled  *tengo.Compiled
	resultVar string
}

func (script *tengoScript) Run() ([]string, error) {
	if err := script.compiled.Run(); err != nil {
		return nil, err
	} else {
		v := script.compiled.Get(script.resultVar)
		golangValue := v.Value()
		//switch v.ValueType() {
		//case "array", "array-iterator", "string-iterator", "immutable-array":
		//	return stringArray(v.Array()), nil
		//case "int", "float", "char":
		//	return []string{fmt.Sprint(v.Value())}, nil
		//case "string":
		//	return []string{v.String()}, nil
		//default:
		//	return nil, invalidReturnTypeError(*script.raw, v)
		//}
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
			return nil, invalidReturnTypeError(*script.raw, v)
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

func invalidReturnTypeError(raw string, v *tengo.Variable) error {
	return errors.New(fmt.Sprint("script return invalid type, excepted array that element is number or string, and primitive number or string", core.LineSeparator, "script: ", raw, core.LineSeparator, "return type:", v.ValueType()))
}

func ParseScript(script string, variables map[string]interface{}) (CompiledScript, error) {
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
