package script

import (
	"errors"
	"github.com/d5/tengo/v2"
)

var RangeFunction = &rangeFunction{}

type rangeFunction struct {
	tengo.ObjectImpl
}

func (o *rangeFunction) CanCall() bool {
	return true
}

func (o *rangeFunction) IsFalsy() bool {
	return true
}

func (o *rangeFunction) Call(args ...tengo.Object) (ret tengo.Object, err error) {
	if len(args) != 2 {
		return nil, tengo.ErrWrongNumArguments
	}

	s1, ok := tengo.ToInt64(args[0])
	if !ok {
		return nil, tengo.ErrInvalidArgumentType{
			Name:     "begin",
			Expected: "int",
			Found:    args[0].TypeName(),
		}
	}

	s2, ok := tengo.ToInt64(args[1])
	if !ok {
		return nil, tengo.ErrInvalidArgumentType{
			Name:     "end",
			Expected: "int",
			Found:    args[0].TypeName(),
		}
	}

	if s1 > s2 {
		return nil, errors.New("the begin parameter must be less than or equal to the end argument for using 'range' function in inline expression")
	}

	array := make([]tengo.Object, s2-s1+1, s2-s1+1)

	if s1 == s2 {
		array[0] = &tengo.Int{Value: s1}
	} else {
		index := 0
		for i := s1; i <= s2; i++ {
			array[index] = &tengo.Int{Value: i}
			index++
		}
	}

	return &tengo.ImmutableArray{
		Value: array,
	}, nil
}
