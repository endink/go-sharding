package core

import (
	"fmt"
	"strings"
)

type StringBuilder struct {
	buffer strings.Builder
}

func NewStringBuilder(s ...string) *StringBuilder {
	sb := StringBuilder{}
	if s != nil && len(s) > 0 {
		_, _ = sb.buffer.WriteString(fmt.Sprint(s))
	}
	return &sb
}

func (w *StringBuilder) Clear() {
	w.buffer.Reset()
}

func (w *StringBuilder) WriteLine(value ...interface{}) {
	for _, v := range value {
		w.Write(v)
	}
	w.buffer.WriteString(LineSeparator)
}

func (w *StringBuilder) WriteLineForEach(value ...interface{}) {
	for _, v := range value {
		w.WriteLine(v)
	}
}

func (w *StringBuilder) WriteJoinCustomize(sep string, print func(item interface{}) string, elems ...interface{}) {
	switch len(elems) {
	case 0:
		return
	case 1:
		w.Write(elems[0])
	}
	n := len(sep) * (len(elems) - 1)
	for i := 0; i < len(elems); i++ {
		n += len(print(elems[i]))
	}

	w.buffer.Grow(n)
	w.buffer.WriteString(fmt.Sprint(elems[0]))
	for _, s := range elems[1:] {
		w.buffer.WriteString(sep)
		w.buffer.WriteString(print(s))
	}
}

func (w *StringBuilder) WriteJoin(sep string, elems ...interface{}) {
	w.WriteJoinCustomize(sep, func(item interface{}) string {
		return fmt.Sprint(item)
	}, elems...)
}

func (w *StringBuilder) Write(value ...interface{}) {
	for _, v := range value {
		if a, isString := v.(string); isString {
			_, _ = w.buffer.WriteString(a)
			return
		}

		if b, isBuilder := v.(fmt.Stringer); isBuilder {
			_, _ = w.buffer.WriteString(b.String())
			return
		}
		_, _ = w.buffer.WriteString(fmt.Sprint(v))
	}
}

func (w *StringBuilder) WriteLineF(format string, args ...interface{}) {
	w.WriteFormat(format, args...)
	w.buffer.WriteString(LineSeparator)
}

func (w *StringBuilder) WriteFormat(format string, arg ...interface{}) {
	_, _ = w.buffer.WriteString(fmt.Sprintf(format, arg...))
}

func (w *StringBuilder) String() string {
	return w.buffer.String()
}
