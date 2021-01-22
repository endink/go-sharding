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
