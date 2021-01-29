/*
 * Copyright 2021. Go-Sharding Author All Rights Reserved.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 *  File author: Anders Xiao
 */

package util

import (
	"fmt"
	"io"
)

// Wrap returns an error annotating err with a stack trace
// at the point Wrap is called, and the supplied message.
// If err is nil, Wrap returns nil.
func Wrap(err error, message string) error {
	if err == nil {
		return nil
	}
	return &wrapping{
		cause: err,
		msg:   message,
	}
}

// Wrapf returns an error annotating err with a stack trace
// at the point Wrapf is call, and the format specifier.
// If err is nil, Wrapf returns nil.
func Wrapf(err error, format string, args ...interface{}) error {
	if err == nil {
		return nil
	}
	return &wrapping{
		cause: err,
		msg:   fmt.Sprintf(format, args...),
	}
}

type wrapping struct {
	cause error
	msg   string
}

func (w *wrapping) Error() string { return w.msg + ": " + w.cause.Error() }
func (w *wrapping) Cause() error  { return w.cause }

func (w *wrapping) Format(s fmt.State, verb rune) {
	if rune('v') == verb {
		panicIfError(fmt.Fprintf(s, "%v\n", w.Cause()))
		panicIfError(io.WriteString(s, w.msg))
		return
	}

	if rune('s') == verb || rune('q') == verb {
		panicIfError(io.WriteString(s, w.Error()))
	}
}

// since we can't return an error, let's panic if something goes wrong here
func panicIfError(_ int, err error) {
	if err != nil {
		panic(err)
	}
}

// RootCause returns the underlying cause of the error, if possible.
// An error value has a cause if it implements the following
// interface:
//
//     type causer interface {
//            Cause() error
//     }
//
// If the error does not implement Cause, the original error will
// be returned. If the error is nil, nil will be returned without further
// investigation.
func RootCause(err error) error {
	for {
		cause := Cause(err)
		if cause == nil {
			return err
		}
		err = cause
	}
}

//
// Cause will return the immediate cause, if possible.
// An error value has a cause if it implements the following
// interface:
//
//     type causer interface {
//            Cause() error
//     }
// If the error does not implement Cause, nil will be returned
func Cause(err error) error {
	type causer interface {
		Cause() error
	}

	causerObj, ok := err.(causer)
	if !ok {
		return nil
	}

	return causerObj.Cause()
}
