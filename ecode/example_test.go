package ecode_test

import (
	"fmt"

	"github.com/pkg/errors"

	"github.com/Darker-D/ddbase/ecode"
)

func ExampleCause() {
	err := errors.WithStack(ecode.AccessDenied)
	ecode.Cause(err)
}

func ExampleInt() {
	err := ecode.Int(500)
	fmt.Println(err)
	// Output:
	// 500
}

func ExampleString() {
	ecode.String("500")
}

// ExampleStack package error with stack.
func Example() {
	err := errors.New("dao error")
	errors.Wrap(err, "some message")
	// package ecode with stack.
	errCode := ecode.AccessDenied
	err = errors.Wrap(errCode, "some message")

	//get ecode from package error
	code := errors.Cause(err).(ecode.Codes)
	fmt.Printf("%d: %s\n", code.Code(), code.Message())
}
