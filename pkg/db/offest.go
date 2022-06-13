package db

import (
	"fmt"
)

func Offset(offset int64) Operator {
	return &offsetOperator{offset: offset}
}

type offsetOperator struct {
	Op
	offset int64
}

func (op *offsetOperator) Iterate(in State, f func(out State) error) error {
	var offset int64

	return op.Prev().Iterate(in, func(out State) error {
		if offset < op.offset {
			offset++
			return nil
		}
		return f(out)
	})
}

func (op *offsetOperator) String() string {
	return fmt.Sprintf("docs.Offset(%d)", op.offset)
}
