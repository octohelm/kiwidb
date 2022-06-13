package db

import (
	"fmt"
)

func Limit(limit int64) Operator {
	return &limitOperator{limit: limit}
}

type limitOperator struct {
	Op
	limit int64
}

func (op *limitOperator) Iterate(in State, f func(out State) error) error {
	var count int64

	return op.Prev().Iterate(in, func(out State) error {
		if count < op.limit {
			count++
			return f(out)
		}
		return nil
	})
}

func (op *limitOperator) String() string {
	return fmt.Sprintf("docs.Limit(%d)", op.limit)
}
