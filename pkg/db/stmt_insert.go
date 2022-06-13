package db

import (
	"fmt"

	"github.com/octohelm/kiwidb/internal/database"
	"github.com/octohelm/kiwidb/pkg/dberr"
)

func Insert(model any) Operator {
	return &insertOperator{model: model}
}

type insertOperator struct {
	Op
	model any
}

func (op *insertOperator) Iterate(in State, f func(out State) error) error {
	var table database.Table

	return op.Prev().Iterate(in, func(out State) error {
		if table == nil {
			t, err := out.Database().Table(out.Tx(), op.model)
			if err != nil {
				return err
			}
			table = t
		}

		d := database.DocumentFrom(op.model)

		key, d, err := table.Insert(out.Context(), d)
		if err != nil {
			return err
		}

		// FIXME update index
		//table.Indexes

		out.SetKey(key)
		out.SetDocument(d)

		return f(out)
	})
}

func (op *insertOperator) String() string {
	return fmt.Sprintf("Insert()")
}

func DoNothing() Operator {
	return nil
}

func Do() Operator {
	return nil
}

func OnConflict(name string, action Operator) Operator {
	return &onConflict{
		constraint: name,
		action:     action,
	}
}

type onConflict struct {
	Op
	constraint string
	action     Operator
}

func (o *onConflict) Iterate(in State, next func(state State) error) error {
	return o.Prev().Iterate(in, func(state State) error {
		if err := next(state); err != nil {
			if ce, ok := dberr.IsConflictError(err); ok {
				if ce.Name == o.constraint {
					// conflict do nothing
					if o.action == nil {
						return nil
					}

					s := database.NewStateWithContext(in.Context())
					s.SetKey(ce.Key)
					s.SetOuter(state)

					return o.action.Iterate(s, func(state database.State) error {
						return nil
					})
				}
			}
			return err
		}

		return nil
	})
}

func (o *onConflict) String() string {
	if o.action == nil {
		return fmt.Sprintf("OnConflict(%s, DoNothing())", o.constraint)
	}
	return fmt.Sprintf("OnConflict(%s, %s)", o.constraint, database.Stringify(o.action))
}
