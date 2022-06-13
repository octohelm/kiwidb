package db

import (
	"fmt"
)

func Eq[T comparable](v T) Matcher[T] {
	return MatchFunc[T](func(actual T) (bool, error) {
		return v == actual, nil
	}, fmt.Sprintf("= %v", v))
}

func MatchFunc[T any](match func(actual T) (bool, error), desc string) Matcher[T] {
	return &matcher[T]{
		match: match,
		desc:  desc,
	}
}

type matcher[T any] struct {
	match func(actual T) (bool, error)
	desc  string
}

func (m *matcher[T]) Match(actual T) (bool, error) {
	return m.match(actual)
}

func (m *matcher[T]) String() string {
	return m.desc
}

type Matcher[T any] interface {
	Match(actual T) (bool, error)
	String() string
}

func Filter[T any](name string, matcher Matcher[T]) Operator {
	return &filterOperator[T]{
		name:    name,
		matcher: matcher,
	}
}

type filterOperator[T any] struct {
	Op
	name    string
	matcher Matcher[T]
}

func (op *filterOperator[T]) Iterate(in State, f func(out State) error) error {
	return op.Prev().Iterate(in, func(out State) error {
		doc := out.Document()
		if doc == nil {
			return nil
		}

		actual, err := doc.Field(op.name)
		if err != nil {
			return err
		}

		v, typeMatched := actual.Value().(T)
		if !typeMatched {
			return nil
		}

		ok, err := op.matcher.Match(v)
		if err != nil {
			return err
		}
		if ok {
			return f(out)
		}

		return nil
	})
}

func (op *filterOperator[T]) String() string {
	return fmt.Sprintf("Filter(%s %s)", op.name, op.matcher)
}
