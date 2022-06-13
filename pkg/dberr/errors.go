package dberr

import (
	"fmt"

	"github.com/octohelm/kiwidb/internal/tree"

	"github.com/cockroachdb/errors"
)

func IsNotFoundError(err error) (*NotFoundError, bool) {
	err = errors.UnwrapAll(err)
	switch x := err.(type) {
	case NotFoundError:
		return &x, true
	case *NotFoundError:
		return x, true
	default:
		return nil, false
	}
}

type NotFoundError struct {
	Name string
}

func (a NotFoundError) Error() string {
	return fmt.Sprintf("%q not found", a.Name)
}

func IsConflictError(err error) (*ConflictError, bool) {
	err = errors.UnwrapAll(err)
	switch x := err.(type) {
	case ConflictError:
		return &x, true
	case *ConflictError:
		return x, true
	default:
		return nil, false
	}
}

type ConflictError struct {
	Name string
	Key  tree.Key
}

func (a ConflictError) Error() string {
	return fmt.Sprintf("%q not found", a.Name)
}
