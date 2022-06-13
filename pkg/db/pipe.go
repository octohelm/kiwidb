package db

import "github.com/octohelm/kiwidb/internal/database"

func Pipe(operators ...Operator) Operator {
	return database.Pipe(operators...)
}
