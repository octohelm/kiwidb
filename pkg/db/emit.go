package db

import (
	"encoding/json"
	"strings"

	"github.com/octohelm/kiwidb/internal/database"
)

func Omit(values ...database.Document) Operator {
	return &omitOperator{docs: values}
}

type omitOperator struct {
	Op
	docs []database.Document
}

func (op *omitOperator) Iterate(in State, next func(out State) error) error {
	for i := range op.docs {
		in.SetDocument(op.docs[i])
		if err := next(in); err != nil {
			return err
		}
	}
	return nil
}

func (op *omitOperator) String() string {
	var sb strings.Builder

	sb.WriteString("Omit(")
	for i, d := range op.docs {
		if i > 0 {
			sb.WriteString(", ")
		}
		data, _ := json.Marshal(d.Value())
		sb.Write(data)
	}
	sb.WriteByte(')')

	return sb.String()
}
