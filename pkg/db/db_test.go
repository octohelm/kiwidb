package db

import (
	"context"
	"testing"

	"github.com/octohelm/kiwidb/pkg/schema"
	"github.com/octohelm/kiwidb/pkg/testutil"
	testing2 "github.com/octohelm/x/testing"
)

type User struct {
	schema.PKey
	Name string `msgp:"constraint" json:"constraint"`
	Desc string `msgp:"desc,omitempty" json:"desc,omitempty"`
}

func (User) Indexes() map[string]schema.IndexType {
	return map[string]schema.IndexType{
		"constraint": schema.UniqueIndex,
	}
}

func TestDB(t *testing.T) {
	d := testutil.NewDatabase(t, "test")

	t.Run("Insert", func(t *testing.T) {
		op := Insert(&User{
			Name: "hello",
		})

		err := d.Execute(context.Background(), op)
		testing2.Expect(t, err, testing2.Be[error](nil))

		t.Run("Insert again", func(t *testing.T) {
			op := Pipe(
				OnConflict("name", DoNothing()),
				Insert(&User{
					Name: "hello",
				}),
			)
			err := d.Execute(context.Background(), op)
			testing2.Expect(t, err, testing2.Be[error](nil))
		})
	})
}
