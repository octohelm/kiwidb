package database_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/octohelm/kiwidb/pkg/schema"

	"github.com/octohelm/kiwidb/internal/database"
	"github.com/octohelm/kiwidb/internal/tree"

	"github.com/octohelm/kiwidb/pkg/testutil"

	. "github.com/octohelm/x/testing"
)

type User struct {
	schema.PKey
	Name string `msgp:"name" json:"name"`
}

func TestDatabase(t *testing.T) {
	t.Run("InsertUser", func(t *testing.T) {
		db := testutil.NewDatabase(t, "test")
		tx := db.Begin()
		tableUser, err := db.Table(tx, &User{})
		Expect(t, err, Be[error](nil))

		for i := 0; i < 100; i++ {
			usr := &User{
				Name: fmt.Sprintf("test - %d", i),
			}
			_, _, err := tableUser.Insert(context.Background(), database.DocumentFrom(usr))
			Expect(t, err, Be[error](nil))
		}

		err = tx.Commit()
		Expect(t, err, Be[error](nil))

		t.Run("Table Range", func(t *testing.T) {
			tx := db.Begin(database.TransactionReadOnly())

			tUser, err := db.Table(tx, &User{})
			Expect(t, err, Be[error](nil))

			ids := make([]uint64, 0)
			err = tUser.Range(context.Background(), nil, false, func(key tree.Key, d database.Document) error {
				ids = append(ids, key.Values()[0].(uint64))
				return nil
			})
			Expect(t, err, Be[error](nil))
			Expect(t, len(ids), Be(100))
		})
	})

}
