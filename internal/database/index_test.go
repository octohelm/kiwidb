package database_test

import (
	"context"
	"testing"

	"github.com/octohelm/kiwidb/internal/database"
	"github.com/octohelm/kiwidb/internal/tree"
	"github.com/octohelm/kiwidb/pkg/schema"
	"github.com/octohelm/kiwidb/pkg/testutil"
	. "github.com/octohelm/x/testing"
)

func TestIndex(t *testing.T) {
	idgen := testutil.NewIDGen(t)
	s := testutil.NewStore(t)

	tx := database.NewTransaction("test", s, idgen)

	t.Run("index name", func(t *testing.T) {
		index := database.NewIndex(tx, &schema.IndexSchema{
			IndexType: schema.Index,
			Paths: []schema.KeyPath{
				{"name"},
			},
		})

		t.Run("When set index", func(t *testing.T) {
			err := index.Set(context.Background(), []any{"hello"}, tree.NewKey(1))
			Expect(t, err, Be[error](nil))

			t.Run("could index", func(t *testing.T) {
				ok, key, err := index.Exists(context.Background(), []any{"hello"})
				Expect(t, err, Be[error](nil))
				Expect(t, ok, Be(true))
				Expect(t, key.Values(), Equal(tree.NewKey(int32(1)).Values()))
			})

			t.Run("When delete index", func(t *testing.T) {
				err := index.Delete(context.Background(), []any{"hello"}, tree.NewKey(1))
				Expect(t, err, Be[error](nil))

				t.Run("could index", func(t *testing.T) {
					ok, _, err := index.Exists(context.Background(), []any{"hello"})
					Expect(t, err, Be[error](nil))
					Expect(t, ok, Be(false))
				})
			})
		})

		t.Run("When set indexes", func(t *testing.T) {
			for i := 0; i < 10; i++ {
				err := index.Set(context.Background(), []any{"hello"}, tree.NewKey(i))
				Expect(t, err, Be[error](nil))

				err = index.Set(context.Background(), []any{"hello2"}, tree.NewKey(i))
				Expect(t, err, Be[error](nil))
			}

			t.Run("could range all ids", func(t *testing.T) {
				rng := tree.NewRange(tree.NewKey("hello"), tree.NewKey("hello"), false)
				ids := make([]int32, 0)
				err := index.Range(context.Background(), rng, false, func(key tree.Key) error {
					ids = append(ids, key.Values()[0].(int32))
					return nil
				})
				Expect(t, err, Be[error](nil))
				Expect(t, len(ids), Be(10))
			})

		})
	})
}
