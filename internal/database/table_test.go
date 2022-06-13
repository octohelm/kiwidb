package database_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/octohelm/kiwidb/internal/database"
	"github.com/octohelm/kiwidb/internal/tree"
	"github.com/octohelm/kiwidb/pkg/dberr"
	"github.com/octohelm/kiwidb/pkg/schema"
	"github.com/octohelm/kiwidb/pkg/testutil"
	. "github.com/octohelm/x/testing"
)

func TestTable(t *testing.T) {
	type Group struct {
		schema.PKey
		Name string `msgp:"name" json:"name"`
	}

	idgen := testutil.NewIDGen(t)
	s := testutil.NewStore(t)
	ts, err := schema.TableSchemaFor(&Group{})
	Expect(t, err, Be[error](nil))

	tx := database.NewTransaction("test", s, idgen)
	tableGroup, err := database.NewTable(tx, ts)
	Expect(t, err, Be[error](nil))

	t.Run("Given group to insert", func(t *testing.T) {
		g := &Group{
			Name: "test",
		}

		_, _, err := tableGroup.Insert(context.Background(), database.DocumentFrom(g))
		Expect(t, err, Be[error](nil))

		t.Run("get by pk", func(t *testing.T) {
			doc, err := tableGroup.Get(context.Background(), tree.NewKey(g.PrimaryKey()))
			Expect(t, err, Be[error](nil))
			found := &Group{}
			err = doc.Unmarshal(found)
			Expect(t, found, Equal(g))

			t.Run("replace by pk", func(t *testing.T) {
				group := &Group{
					Name: "test2",
				}
				group.SetPrimaryKey(g.PrimaryKey())
				err := tableGroup.Replace(context.Background(), tree.NewKey(g.PrimaryKey()), database.DocumentFrom(group))
				Expect(t, err, Be[error](nil))

				doc, err := tableGroup.Get(context.Background(), tree.NewKey(group.PrimaryKey()))
				Expect(t, err, Be[error](nil))
				found := &Group{}
				err = doc.Unmarshal(found)
				Expect(t, found, Equal(group))
			})

			t.Run("delete by pk", func(t *testing.T) {
				err := tableGroup.Delete(context.Background(), tree.NewKey(g.PrimaryKey()))
				Expect(t, err, Be[error](nil))

				_, err = tableGroup.Get(context.Background(), tree.NewKey(g.PrimaryKey()))
				_, ok := dberr.IsNotFoundError(err)
				Expect(t, ok, Be(true))
			})
		})
	})

	t.Run("Given groups to insert", func(t *testing.T) {
		for i := 0; i < 10; i++ {
			g := &Group{
				Name: fmt.Sprintf("test %d", i),
			}
			_, _, err := tableGroup.Insert(context.Background(), database.DocumentFrom(g))
			Expect(t, err, Be[error](nil))
		}

		t.Run("range", func(t *testing.T) {
			err := tableGroup.Range(context.Background(), nil, true, func(key tree.Key, d database.Document) error {
				fmt.Println(key.Values(), d.Value())
				return nil
			})
			Expect(t, err, Be[error](nil))
		})
	})

}
