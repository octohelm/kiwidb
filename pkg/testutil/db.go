package testutil

import (
	"context"
	"io/ioutil"
	"os"
	"testing"

	"github.com/octohelm/kiwidb/internal/database"
	"github.com/octohelm/kiwidb/internal/tree"
	"github.com/octohelm/kiwidb/pkg/kv"
	_ "github.com/octohelm/kiwidb/pkg/kv/pebble"
	. "github.com/octohelm/x/testing"
)

func TempDir(t testing.TB) string {
	dir, err := ioutil.TempDir("", "kiwidb")
	Expect(t, err, Be[error](nil))
	t.Cleanup(func() {
		os.RemoveAll(dir)
	})
	return dir
}

func NewStore(t testing.TB) kv.Store {
	dir := TempDir(t)
	t.Helper()
	s, err := kv.NewStore("pebble", kv.Options{
		Extra: map[string]string{
			"path": dir,
		},
	})
	Expect(t, err, Be[error](nil))
	t.Cleanup(func() {
		s.Shutdown(context.Background())
	})
	return s
}

func NewTree(t testing.TB, namespace tree.Namespace) *tree.Tree {
	t.Helper()
	s := NewStore(t)
	session := s.NewBatchSession("test")
	t.Cleanup(func() {
		session.Close()
	})
	return tree.New(session, namespace)
}

func NewDatabase(t testing.TB, dbName string) database.Database {
	idgen := NewIDGen(t)
	s := NewStore(t)
	return database.New(dbName, s, idgen)
}
