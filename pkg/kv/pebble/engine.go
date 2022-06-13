package pebble

import (
	"errors"
	"fmt"
	"os"

	"github.com/octohelm/kiwidb/pkg/encoding/msgp"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/octohelm/kiwidb/pkg/kv"
)

func init() {
	kv.RegisterEngine("pebble", &engine{})
}

type engine struct {
}

func (e engine) New(opt kv.Options) (kv.Store, error) {
	var opts pebble.Options

	path, ok := opt.Extra["path"]
	if !ok {
		return nil, errors.New("engine pebble need `path`")
	}

	if path == ":memory:" {
		opts.FS = vfs.NewMem()
		path = ""
	}

	pdb, err := Open(path, &opts)
	if err != nil {
		return nil, err
	}

	return NewStore(pdb, opt), nil
}

// Open a database with a custom comparer.
func Open(path string, opts *pebble.Options) (*pebble.DB, error) {
	if opts == nil {
		opts = &pebble.Options{}
	}
	if opts.Comparer == nil {
		opts.Comparer = DefaultComparer
	}
	return pebble.Open(path, opts)
}

type DB = pebble.DB

var DefaultComparer = &pebble.Comparer{
	// This name is part of the C++ Level-DB implementation's default file
	// format, and should not be changed.
	Name:           "leveldb.BytewiseComparator",
	FormatKey:      pebble.DefaultComparer.FormatKey,
	Separator:      pebble.DefaultComparer.Separator,
	Compare:        msgp.Compare,
	Equal:          msgp.Equal,
	AbbreviatedKey: msgp.AbbreviatedKey,
	Successor:      msgp.Successor,
}

func EnsureDirectory(dir string) error {
	info, err := os.Stat(dir)
	if os.IsNotExist(err) {
		return os.MkdirAll(dir, 0o777)
	} else if err == nil && !info.IsDir() {
		return fmt.Errorf("not a directory: %s", dir)
	}
	return err
}
