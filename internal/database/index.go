package database

import (
	"context"

	"github.com/octohelm/kiwidb/internal/tree"
	"github.com/octohelm/kiwidb/pkg/kv"
	"github.com/octohelm/kiwidb/pkg/schema"
	"github.com/pkg/errors"
)

type Index interface {
	Set(ctx context.Context, values []any, key tree.Key) error
	Exists(ctx context.Context, values []any) (bool, tree.Key, error)
	Delete(ctx context.Context, values []any, key tree.Key) error
	Range(ctx context.Context, rng tree.Range, reverse bool, fn func(key tree.Key) error) error
	Truncate(ctx context.Context) error
}

type index struct {
	schema *schema.IndexSchema
	tree   *tree.Tree
}

func NewIndex(tx Transaction, s *schema.IndexSchema) Index {
	return &index{
		tree:   tree.New(tx.Session(), tree.Namespace(s.ID)),
		schema: s,
	}
}

var errStop = errors.New("stop")

func (idx *index) Set(ctx context.Context, vs []any, key tree.Key) error {
	if key == nil {
		return errors.New("cannot index value without a key")
	}

	if len(vs) != len(idx.schema.Paths) {
		return errors.New("cannot index without enough values")
	}

	return idx.tree.Put(tree.NewKey(append(vs, key.Bytes())...), nil)
}

func (idx *index) Exists(ctx context.Context, vs []any) (bool, tree.Key, error) {
	if len(vs) != len(idx.schema.Paths) {
		return false, nil, errors.New("cannot index without enough values")
	}

	seek := tree.NewNamespacedKey(idx.tree.Namespace, vs...)

	var found bool
	var dKey tree.Key

	rng := tree.NewRange(seek, seek, false)

	err := idx.tree.Range(rng, false, func(k tree.Key, _ []byte) error {
		values := k.Values()
		if len(values) != 2 {
			return errors.Errorf("invalid index value %q", k)
		}
		if keyBytes, ok := values[len(values)-1].([]byte); ok {
			dKey = tree.NewEncodedKey(keyBytes)
			found = true
			return errStop
		}
		return errors.Errorf("invalid index value %q", k)
	})
	if err == errStop {
		err = nil
	}
	return found, dKey, err
}

func (idx *index) Delete(ctx context.Context, vs []any, key tree.Key) error {
	if len(vs) != len(idx.schema.Paths) {
		return errors.New("cannot index without enough values")
	}

	err := idx.tree.Delete(tree.NewKey(append(vs, key.Bytes())...))
	if errors.Is(err, kv.ErrKeyNotFound) {
		return nil
	}
	return err
}

func (idx *index) Range(ctx context.Context, rng tree.Range, reverse bool, fn func(key tree.Key) error) error {
	return idx.iterateOnRange(ctx, rng, reverse, func(itmKey, key tree.Key) error {
		return fn(key)
	})
}

func (idx *index) iterateOnRange(ctx context.Context, rng tree.Range, reverse bool, fn func(itmKey tree.Key, key tree.Key) error) error {
	return idx.tree.Range(rng, reverse, idx.iterator(ctx, fn))
}

func (idx *index) iterator(ctx context.Context, fn func(itmKey tree.Key, key tree.Key) error) func(k tree.Key, d []byte) error {
	return func(k tree.Key, _ []byte) error {
		values := k.Values()
		pk := tree.NewEncodedKey(values[len(values)-1].([]byte))
		return fn(k, pk)
	}
}

func (idx *index) Truncate(ctx context.Context) error {
	return idx.tree.Truncate()
}
