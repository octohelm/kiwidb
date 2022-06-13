package database

import (
	"context"

	"github.com/octohelm/kiwidb/pkg/dberr"
	"github.com/octohelm/kiwidb/pkg/kv"
	"github.com/pkg/errors"

	"github.com/octohelm/kiwidb/internal/tree"
	"github.com/octohelm/kiwidb/pkg/schema"
)

type Table interface {
	Insert(ctx context.Context, d Document) (tree.Key, Document, error)
	Delete(ctx context.Context, key tree.Key) error
	Replace(ctx context.Context, key tree.Key, d Document) error
	Get(ctx context.Context, key tree.Key) (Document, error)
	Range(ctx context.Context, rng tree.Range, reverse bool, fn func(key tree.Key, d Document) error) error
	Truncate(ctx context.Context) error
}

func NewTable(tx Transaction, s *schema.TableSchema) (Table, error) {
	return &table{
		tx:     tx,
		tree:   tree.New(tx.Session(), tree.Namespace(s.ID)),
		schema: s,
	}, nil
}

type table struct {
	tx     Transaction
	tree   *tree.Tree
	schema *schema.TableSchema
}

func (t *table) Truncate(ctx context.Context) error {
	return t.tree.Truncate()
}

func (t *table) Insert(ctx context.Context, d Document) (tree.Key, Document, error) {
	pk := d.PrimaryKey()
	if pk == 0 {
		i, err := t.tx.ID()
		if err != nil {
			return nil, nil, err
		}
		pk = i
		d.SetPrimaryKey(pk)
	}

	enc, err := d.Marshal()
	if err != nil {
		return nil, nil, err
	}

	key := tree.NewKey(pk)

	if err := t.tree.Insert(key, enc); err != nil {
		if errors.Is(err, kv.ErrKeyAlreadyExists) {
			return nil, nil, &dberr.ConflictError{
				Name: "pk",
				Key:  key,
			}
		}
	}
	return key, d, nil
}

func (t *table) Get(ctx context.Context, key tree.Key) (Document, error) {
	data, err := t.tree.Get(key)
	if err != nil {
		if errors.Is(err, kv.ErrKeyNotFound) {
			return nil, &dberr.NotFoundError{
				Name: string(key.Bytes()),
			}
		}
		return nil, err
	}
	return DocumentFromBytes(data), nil
}

func (t *table) Delete(ctx context.Context, key tree.Key) error {
	err := t.tree.Delete(key)
	if errors.Is(err, kv.ErrKeyNotFound) {
		return nil
	}
	return err
}

func (t *table) Replace(ctx context.Context, key tree.Key, d Document) error {
	ok, err := t.tree.Exists(key)
	if err != nil {
		return err
	}
	if !ok {
		return errors.Wrapf(dberr.NotFoundError{}, "can't replace key %v", key.Values())
	}

	enc, err := d.Marshal()
	if err != nil {
		return nil
	}

	// replace old document with new document
	err = t.tree.Put(key, enc)
	return err
}

func (t *table) Range(ctx context.Context, rng tree.Range, reverse bool, fn func(key tree.Key, d Document) error) error {
	return t.tree.Range(rng, reverse, func(k tree.Key, enc []byte) error {
		return fn(k, DocumentFromBytes(enc))
	})
}
