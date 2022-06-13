package pebble

import (
	"github.com/cockroachdb/pebble"
	"github.com/octohelm/kiwidb/pkg/kv"
	"github.com/pkg/errors"
)

var _ kv.Session = (*BatchSession)(nil)

const (
	// 10MB
	defaultMaxBatchSize = 10 * 1024 * 1024
)

var (
	tombStone = []byte{0}
)

type BatchSession struct {
	DB           *pebble.DB
	Batch        *pebble.Batch
	store        *store
	closed       bool
	maxBatchSize int
}

func (s *BatchSession) Commit(opts ...kv.CommitOptionFunc) error {
	if s.closed {
		return errors.New("already closed")
	}

	w := pebble.Sync

	opt := &kv.CommitOption{}
	for i := range opts {
		opts[i](opt)
	}

	if opt.NoSync {
		w = pebble.NoSync
	}

	err := s.Batch.Commit(w)
	if err != nil {
		return err
	}

	return s.Close()
}

func (s *BatchSession) Close() error {
	if s.closed {
		return errors.New("already closed")
	}
	s.closed = true
	s.store.unlockSharedSnapshot()
	return s.Batch.Close()
}

// Get returns a value associated with the given key. If not found, returns ErrKeyNotFound.
func (s *BatchSession) Get(k []byte) ([]byte, error) {
	return get(s.Batch, k)
}

// Exists returns whether a key exists and is visible by the current session.
func (s *BatchSession) Exists(k []byte) (bool, error) {
	return exists(s.Batch, k)
}

func (s *BatchSession) ensureBatchSize() error {
	if s.Batch.Len() < s.maxBatchSize {
		return nil
	}

	//// The batch is too large. Insert the rollback segments and commit the batch.
	//err := s.rollbackSegment.Apply(s.Batch)
	//if err != nil {
	//	return err
	//}

	// this is an intermediary commit that might be rolled back by the user
	// so we don't need durability here.
	err := s.Batch.Commit(pebble.NoSync)
	if err != nil {
		return err
	}

	// reset batch
	s.Batch.Reset()

	return nil
}

// Insert inserts a key-value pair. If it already exists, it returns ErrKeyAlreadyExists.
func (s *BatchSession) Insert(k, v []byte) error {
	if len(k) == 0 {
		return errors.New("cannot store empty key")
	}

	if len(v) == 0 {
		return errors.New("cannot store empty value")
	}

	ok, err := s.Exists(k)
	if err != nil {
		return err
	}
	if ok {
		return kv.ErrKeyAlreadyExists
	}

	//s.rollbackSegment.EnqueueOp(k, kvOpInsert)

	err = s.Batch.Set(k, v, nil)
	if err != nil {
		return err
	}

	return s.ensureBatchSize()
}

// Put stores a key value pair. If it already exists, it overrides it.
func (s *BatchSession) Put(k, v []byte) error {
	if len(k) == 0 {
		return errors.New("cannot store empty key")
	}

	if len(v) == 0 {
		return errors.New("cannot store empty value")
	}

	//s.rollbackSegment.EnqueueOp(k, kvOpSet)

	err := s.Batch.Set(k, v, nil)
	if err != nil {
		return err
	}

	return s.ensureBatchSize()
}

// Delete a record by key. If the key doesn't exist, it doesn't do anything.
func (s *BatchSession) Delete(k []byte) error {
	//s.rollbackSegment.EnqueueOp(k, kvOpDel)

	err := s.Batch.Delete(k, nil)
	if err != nil {
		return err
	}

	return s.ensureBatchSize()
}

func (s *BatchSession) Iterator(start []byte, end []byte) kv.Iterator {
	return s.Batch.NewIter(&pebble.IterOptions{
		LowerBound: start,
		UpperBound: end,
	})
}
