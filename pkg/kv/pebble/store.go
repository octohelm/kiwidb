package pebble

import (
	"context"
	"sync"

	"github.com/go-logr/logr"
	"github.com/pkg/errors"

	"github.com/cockroachdb/pebble"
	"github.com/octohelm/kiwidb/pkg/kv"
)

type store struct {
	db   *pebble.DB
	opts kv.Options

	// holds the shared snapshot read by all the read sessions
	// when a write session is open.
	// when no write session is open, the snapshot is nil
	// and every read session will use db.NewSnapshot()
	sharedSnapshot struct {
		sync.RWMutex

		snapshot *snapshot
	}
}

func (s *store) Shutdown(ctx context.Context) error {
	defer func() {
		if err := s.db.Close(); err != nil {
			logr.FromContextOrDiscard(ctx).Error(err, "Close")
		}
	}()

	// To make sure mem data write to disk
	f, err := s.db.AsyncFlush()
	if err != nil {
		return errors.Wrap(err, "AsyncFlush")
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-f:
			logr.FromContextOrDiscard(ctx).Info("Flushed")
			return nil
		}
	}
}

func NewStore(db *pebble.DB, opts kv.Options) kv.Store {
	if opts.MaxBatchSize <= 0 {
		opts.MaxBatchSize = defaultMaxBatchSize
	}
	return &store{
		db:   db,
		opts: opts,
	}
}

func (s *store) NewSnapshotSession(dbName string) kv.Session {
	var sn *snapshot

	// if there is a shared snapshot, use it.
	s.sharedSnapshot.RLock()
	sn = s.sharedSnapshot.snapshot

	// if there is no shared snapshot, create one.
	if sn == nil {
		sn = &snapshot{
			snapshot: s.db.NewSnapshot(),
			refCount: 1,
		}
	} else {
		// if there is a shared snapshot, increment the ref count.
		sn.Incr()
	}

	s.sharedSnapshot.RUnlock()

	return &SnapshotSession{
		store:    s,
		Snapshot: sn,
	}
}

func (s *store) NewBatchSession(dbName string) kv.Session {
	// before creating a batch session, create a shared snapshot
	// at this point-in-time.
	s.lockSharedSnapshot()
	b := s.db.NewIndexedBatch()

	return &BatchSession{
		store:        s,
		DB:           s.db,
		Batch:        b,
		maxBatchSize: s.opts.MaxBatchSize,
	}
}

func (s *store) lockSharedSnapshot() {
	s.sharedSnapshot.Lock()
	s.sharedSnapshot.snapshot = &snapshot{
		snapshot: s.db.NewSnapshot(),
		refCount: 1,
	}
	s.sharedSnapshot.Unlock()
}

func (s *store) unlockSharedSnapshot() {
	s.sharedSnapshot.Lock()
	s.sharedSnapshot.snapshot.Done()
	s.sharedSnapshot.snapshot = nil
	s.sharedSnapshot.Unlock()
}
