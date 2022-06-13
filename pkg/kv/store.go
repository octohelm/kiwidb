package kv

import "context"

type Store interface {
	NewSnapshotSession(dbName string) Session
	NewBatchSession(dbName string) Session
	Shutdown(ctx context.Context) error
}
