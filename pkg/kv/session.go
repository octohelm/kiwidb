package kv

type CommitOptionFunc = func(opt *CommitOption)

var NoSync = func(opt *CommitOption) {
	opt.NoSync = true
}

type CommitOption struct {
	NoSync bool
}

type Session interface {
	// Insert inserts a key-value pair. If it already exists, it returns ErrKeyAlreadyExists.
	Insert(k, v []byte) error
	// Put stores a key-value pair. If it already exists, it overrides it.
	Put(k, v []byte) error
	// Get returns a value associated with the given key. If not found, returns ErrKeyNotFound.
	Get(k []byte) ([]byte, error)
	// Commit apply changes of batch
	Commit(opts ...CommitOptionFunc) error

	Close() error
	// Exists returns whether a key exists and is visible by the current session.
	Exists(k []byte) (bool, error)
	// Delete a record by key. If not found, returns ErrKeyNotFound.
	Delete(k []byte) error

	Iterator(start []byte, end []byte) Iterator
}

type Iterator interface {
	First() bool
	Next() bool

	Last() bool // reverse
	Prev() bool

	Valid() bool
	Error() error

	Key() []byte
	Value() []byte
	Close() error
}
