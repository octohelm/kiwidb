package kv

import "github.com/pkg/errors"

type Options struct {
	MaxBatchSize int
	Extra        map[string]string
}

type StoreEngine interface {
	New(opt Options) (Store, error)
}

var engines = map[string]StoreEngine{}

func RegisterEngine(engine string, store StoreEngine) {
	engines[engine] = store
}

func NewStore(engine string, opt Options) (Store, error) {
	if e, ok := engines[engine]; ok {
		return e.New(opt)
	}
	return nil, errors.Errorf("unknown engine %s", engine)
}
