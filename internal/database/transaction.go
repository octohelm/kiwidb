package database

import (
	"github.com/octohelm/kiwidb/pkg/id"
	"github.com/octohelm/kiwidb/pkg/kv"
	"github.com/pkg/errors"
)

type Transaction interface {
	ID() (uint64, error)
	Session() kv.Session

	Rollback() error
	Commit() error
	On(event TransactionEvent, callback func())
}

type TransactionEvent string

var (
	TransactionEventCommit   TransactionEvent = "commit"
	TransactionEventRollback TransactionEvent = "rollback"
)

type TransactionOptionFunc = func(o *transactionOption)

type transactionOption struct {
	readOnly bool
}

func TransactionReadOnly() func(o *transactionOption) {
	return func(o *transactionOption) {
		o.readOnly = true
	}
}

func NewTransaction(dbName string, s kv.Store, idgen id.Gen, optFns ...TransactionOptionFunc) Transaction {
	o := &transactionOption{}

	for i := range optFns {
		optFns[i](o)
	}

	if o.readOnly {
		return &transaction{
			gen:      idgen,
			readOnly: true,
			session:  s.NewSnapshotSession(dbName),
			hooks:    map[TransactionEvent][]func(){},
		}
	}
	return &transaction{
		gen:      idgen,
		readOnly: false,
		session:  s.NewBatchSession(dbName),
		hooks:    map[TransactionEvent][]func(){},
	}
}

type transaction struct {
	gen      id.Gen
	session  kv.Session
	hooks    map[TransactionEvent][]func()
	readOnly bool
}

func (tx *transaction) ID() (uint64, error) {
	return tx.gen.ID()
}

func (tx *transaction) Session() kv.Session {
	return tx.session
}

func (tx *transaction) On(event TransactionEvent, callback func()) {
	tx.hooks[event] = append(tx.hooks[event], callback)
}

func (tx *transaction) Rollback() error {
	err := tx.session.Close()
	if err != nil {
		return err
	}

	if hooks, ok := tx.hooks[TransactionEventRollback]; ok {
		for i := len(hooks) - 1; i >= 0; i-- {
			hooks[i]()
		}
	}

	return nil
}

func (tx *transaction) Commit() error {
	if tx.readOnly {
		return errors.New("cannot commit read-only transaction")
	}

	err := tx.session.Commit()
	if err != nil {
		return err
	}

	_ = tx.session.Close()

	if hooks, ok := tx.hooks[TransactionEventCommit]; ok {
		for i := len(hooks) - 1; i >= 0; i-- {
			hooks[i]()
		}
	}
	return nil
}
