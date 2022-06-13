package database

import (
	"context"
	"fmt"
	"github.com/octohelm/kiwidb/pkg/id"
	"github.com/octohelm/kiwidb/pkg/kv"
)

type Database interface {
	Execute(ctx context.Context, op Operator) error
	Table(tx Transaction, model any) (Table, error)
	Index(tx Transaction, model any, name string) (Index, error)
	Begin(optFns ...TransactionOptionFunc) Transaction
}

func New(dbName string, s kv.Store, gen id.Gen) Database {
	db := &database{
		name:  dbName,
		store: s,
		gen:   gen,
	}

	db.catalog = &catalog{db: db}
	return db
}

type database struct {
	name    string
	store   kv.Store
	gen     id.Gen
	catalog *catalog
}

type databaseTx struct {
	Op
	db *database
}

func (d *databaseTx) String() string {
	return fmt.Sprintf("Tx(db=%s)", d.db.name)
}

func (d *databaseTx) Iterate(in State, next func(state State) error) error {
	tx := d.db.Begin()

	in.SetTx(tx)
	in.SetDatabase(d.db)

	if err := next(in); err != nil {
		_ = tx.Rollback()
		return err
	}

	return tx.Commit()
}

func (d *database) Execute(ctx context.Context, op Operator) (err error) {
	c := NewStateWithContext(ctx)

	return Pipe(&databaseTx{db: d}, op).Iterate(c, func(out State) error {
		return nil
	})
}

func (d *database) Begin(optFns ...TransactionOptionFunc) Transaction {
	return NewTransaction(d.name, d.store, d.gen, optFns...)
}

func (d *database) Table(tx Transaction, model any) (Table, error) {
	s, err := d.catalog.TableSchema(model)
	if err != nil {
		return nil, err
	}
	return NewTable(tx, s)
}

func (d *database) Index(tx Transaction, model any, name string) (Index, error) {
	s, err := d.catalog.TableSchema(model)
	if err != nil {
		return nil, err
	}
	return NewIndex(tx, s.IndexSchema(name)), nil
}
