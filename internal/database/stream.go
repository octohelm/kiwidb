package database

import (
	"context"
	"github.com/octohelm/kiwidb/internal/tree"
	"strings"
)

type Operator interface {
	Iterate(global State, next func(state State) error) error
	String() string

	Prev() Operator
	Next() Operator

	setPrev(prev Operator)
	setNext(next Operator)
}

type Receiver interface {
	Next(data []byte) error
}

func Pipe(operators ...Operator) Operator {
	if len(operators) == 0 {
		return nil
	}

	ops := make([]Operator, 0, len(operators))

	for i := range operators {
		if op := operators[i]; op != nil {
			ops = append(ops, op)
		}
	}

	for i := len(ops) - 1; i > 0; i-- {
		ops[i].setPrev(ops[i-1])
		ops[i-1].setNext(ops[i])
	}

	return ops[len(ops)-1]
}

type Op struct {
	prev Operator
	next Operator
}

func (op *Op) setPrev(o Operator) {
	op.prev = o
}

func (op *Op) setNext(o Operator) {
	op.next = o
}

func (op *Op) Prev() Operator {
	return op.prev
}

func (op *Op) Next() Operator {
	return op.next
}

func Stringify(o Operator) string {
	prev := o.Prev()
	for {
		p := prev.Prev()
		if p == nil {
			break
		}
		prev = p
	}

	b := strings.Builder{}

	for next := prev; next != nil; next = next.Next() {
		if next != prev {
			b.WriteString(" | ")
		}
		b.WriteString(next.String())
	}

	return b.String()
}

type State interface {
	Context() context.Context

	SetOuter(out State)

	Database() Database
	SetDatabase(db Database)

	SetTx(tx Transaction)
	Tx() Transaction

	Key() tree.Key
	SetKey(key tree.Key)

	Document() Document
	SetDocument(doc Document)
}

func NewStateWithContext(ctx context.Context) State {
	return &streamState{
		ctx: ctx,
	}
}

type streamState struct {
	ctx context.Context
	db  Database
	tx  Transaction
	key tree.Key
	doc Document
	out State
}

func (c *streamState) SetOuter(out State) {
	c.out = out
}

func (c *streamState) SetTx(tx Transaction) {
	c.tx = tx
}

func (c *streamState) Tx() Transaction {
	if tx := c.tx; tx != nil {
		return tx
	}
	if c.out != nil {
		return c.out.Tx()
	}
	return nil
}

func (c *streamState) Key() tree.Key {
	return c.key
}

func (c *streamState) SetKey(key tree.Key) {
	c.key = key
}

func (c *streamState) Context() context.Context {
	return c.ctx
}

func (c *streamState) SetDocument(doc Document) {
	c.doc = doc
}

func (c *streamState) Document() Document {
	return c.doc
}

func (c *streamState) Database() Database {
	if db := c.db; db != nil {
		return db
	}
	if c.out != nil {
		return c.out.Database()
	}
	return nil
}

func (c *streamState) SetDatabase(db Database) {
	c.db = db
}
