package database

import (
	"github.com/octohelm/kiwidb/pkg/encoding/msgp"
	"github.com/octohelm/kiwidb/pkg/schema"
)

type Document interface {
	Marshal() ([]byte, error)
	Unmarshal(v any) error

	Field(keyPath ...any) (Document, error)
	Value() any

	PrimaryKey() uint64
	SetPrimaryKey(uint64)
}

func DocumentFrom(v any) Document {
	return &doc{
		value: v,
	}
}

func DocumentFromBytes(b []byte) Document {
	return &doc{
		raw: b,
	}
}

type doc struct {
	value any
	raw   []byte
}

func (d *doc) SetPrimaryKey(id uint64) {
	if can, ok := d.value.(schema.CanPrimaryKey); ok {
		can.SetPrimaryKey(id)
		d.raw = nil
		return
	}
	raw, err := d.Marshal()
	if err != nil {
		return
	}

	valueRaw, _ := msgp.Set(raw, []any{"id"}, func(current []byte) ([]byte, error) {
		return msgp.Marshal(id)
	})

	d.raw = valueRaw
	return
}

func (d *doc) PrimaryKey() uint64 {
	if can, ok := d.value.(schema.CanPrimaryKey); ok {
		return can.PrimaryKey()
	}
	id, err := d.Field("id")
	if err != nil {
		return 0
	}
	return id.Value().(uint64)
}

func (d *doc) Field(keyPath ...any) (Document, error) {
	raw, err := d.Marshal()
	if err != nil {
		return nil, err
	}

	valueRaw, err := msgp.Get(raw, keyPath)
	if err != nil {
		return nil, err
	}

	return DocumentFromBytes(valueRaw), nil
}

func (d *doc) Marshal() ([]byte, error) {
	if d.raw != nil {
		return d.raw, nil
	}
	raw, err := msgp.Marshal(d.value)
	if err != nil {
		return nil, err
	}
	d.raw = raw
	return d.raw, nil
}

func (d *doc) Value() any {
	if d.value != nil {
		return d.value
	}

	if err := msgp.Unmarshal(d.raw, &d.value); err != nil {
		panic(err)
	}

	return d.value
}

func (d *doc) Unmarshal(v any) error {
	if d.raw != nil {
		return msgp.Unmarshal(d.raw, v)
	}
	return nil
}
