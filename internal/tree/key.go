package tree

import (
	"bytes"
	"io"

	"github.com/octohelm/kiwidb/pkg/encoding/msgp"
)

type Key interface {
	WithNamespace(ns Namespace) Key
	Values() []any
	Bytes() []byte
}

func NewNamespacedKey(ns Namespace, values ...any) Key {
	return &key{
		ns:     ns,
		values: values,
	}
}

func NewKey(values ...any) Key {
	return &key{
		values: values,
	}
}

func NewEncodedKey(enc []byte) Key {
	return &key{
		raw: enc,
	}
}

type key struct {
	ns     Namespace
	values []any
	raw    []byte
}

func (k key) WithNamespace(ns Namespace) Key {
	if k.ns != ns {
		k.ns = ns
		// when namespace not equal should remove raw
		k.raw = nil
	}
	return &k
}

func (k *key) Values() []any {
	if k.values != nil {
		return k.values
	}

	buf := bytes.NewBuffer(k.raw[:])

	dec := msgp.NewDecoder(buf)

	// skip namespace
	if err := dec.Decode(&k.ns); err != nil {
		panic(err)
	}

	k.values = nil

	for {
		var v any
		if err := dec.Decode(&v); err != nil {
			if err == io.EOF {
				break
			}
			panic(err)
		}
		k.values = append(k.values, v)
	}

	return k.values
}

func (k *key) Bytes() []byte {
	if k.raw != nil {
		return k.raw
	}
	buf := bytes.NewBuffer(nil)
	enc := msgp.NewEncoder(buf)

	if err := enc.Encode(k.ns); err != nil {
		panic(err)
	}

	for i := range k.values {
		if err := enc.Encode(k.values[i]); err != nil {
			panic(err)
		}
	}

	k.raw = buf.Bytes()
	return k.raw
}
