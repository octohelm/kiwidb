package tree

import (
	"github.com/octohelm/kiwidb/pkg/encoding/msgp"
	"github.com/octohelm/kiwidb/pkg/kv"
)

type Namespace uint64

func New(session kv.Session, ns Namespace) *Tree {
	return &Tree{
		Namespace: ns,
		Session:   session,
	}
}

type Tree struct {
	Namespace Namespace
	Session   kv.Session
}

var defaultValue = []byte{0}

func (t *Tree) Insert(key Key, value []byte) error {
	if len(value) == 0 {
		value = defaultValue
	}
	return t.Session.Insert(key.WithNamespace(t.Namespace).Bytes(), value)
}

func (t *Tree) Put(key Key, value []byte) error {
	if len(value) == 0 {
		value = defaultValue
	}
	return t.Session.Put(key.WithNamespace(t.Namespace).Bytes(), value)
}

func (t *Tree) Get(key Key) ([]byte, error) {
	return t.Session.Get(key.WithNamespace(t.Namespace).Bytes())
}

func (t *Tree) Exists(key Key) (bool, error) {
	return t.Session.Exists(key.WithNamespace(t.Namespace).Bytes())
}

func (t *Tree) Delete(key Key) error {
	return t.Session.Delete(key.WithNamespace(t.Namespace).Bytes())
}

func (t *Tree) Truncate() error {
	from := NewNamespacedKey(t.Namespace).Bytes()
	to := NewNamespacedKey(t.Namespace + 1).Bytes()

	s := t.Session.Iterator(from, to)
	defer s.Close()

	for s.First(); s.Valid(); s.Next() {
		t.Session.Delete(s.Key())
	}

	return t.Session.Commit()
}

func (t *Tree) Range(rng Range, reverse bool, fn func(key Key, value []byte) error) error {
	if rng == nil {
		rng = NewRange(nil, nil, false)
	}

	var start, end []byte

	min := rng.Min()
	max := rng.Max()

	exclusive := rng.Exclusive()

	if !exclusive {
		if min == nil {
			start = t.buildMinKeyForType(max)
		} else {
			start = t.buildStartKeyInclusive(min)
		}
		if max == nil {
			end = t.buildMaxKeyForType(min)
		} else {
			end = t.buildEndKeyInclusive(max)
		}
	} else {
		if min == nil {
			start = t.buildMinKeyForType(max)
		} else {
			start = t.buildStartKeyExclusive(min)
		}
		if max == nil {
			end = t.buildMaxKeyForType(min)
		} else {
			end = t.buildEndKeyExclusive(max)
		}
	}

	//fmt.Printf("%x %x\n", start, end)

	it := t.Session.Iterator(start, end)
	defer it.Close()

	if !reverse {
		it.First()
	} else {
		it.Last()
	}

	var k Key
	for it.Valid() {
		k = NewEncodedKey(it.Key())

		err := fn(k, it.Value())
		if err != nil {
			return err
		}

		if !reverse {
			it.Next()
		} else {
			it.Prev()
		}
	}

	return it.Error()
}

func (t *Tree) buildMinKeyForType(max Key) []byte {
	if max == nil {
		return t.buildFirstKey()
	}

	if len(max.Values()) == 1 {
		return NewNamespacedKey(t.Namespace, msgp.MinValueForType(max.Values()[0])).Bytes()
	}

	var values []any

	for i := range max.Values() {
		if i < len(max.Values())-1 {
			values = append(values, max.Values()[i])
			continue
		}
		values = append(values, msgp.MinValueForType(max.Values()[i]))
	}

	return NewNamespacedKey(t.Namespace, values...).Bytes()
}

func (t *Tree) buildFirstKey() []byte {
	return NewNamespacedKey(t.Namespace).Bytes()
}

func (t *Tree) buildMaxKeyForType(min Key) []byte {
	if min == nil {
		return t.buildLastKey()
	}
	if len(min.Values()) == 1 {
		return append(NewKey().WithNamespace(t.Namespace).Bytes(), msgp.MaxTypeCodeForType(min.Values()[0]))
	}
	return append(NewNamespacedKey(t.Namespace, min.Values()[:]...).Bytes(), msgp.MaxTypeCodeForType(min.Values()[len(min.Values())-1]))
}

func (t *Tree) buildLastKey() []byte {
	return append(NewNamespacedKey(t.Namespace).Bytes(), 0xFF)
}

func (t *Tree) buildStartKeyInclusive(key Key) []byte {
	return key.WithNamespace(t.Namespace).Bytes()
}

func (t *Tree) buildStartKeyExclusive(key Key) []byte {
	return append(key.WithNamespace(t.Namespace).Bytes(), 0xFF) // should this 0x00 ?
}

func (t *Tree) buildEndKeyInclusive(key Key) []byte {
	return append(key.WithNamespace(t.Namespace).Bytes(), 0xFF)
}

func (t *Tree) buildEndKeyExclusive(key Key) []byte {
	return key.WithNamespace(t.Namespace).Bytes()
}

type Range interface {
	Min() Key
	Max() Key
	Exclusive() bool
}

func NewRange(min Key, max Key, exclusive bool) Range {
	return &rng{min: min, max: max, exclusive: exclusive}
}

type rng struct {
	min       Key
	max       Key
	exclusive bool
}

func (r *rng) Min() Key {
	return r.min
}

func (r *rng) Max() Key {
	return r.max
}

func (r *rng) Exclusive() bool {
	return r.exclusive
}
