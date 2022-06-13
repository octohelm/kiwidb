package schema

import (
	"bytes"
	"strconv"
	"strings"
)

func ParseKeyPath(s string) (KeyPath, error) {
	p := KeyPath{}

	buf := bytes.NewBuffer(nil)

	var appendPath = func(parse func(v string) (any, error)) error {
		if parse != nil {
			v, err := parse(buf.String())
			if err != nil {
				return err
			}
			p = append(p, v)
		} else {
			p = append(p, buf.String())
		}
		buf.Reset()
		return nil
	}

	for i := range s {
		b := s[i]

		switch b {
		case '[':
			// skip
		case ']':
			if err := appendPath(func(v string) (any, error) {
				return strconv.ParseInt(v, 10, 64)
			}); err != nil {
				return nil, err
			}
		case '.':
			_ = appendPath(nil)
		default:
			buf.WriteByte(b)
		}
	}

	if buf.Len() > 0 {
		_ = appendPath(nil)
	}

	return p, nil
}

type KeyPath []any

func (p KeyPath) String() string {
	var b strings.Builder

	for i := range p {
		switch x := p[i].(type) {
		case string:
			if i != 0 {
				b.WriteRune('.')
			}
			b.WriteString(x)
		case int:
			b.WriteString("[" + strconv.Itoa(x) + "]")
		}
	}

	return b.String()
}

// IsEqual returns whether other is equal to p.
func (p KeyPath) IsEqual(other KeyPath) bool {
	if len(other) != len(p) {
		return false
	}

	for i := range p {
		if other[i] != p[i] {
			return false
		}
	}

	return true
}
