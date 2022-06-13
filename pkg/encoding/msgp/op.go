package msgp

import (
	"encoding/binary"
	"fmt"
	"io"

	"github.com/pkg/errors"
)

var ErrKeyPathNotExists = errors.New("key path is not found")

func Get(b []byte, keyPath []any) ([]byte, error) {
	if len(keyPath) == 0 {
		return b, nil
	}

	s := &scanner{b: b}
	err := s.scan(0, keyPath)
	if err != nil {
		if !errors.Is(err, io.EOF) {
			return nil, err
		}
	}
	if s.found == nil {
		return nil, ErrKeyPathNotExists
	}
	return s.found.read(b), nil
}

func Set(b []byte, keyPath []any, replace func(current []byte) ([]byte, error)) ([]byte, error) {
	if len(keyPath) == 0 {
		return b, nil
	}

	s := &scanner{b: b}
	err := s.scan(0, keyPath)
	if err != nil {
		if !errors.Is(err, io.EOF) {
			return nil, err
		}
	}
	if s.found == nil {
		return nil, ErrKeyPathNotExists
	}

	r, err := replace(s.found.read(b))
	if err != nil {
		return nil, err
	}

	final := make([]byte, 0, len(r)-s.found.n+len(r))
	final = append(final, b[0:s.found.off]...)
	final = append(final, r...)
	final = append(final, b[s.found.next():]...)

	return final, nil
}

type scanner struct {
	b     []byte
	found *seek
}

func (s *scanner) scan(off int, keyPath []any) error {
	if len(keyPath) == 0 {
		return nil
	}

	code, err := s.seek(off, 1, 0, 0)
	if err != nil {
		return err
	}

	typ := code.read(s.b)[0]

	switch typ {
	case map16Value, map32Value:
		n := 2
		lenTyp := uint16Value
		if typ == map32Value {
			n = 4
			lenTyp = uint32Value
		}
		p, err := s.seek(off+1, n, lenTyp, 0)
		if err != nil {
			return err
		}

		l := getLen(p.read(s.b))

		for i := 0; i < int(l); i++ {
			// key
			p, err = s.seekValue(p.next())
			if err != nil {
				return err
			}

			// unmarshal key value
			key := p.read(s.b)
			var v any
			if err := Unmarshal(key, &v); err != nil {
				return err
			}

			if v == keyPath[0] {
				// last key path
				if len(keyPath) == 1 {
					p, err := s.seekValue(p.next())
					if err != nil {
						return nil
					}
					s.found = &p
					return nil
				}

				if err := s.scan(p.next(), keyPath[1:]); err != nil {
					return err
				}
				break
			}

			// value
			p, err = s.seekValue(p.next())
			if err != nil {
				return err
			}
		}
		return nil
	case array16Value, array32Value:
		if idx, ok := keyPath[0].(int); ok {
			n := 2
			lenTyp := uint16Value
			if typ == map32Value {
				n = 4
				lenTyp = uint32Value
			}

			p, err := s.seek(off+1, n, lenTyp, 0)
			if err != nil {
				return err
			}

			l := getLen(p.read(s.b))

			for i := 0; i < int(l); i++ {
				if i == idx {
					// last key path
					if len(keyPath) == 1 {
						p, err := s.seekValue(p.next())
						if err != nil {
							return nil
						}
						s.found = &p
						return nil
					}

					if err := s.scan(p.next(), keyPath[1:]); err != nil {
						return err
					}
					break
				}

				if i > idx {
					// skip
					break
				}

				// value
				p, err = s.seekValue(p.next())
				if err != nil {
					return err
				}
			}
		}
	default:
		return fmt.Errorf("%s is not map or array value", keyPath)
	}

	return nil
}

type seek struct {
	off  int
	n    int
	typ  byte
	size int
}

func (s seek) withType(typ byte) seek {
	s.typ = typ
	return s
}

func (s seek) read(b []byte) []byte {
	return b[s.off : s.off+s.n]
}

func (s seek) next() int {
	return s.off + s.n
}

func (s *scanner) seekValue(off int) (seek, error) {
	code, err := s.seek(off, 1, 0, 0)
	if err != nil {
		return seek{}, err
	}

	typ := code.read(s.b)[0]

	switch typ {
	case nullValue:
		return code.withType(typ), nil
	case trueValue:
		return code.withType(typ), nil
	case falseValue:
		return code.withType(typ), nil
	case int8Value:
		return s.seek(off, 1+1, typ, 0)
	case int16Value:
		return s.seek(off, 1+2, typ, 0)
	case int32Value:
		return s.seek(off, 1+4, typ, 0)
	case int64Value:
		return s.seek(off, 1+8, typ, 0)
	case uint8Value:
		return s.seek(off, 1+1, typ, 0)
	case uint16Value:
		return s.seek(off, 1+2, typ, 0)
	case uint32Value:
		return s.seek(off, 1+4, typ, 0)
	case uint64Value:
		return s.seek(off, 1+8, typ, 0)
	case float32Value:
		return s.seek(off, 1+4, typ, 0)
	case float64Value:
		return s.seek(off, 1+8, typ, 0)
	case bin8Value, str8Value:
		lp, err := s.seek(off+1, 1, uint8Value, 0)
		if err != nil {
			return seek{}, err
		}
		l := getLen(lp.read(s.b))
		return s.seek(off, 1+1+int(l), typ, int(l))
	case bin16Value, str16Value:
		lp, err := s.seek(off+1, 2, uint16Value, 0)
		if err != nil {
			return seek{}, err
		}
		l := getLen(lp.read(s.b))
		return s.seek(off, 1+2+int(l), typ, int(l))
	case bin32Value, str32Value:
		p, err := s.seek(off+1, 4, uint32Value, 0)
		if err != nil {
			return seek{}, err
		}
		l := getLen(p.read(s.b))
		return s.seek(off, 1+4+int(l), typ, int(l))
	case array16Value, array32Value:
		n := 2
		lt := uint16Value
		if typ == array32Value {
			n = 4
			lt = uint32Value
		}
		p, err := s.seek(off+1, n, lt, 0)
		if err != nil {
			return seek{}, err
		}
		l := getLen(p.read(s.b))
		for i := 0; i < int(l); i++ {
			p, err = s.seekValue(p.next())
			if err != nil {
				return seek{}, err
			}
			n = n + p.n
		}
		return s.seek(off, 1+int(n), typ, int(l))
	case map16Value, map32Value:
		n := 2
		lt := uint16Value
		if typ == map32Value {
			n = 4
			lt = uint32Value
		}
		p, err := s.seek(off+1, n, lt, 0)
		if err != nil {
			return seek{}, err
		}
		l := getLen(p.read(s.b))
		for i := 0; i < int(l); i++ {
			// key
			p, err = s.seekValue(p.next())
			if err != nil {
				return seek{}, err
			}
			n = n + p.n
			// value
			p, err = s.seekValue(p.next())
			if err != nil {
				return seek{}, err
			}
			n = n + p.n
		}
		return s.seek(off, 1+int(n), map16Value, int(l))
	}

	return seek{}, fmt.Errorf("unsupported type %x", typ)
}

func (s *scanner) seek(start, n int, typ byte, size int) (seek, error) {
	if start+n > len(s.b) {
		return seek{}, io.EOF
	}
	return seek{off: start, n: n, typ: typ, size: size}, nil
}

func getLen(b []byte) uint64 {
	switch len(b) {
	case 8:
		return binary.BigEndian.Uint64(b)
	case 4:
		return uint64(binary.BigEndian.Uint32(b))
	case 2:
		return uint64(binary.BigEndian.Uint16(b))
	default:
		return uint64(b[0])
	}
}
