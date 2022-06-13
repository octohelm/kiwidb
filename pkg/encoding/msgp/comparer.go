package msgp

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
)

func Equal(a, b []byte) bool {
	return bytes.Equal(a, b)
}

func Compare(a, b []byte) int {
	var n, cmp int

	for {
		if n == len(a) {
			if len(b) == n {
				return 0
			}
			return -1
		} else if n == len(b) {
			return 1
		}

		a = a[n:]
		b = b[n:]

		cmp, n = compareNextValue(a, b)
		if cmp != 0 {
			return cmp
		}
	}
}

func compareNextValue(a, b []byte) (cmp int, n int) {
	if len(a) == 0 || len(b) == 0 {
		if len(a) == 0 && len(b) == 0 {
			return 0, 0
		}

		if len(a) == 0 {
			return -1, 0
		}

		return 1, 0
	}

	// compare the type first
	cmp = int(a[0]) - int(b[0])
	if cmp != 0 {
		return cmp, 1
	}

	// then compare values
	switch a[0] {
	case nullValue, falseValue, trueValue:
		fallthrough
	case 0: // tombstone
		return 0, 1
	}

	// deal with empty values
	if len(a) == 1 || len(b) == 1 {
		if len(a) == 1 && len(b) > 1 {
			return -1, 1
		}

		if len(a) > 1 && len(b) == 1 {
			return 1, 1
		}

		return 0, 1
	}

	// compare non empty values
	switch a[0] {
	case int64Value, uint64Value, float64Value:
		return bytes.Compare(a[1:9], b[1:9]), 9
	case int32Value, uint32Value, float32Value:
		return bytes.Compare(a[1:5], b[1:5]), 5
	case int16Value, uint16Value:
		return bytes.Compare(a[1:3], b[1:3]), 3
	case int8Value, uint8Value:
		return bytes.Compare(a[1:2], b[1:2]), 2
	case str8Value, str16Value, str32Value, bin8Value, bin16Value, bin32Value:
		l, n := binary.Uvarint(a[1:])
		n++
		enda := n + int(l)
		l, n = binary.Uvarint(b[1:])
		n++
		endb := n + int(l)
		return bytes.Compare(a[n:enda], b[n:endb]), enda
	case array16Value, array32Value:
		la, _ := binary.Uvarint(a[1:])
		lb, n := binary.Uvarint(b[1:])
		minl := la
		if lb < minl {
			minl = lb
		}
		n++
		for i := 0; i < int(minl); i++ {
			cmp, nn := compareNextValue(a[n:], b[n:])
			n += nn
			if cmp != 0 {
				return cmp, n
			}
		}
		if la < lb {
			return -1, n
		}
		if la > lb {
			return 1, n
		}

		return 0, n
	case map16Value, map32Value:
		la, _ := binary.Uvarint(a[1:])
		lb, n := binary.Uvarint(b[1:])
		minl := la
		if lb < minl {
			minl = lb
		}
		n++
		for i := 0; i < int(minl); i++ {
			// compare field
			cmp, nn := compareNextValue(a[n:], b[n:])
			n += nn
			if cmp != 0 {
				return cmp, n
			}

			// compare value
			cmp, nn = compareNextValue(a[n:], b[n:])
			n += nn
			if cmp != 0 {
				return cmp, n
			}
		}
		if la < lb {
			return -1, n
		}
		if la > lb {
			return 1, n
		}

		return 0, n
	}

	panic(fmt.Sprintf("unsupported value type: %d", a[0]))
}

func Successor(dst, a []byte) []byte {
	if len(a) == 0 {
		return a
	}
	namespace := binary.BigEndian.Uint64(a[1:])
	if namespace == math.MaxUint64 {
		return a
	}
	return write8(dst, uint64Value, namespace)
}

func AbbreviatedKey(key []byte) uint64 {
	if len(key) == 0 {
		return 0
	}

	var abbv uint64

	// get the namespace
	namespace, n := binary.BigEndian.Uint64(key[1:]), 9
	key = key[n:]
	if namespace >= 1<<16 {
		return math.MaxUint16 << 48
	}

	// First 16 bits are the namespace. (64 - 16 = 48)
	abbv |= namespace << 48

	if len(key) == 0 {
		return abbv
	}

	// Get a sorted int value from the key.
	// The type is encoded on 8 bits
	tn := key[0]

	// Set the type. (48 - 8 = 40)
	abbv |= uint64(tn) << 40

	abbv |= abbreviatedValue(key)
	return abbv
}

// return the abbreviated value of the first value on max 5 bytes.
func abbreviatedValue(key []byte) uint64 {
	if len(key) == 0 {
		return 0
	}

	switch key[0] {
	case nullValue:
		return 0
	case trueValue, falseValue:
		return 0
	case uint8Value, int8Value:
		x := key[1]
		return uint64(x)
	case uint16Value, int16Value:
		return uint64(binary.BigEndian.Uint16(key[1:]))
	case uint32Value, int32Value:
		return uint64(binary.BigEndian.Uint32(key[1:]))
	case uint64Value, int64Value, float64Value:
		x := binary.BigEndian.Uint64(key[1:])
		return x >> 24
	case str8Value, str16Value, str32Value, bin8Value, bin16Value, bin32Value:
		var abbv uint64
		l, n := binary.Uvarint(key[1:])
		n++
		key = key[n:]
		ll := int(l)
		// put the first 5 bytes of the value
		for i := 0; i < 5 && i < ll; i++ {
			abbv |= uint64(key[i]) << (32 - uint64(i)*8)
		}
		return abbv
	case array16Value, array32Value, map16Value, map32Value:
		key = key[1:]
		l, n := binary.Uvarint(key)
		key = key[n:]
		if l > 0 {
			switch key[0] {
			case array16Value, array32Value, map16Value, map32Value:
				return uint64(key[0]) << 32
			default:
				abbv := uint64(key[0]) << 32
				x := abbreviatedValue(key) >> 8
				return abbv | x
			}
		}

		return 0
	}

	return 0
}
