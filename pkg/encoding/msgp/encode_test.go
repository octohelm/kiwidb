package msgp

import (
	"bytes"
	"fmt"
	"math"
	"reflect"
	"strings"
	"testing"

	textingx "github.com/octohelm/x/testing"
)

func TestMarshalUnmarshal(t *testing.T) {
	t.Run("struct", func(t *testing.T) {
		type Anonymous struct {
			S string
		}

		tests := []struct {
			input any
			want  []byte
		}{
			{
				struct {
					I8 int8
				}{
					I8: 1,
				},
				makeSizedValue(
					map16Value, 1,
					makeText("I8"), makeLit[int8](1),
				),
			},
			{
				struct {
					I8 int8
					Anonymous
				}{
					I8: 1,
					Anonymous: Anonymous{
						S: "1",
					},
				},
				makeSizedValue(
					map16Value, 2,
					makeText("I8"), makeLit[int8](1),
					makeText("S"), makeText("1"),
				),
			},
		}

		for _, test := range tests {
			t.Run(fmt.Sprintf("%v", test.input), func(t *testing.T) {
				got, err := Marshal(test.input)
				textingx.Expect(t, err, textingx.Be[error](nil))
				textingx.Expect(t, got, textingx.Equal(test.want))

				rv := reflect.New(reflect.TypeOf(test.input))
				err = Unmarshal(got, rv)
				textingx.Expect(t, err, textingx.Be[error](nil))
				textingx.Expect(t, rv.Elem().Interface(), textingx.Equal(test.input))
			})
		}
	})

	t.Run("object", func(t *testing.T) {
		tests := []struct {
			input map[string]any
			want  []byte
		}{
			{
				map[string]any{},
				makeSizedValue(map16Value, 0),
			},
			{
				map[string]any{
					"a": int8(1),
				},
				makeSizedValue(
					map16Value, 1,
					makeText("a"), makeLit[int8](1),
				),
			},
			{
				map[string]any{
					"a": map[string]any{
						"b": int8(1),
					},
					"c": int32(1),
				},
				makeSizedValue(
					map16Value, 2,
					makeText("a"), makeSizedValue(
						map16Value, 1,
						makeText("b"), makeLit[int8](1),
					),
					makeText("c"), makeLit[int32](1),
				),
			},
		}

		for _, test := range tests {
			t.Run(fmt.Sprintf("%v", test.input), func(t *testing.T) {
				got, err := Marshal(test.input)

				textingx.Expect(t, err, textingx.Be[error](nil))
				textingx.Expect(t, got, textingx.Equal(test.want))

				var v map[string]any
				err = Unmarshal(got, &v)
				textingx.Expect(t, err, textingx.Be[error](nil))
				textingx.Expect(t, v, textingx.Equal(test.input))
			})
		}
	})

	t.Run("array", func(t *testing.T) {
		tests := []struct {
			input []any
			want  []byte
		}{
			{
				[]any{},
				makeSizedValue(array16Value, 0),
			},
			{
				[]any{int32(1)},
				makeSizedValue(array16Value, 1, makeLit[int32](1)),
			},
			{
				[]any{"1111111111111111111"},
				makeSizedValue(array16Value, 1, makeText("1111111111111111111")),
			},
			{
				[]any{int32(1), []any{}},
				makeSizedValue(array16Value, 2, makeLit[int32](1), makeSizedValue(array16Value, 0)),
			},
		}

		for _, test := range tests {
			t.Run(fmt.Sprintf("%v", test.input), func(t *testing.T) {
				got, err := Marshal(test.input)
				textingx.Expect(t, err, textingx.Be[error](nil))
				textingx.Expect(t, got, textingx.Equal(test.want))

				var v []any
				err = Unmarshal(got, &v)
				textingx.Expect(t, err, textingx.Be[error](nil))
				textingx.Expect(t, v, textingx.Equal(test.input))
			})
		}
	})

	t.Run("bin", func(t *testing.T) {
		a100 := makeSizedValue(bin8Value, 100, bytes.Repeat([]byte{'a'}, 100))

		tests := []struct {
			input []byte
			want  []byte
		}{
			{[]byte{}, makeSizedValue(bin8Value, 0)},
			{[]byte{'a'}, makeSizedValue(bin8Value, 1, []byte{'a'})},
			{bytes.Repeat([]byte{'a'}, 100), a100},
		}

		for _, test := range tests {
			t.Run(fmt.Sprintf("%v", test.input), func(t *testing.T) {
				got, err := Marshal(test.input)
				textingx.Expect(t, err, textingx.Be[error](nil))
				textingx.Expect(t, got, textingx.Equal(test.want))

				var v []byte
				err = Unmarshal(got, &v)
				textingx.Expect(t, err, textingx.Be[error](nil))
				textingx.Expect(t, v, textingx.Equal(test.input))
			})
		}
	})

	t.Run("text", func(t *testing.T) {
		a100 := makeText(strings.Repeat("a", 100))

		tests := []struct {
			input string
			want  []byte
		}{
			{"", makeSizedValue(str8Value, 0)},
			{"a", makeSizedValue(str8Value, 1, []byte{'a'})},
			{strings.Repeat("a", 100), a100},
			{"中文测试", makeSizedValue(str8Value, 12, []byte("中文测试"))},
		}

		for _, test := range tests {
			t.Run(fmt.Sprintf("%v", test.input), func(t *testing.T) {
				got, err := Marshal(test.input)
				textingx.Expect(t, err, textingx.Be[error](nil))
				textingx.Expect(t, got, textingx.Equal(test.want))

				var v string
				err = Unmarshal(got, &v)
				textingx.Expect(t, err, textingx.Be[error](nil))
				textingx.Expect(t, v, textingx.Equal(test.input))
			})
		}
	})

	t.Run("uint", func(t *testing.T) {
		tests := []struct {
			input any
			want  []byte
		}{
			// uint8
			{uint8(128), []byte{uint8Value, 0x80}},
			{uint8(math.MaxUint8), []byte{uint8Value, 0xff}},
			// uint16
			{uint16(math.MaxUint16), []byte{uint16Value, 0xff, 0xff}},
			// uint32
			{uint32(math.MaxUint32), []byte{uint32Value, 0xff, 0xff, 0xff, 0xff}},
			// uint64
			{uint64(math.MaxUint32 + 1), []byte{uint64Value, 0x0, 0x0, 0x0, 0x1, 0x0, 0x0, 0x0, 0x0}},
		}

		for _, test := range tests {
			t.Run(fmt.Sprintf("%d", test.input), func(t *testing.T) {
				got, err := Marshal(test.input)
				textingx.Expect(t, err, textingx.Be[error](nil))
				textingx.Expect(t, got, textingx.Equal(test.want))

				rv := reflect.New(reflect.TypeOf(test.input))
				err = Unmarshal(got, rv)
				textingx.Expect(t, err, textingx.Be[error](nil))
				textingx.Expect(t, rv.Elem().Interface(), textingx.Equal(test.input))
			})
		}
	})

	t.Run("int", func(t *testing.T) {
		tests := []struct {
			input any
			want  []byte
		}{
			// int8
			{int8(math.MinInt8), []byte{int8Value, 0x00}},
			{int8(-33), []byte{int8Value, 0x5f}},
			{int8(-40), []byte{int8Value, 0x58}},

			// int16
			{int16(math.MinInt16), []byte{int16Value, 0x00, 0x00}},
			{int16(-400), []byte{int16Value, 0x7e, 0x70}},

			// int32
			{int32(math.MinInt32), []byte{int32Value, 0x00, 0x00, 0x00, 0x00}},
			{int32(-4000000), []byte{int32Value, 0x7f, 0xc2, 0xf7, 0x0}},

			// int64
			{int64(math.MinInt64), []byte{int64Value, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}},
			{int64(-4000000000000), []byte{int64Value, 0x7f, 0xff, 0xfc, 0x5c, 0xad, 0x6b, 0xc0, 0x0}},
		}

		for _, test := range tests {
			t.Run(fmt.Sprintf("%d", test.input), func(t *testing.T) {
				got, err := Marshal(test.input)
				textingx.Expect(t, err, textingx.Be[error](nil))
				textingx.Expect(t, got, textingx.Equal(test.want))

				rv := reflect.New(reflect.TypeOf(test.input))
				err = Unmarshal(got, rv)
				textingx.Expect(t, err, textingx.Be[error](nil))
				textingx.Expect(t, rv.Elem().Interface(), textingx.Equal(test.input))
			})
		}
	})

	t.Run("float", func(t *testing.T) {
		tests := []struct {
			input any
			want  []byte
		}{
			{
				-3.14,
				[]byte{float64Value, 0x3f, 0xf6, 0xe1, 0x47, 0xae, 0x14, 0x7a, 0xe0}},
			{
				float32(-3),
				[]byte{float32Value, 0x3f, 0xbf, 0xff, 0xff},
			},
		}

		for _, test := range tests {
			t.Run(fmt.Sprintf("Marshal %f", test.input), func(t *testing.T) {
				got, err := Marshal(test.input)
				textingx.Expect(t, err, textingx.Be[error](nil))
				textingx.Expect(t, got, textingx.Equal(test.want))

				rv := reflect.New(reflect.TypeOf(test.input))
				err = Unmarshal(got, rv)
				textingx.Expect(t, err, textingx.Be[error](nil))
				textingx.Expect(t, rv.Elem().Interface(), textingx.Equal(test.input))
			})
		}
	})
}

func makeValue(typ byte, bytes ...[]byte) []byte {
	var out = []byte{typ}
	for _, b := range bytes {
		out = append(out, b...)
	}
	return out
}

func makeSizedValue(typ byte, n int, bytes ...[]byte) []byte {
	size := 8

	switch typ {
	case str32Value, bin32Value, array32Value, map32Value:
		size = 32
	case str16Value, bin16Value, array16Value, map16Value:
		size = 16
	}

	var out = append([]byte{typ}, makeUvarint(size, uint64(n))...)
	for _, b := range bytes {
		out = append(out, b...)
	}
	return out
}

func makeLit[T any](s T) []byte {
	d, _ := Marshal(s)
	return d
}

func makeText(s string) []byte {
	b := []byte(s)
	n := len(b)
	size := bitSize(n)
	switch size {
	case 32:
		return makeSizedValue(str32Value, n, b)
	case 16:
		return makeSizedValue(str16Value, n, b)
	}
	return makeSizedValue(str8Value, n, b)
}
