package msgp

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"reflect"
	"sort"
	"sync"
)

func Marshal(v any) ([]byte, error) {
	e := newEncodeState()
	err := e.marshal(v)
	if err != nil {
		return nil, err
	}
	buf := append([]byte(nil), e.Bytes()...)
	encodeStatePool.Put(e)
	return buf, nil
}

var encodeStatePool sync.Pool

func newEncodeState() *encodeState {
	if v := encodeStatePool.Get(); v != nil {
		e := v.(*encodeState)
		e.Reset()
		return e
	}
	return &encodeState{}
}

type encodeState struct {
	bytes.Buffer
}

type Encoded []byte

func (e *encodeState) marshal(v any) (err error) {
	if encoded, ok := v.(Encoded); ok {
		_, err := e.Write(encoded)
		if err != nil {
			return err
		}
	}

	defer func() {
		if r := recover(); r != nil {
			if je, ok := r.(error); ok {
				err = je
			} else {
				panic(r)
			}
		}
	}()
	e.reflectValue(reflect.ValueOf(v))
	return nil
}

func (e *encodeState) reflectValue(v reflect.Value) {
	valueEncoder(v)(e, v)
}

type writer interface {
	WriteByte(b byte) error
	io.Writer
}

type encoderFunc func(e writer, v reflect.Value)

func valueEncoder(v reflect.Value) encoderFunc {
	if !v.IsValid() {
		return invalidValueEncoder
	}
	return typeEncoder(v.Type())
}

var encoderCache sync.Map // map[reflect.Type]encoderFunc

func typeEncoder(t reflect.Type) encoderFunc {
	if fi, ok := encoderCache.Load(t); ok {
		return fi.(encoderFunc)
	}

	// To deal with recursive types, populate the map with an
	// indirect func before we build it. This type waits on the
	// real func (f) to be ready and then calls it. This indirect
	// func is only used for recursive types.
	var (
		wg sync.WaitGroup
		f  encoderFunc
	)

	wg.Add(1)

	fi, loaded := encoderCache.LoadOrStore(t, encoderFunc(func(e writer, v reflect.Value) {
		wg.Wait()
		f(e, v)
	}))

	if loaded {
		return fi.(encoderFunc)
	}

	// Compute the real encoder and replace the indirect func with it.
	f = newTypeEncoder(t)

	wg.Done()
	encoderCache.Store(t, f)
	return f
}

func newTypeEncoder(t reflect.Type) encoderFunc {
	switch t.Kind() {
	case reflect.Bool:
		return boolEncoder
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return intEncoder
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return uintEncoder
	case reflect.Float32:
		return float32Encoder
	case reflect.Float64:
		return float64Encoder
	case reflect.String:
		return stringEncoder
	case reflect.Interface:
		return interfaceEncoder
	case reflect.Slice:
		return newSliceEncoder(t)
	case reflect.Array:
		return newArrayEncoder(t)
	case reflect.Map:
		return newMapEncoder(t)
	case reflect.Struct:
		return newStructEncoder(t)
	case reflect.Pointer:
		return newPtrEncoder(t)
	default:
		return unsupportedTypeEncoder
	}
}

type structEncoder struct {
	fields structFields
}

func newStructEncoder(t reflect.Type) encoderFunc {
	se := structEncoder{fields: cachedTypeFields(t)}
	return se.encode
}

func (se structEncoder) encode(e writer, v reflect.Value) {
	n := len(se.fields.list)
	size := bitSize(n)

	switch size {
	case 64:
		panic(fmt.Errorf("map len is to big"))
	case 32:
		_ = e.WriteByte(map32Value)
		_, _ = e.Write(makeUvarint(32, uint64(n)))
	default:
		_ = e.WriteByte(map16Value)
		_, _ = e.Write(makeUvarint(16, uint64(n)))
	}

	for i := range se.fields.list {
		f := &se.fields.list[i]

		stringEncoder(e, reflect.ValueOf(f.name))

		fv := v
		for _, i := range f.index {
			if fv.Kind() == reflect.Pointer {
				if fv.IsNil() {
					f.encoder(e, reflect.ValueOf(nullValue))
					continue
				}
				fv = fv.Elem()
			}
			fv = fv.Field(i)
		}

		f.encoder(e, fv)
	}
}

func newMapEncoder(t reflect.Type) encoderFunc {
	switch t.Key().Kind() {
	case reflect.String, reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
	default:
		//if !t.Key().Implements(textMarshalerType) {
		return unsupportedTypeEncoder
		//}
	}
	me := mapEncoder{
		keyEnc:  typeEncoder(t.Key()),
		elemEnc: typeEncoder(t.Elem()),
	}
	return me.encode
}

type mapEncoder struct {
	keyEnc  encoderFunc
	elemEnc encoderFunc
}

func (me mapEncoder) encode(e writer, rv reflect.Value) {
	n := rv.Len()
	size := bitSize(n)

	switch size {
	case 64:
		panic(fmt.Errorf("map len is to big"))
	case 32:
		_ = e.WriteByte(map32Value)
		_, _ = e.Write(makeUvarint(32, uint64(n)))
	default:
		_ = e.WriteByte(map16Value)
		_, _ = e.Write(makeUvarint(16, uint64(n)))
	}

	sv := make([]reflectWithString, n)

	mi := rv.MapRange()
	for i := 0; mi.Next(); i++ {
		sv[i].k = mi.Key()
		sv[i].v = mi.Value()
		if err := sv[i].resolve(); err != nil {
			panic(fmt.Errorf("msgp: encoding error for type %q: %q", rv.Type().String(), err.Error()))
		}
	}

	sort.Slice(sv, func(i, j int) bool { return Compare(sv[i].ks, sv[j].ks) < 0 })

	for _, s := range sv {
		_, _ = e.Write(s.ks)
		me.elemEnc(e, s.v)
	}
}

type reflectWithString struct {
	ks []byte
	k  reflect.Value
	v  reflect.Value
}

func (w *reflectWithString) resolve() error {
	if w.k.Kind() == reflect.String {
		b := &bytes.Buffer{}
		stringEncoder(b, w.k)
		w.ks = b.Bytes()
		return nil
	}
	//if tm, ok := w.k.Interface().(encoding.TextMarshaler); ok {
	//	return err
	//}
	switch w.k.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		b := &bytes.Buffer{}
		intEncoder(b, w.k)
		w.ks = b.Bytes()
		return nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		b := &bytes.Buffer{}
		uintEncoder(b, w.k)
		w.ks = b.Bytes()
		return nil
	}
	panic("unexpected map key type")
}

func newSliceEncoder(t reflect.Type) encoderFunc {
	// Byte slices get special treatment; arrays don't.
	if t.Elem().Kind() == reflect.Uint8 {
		return encodeByteSlice
	}
	enc := arrayEncoder{typeEncoder(t.Elem())}
	return enc.encode
}

func encodeByteSlice(e writer, v reflect.Value) {
	b := make([]byte, 0)
	if !v.IsNil() {
		b = v.Bytes()
	}

	n := len(b)
	size := bitSize(n)

	switch size {
	case 64:
		panic(fmt.Errorf("bin len is to big"))
	case 32:
		_ = e.WriteByte(bin32Value)
	case 16:
		_ = e.WriteByte(bin16Value)
	default:
		_ = e.WriteByte(bin8Value)
	}

	_, _ = e.Write(makeUvarint(size, uint64(n)))
	_, _ = e.Write(b)
}

func newArrayEncoder(t reflect.Type) encoderFunc {
	enc := arrayEncoder{typeEncoder(t.Elem())}
	return enc.encode
}

type arrayEncoder struct {
	elemEnc encoderFunc
}

func (ae arrayEncoder) encode(e writer, v reflect.Value) {
	n := v.Len()
	size := bitSize(n)

	switch size {
	case 64:
		panic(fmt.Errorf("array len is to big"))
	case 32:
		_ = e.WriteByte(array32Value)
		_, _ = e.Write(makeUvarint(32, uint64(n)))
	default:
		_ = e.WriteByte(array16Value)
		_, _ = e.Write(makeUvarint(16, uint64(n)))
	}

	for i := 0; i < v.Len(); i++ {
		ae.elemEnc(e, v.Index(i))
	}
}

type ptrEncoder struct {
	elemEnc encoderFunc
}

func (pe ptrEncoder) encode(e writer, v reflect.Value) {
	if v.IsNil() {
		_ = e.WriteByte(nullValue)
		return
	}
	pe.elemEnc(e, v.Elem())
}

func newPtrEncoder(t reflect.Type) encoderFunc {
	enc := ptrEncoder{typeEncoder(t.Elem())}
	return enc.encode
}

func boolEncoder(e writer, v reflect.Value) {
	if v.Bool() {
		_ = e.WriteByte(trueValue)
	} else {
		_ = e.WriteByte(falseValue)
	}
}

func intEncoder(e writer, v reflect.Value) {
	switch v.Kind() {
	case reflect.Int8:
		write1To(e, int8Value, uint8(v.Int())+math.MaxInt8+1)
	case reflect.Int16:
		write2To(e, int16Value, uint16(v.Int())+math.MaxInt16+1)
	case reflect.Int, reflect.Int32:
		write4To(e, int32Value, uint32(v.Int())+math.MaxInt32+1)
	case reflect.Int64:
		write8To(e, int64Value, uint64(v.Int())+math.MaxInt64+1)
	}
}

func uintEncoder(e writer, v reflect.Value) {
	switch v.Kind() {
	case reflect.Uint8:
		write1To(e, uint8Value, uint8(v.Uint()))
	case reflect.Uint16:
		write2To(e, uint16Value, uint16(v.Uint()))
	case reflect.Uint, reflect.Uint32:
		write4To(e, uint32Value, uint32(v.Uint()))
	case reflect.Uint64:
		write8To(e, uint64Value, v.Uint())
	}
}

func float32Encoder(e writer, v reflect.Value) {
	x := float32(v.Float())
	fb := math.Float32bits(x)
	if x >= 0 {
		fb ^= 1 << 31
	} else {
		fb ^= 1<<32 - 1
	}
	write4To(e, float32Value, fb)
}

func float64Encoder(e writer, v reflect.Value) {
	x := v.Float()
	fb := math.Float64bits(x)
	if x >= 0 {
		fb ^= 1 << 63
	} else {
		fb ^= 1<<64 - 1
	}
	write8To(e, float64Value, fb)
}

func stringEncoder(e writer, v reflect.Value) {
	b := []byte(v.String())
	n := len(b)
	size := bitSize(n)

	switch size {
	case 64:
		panic(fmt.Errorf("str len is to big"))
	case 32:
		_ = e.WriteByte(str32Value)
	case 16:
		_ = e.WriteByte(str16Value)
	case 8:
		_ = e.WriteByte(str8Value)
	}

	_, _ = e.Write(makeUvarint(size, uint64(n)))
	_, _ = e.Write(b)
}

func interfaceEncoder(e writer, v reflect.Value) {
	if v.IsNil() {
		_ = e.WriteByte(nullValue)
		return
	}
	elemV := v.Elem()
	valueEncoder(elemV)(e, elemV)
}

func write1To(e writer, code byte, n uint8) {
	_ = e.WriteByte(code)
	_ = e.WriteByte(n)
}

func write2To(e writer, code byte, n uint16) {
	_ = e.WriteByte(code)
	b := make([]byte, 2)
	binary.BigEndian.PutUint16(b, n)
	_, _ = e.Write(b)
}

func write4To(e writer, code byte, n uint32) {
	_ = e.WriteByte(code)
	b := make([]byte, 4)
	binary.BigEndian.PutUint32(b, n)
	_, _ = e.Write(b)
}

func write8To(e writer, code byte, n uint64) {
	_ = e.WriteByte(code)
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, n)
	_, _ = e.Write(b)
}

func write8(b []byte, code byte, n uint64) []byte {
	b = append(b, code)
	bb := make([]byte, 8)
	binary.BigEndian.PutUint64(bb, n)
	return append(b, bb...)
}

func invalidValueEncoder(e writer, v reflect.Value) {
	_ = e.WriteByte(nullValue)
}

func unsupportedTypeEncoder(e writer, v reflect.Value) {
	panic(&UnsupportedTypeError{v.Type()})
}

func bitSize(n int) int {
	if n > math.MaxUint32 {
		return 64
	}
	if n > math.MaxUint16 {
		return 32
	}
	if n > math.MaxUint8 {
		return 16
	}
	return 8
}

func makeUvarint(bitSize int, n uint64) (b []byte) {
	switch bitSize {
	case 64:
		b = make([]byte, 8)
		binary.BigEndian.PutUint64(b, n)
	case 32:
		b = make([]byte, 4)
		binary.BigEndian.PutUint32(b, uint32(n))
	case 16:
		b = make([]byte, 2)
		binary.BigEndian.PutUint16(b, uint16(n))
	case 8:
		b = make([]byte, 1)
		b[0] = uint8(n)
	}
	return
}
