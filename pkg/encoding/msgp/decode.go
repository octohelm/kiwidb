package msgp

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"reflect"
	"strings"
	"unsafe"
)

func Unmarshal(data []byte, v any) error {
	var d decodeState
	d.init(bytes.NewBuffer(data))
	return d.unmarshal(v)
}

// decodeState represents the state while decoding a JSON value.
type decodeState struct {
	io.Reader
	off                   int
	errorContext          *errorContext
	savedError            error
	useNumber             bool
	disallowUnknownFields bool
}

func (d *decodeState) unmarshal(v any) error {
	rv, ok := v.(reflect.Value)
	if !ok {
		rv = reflect.ValueOf(v)
	}
	if rv.Kind() != reflect.Pointer || rv.IsNil() {
		return &InvalidUnmarshalError{reflect.TypeOf(v)}
	}
	err := d.value(rv.Elem())
	if err != nil {
		return d.addErrorContext(err)
	}
	return d.savedError
}

func (d *decodeState) value(rv reflect.Value) error {
	code, err := d.readCode()
	if err != nil {
		return err
	}

	// todo handler sql.Scanner

	switch code {
	case nullValue:
		// TODO
	case trueValue:
		d.bool(true, rv)
	case falseValue:
		d.bool(false, rv)
	case int8Value:
		if err := d.int(1, rv); err != nil {
			return err
		}
	case int16Value:
		if err := d.int(2, rv); err != nil {
			return err
		}
	case int32Value:
		if err := d.int(4, rv); err != nil {
			return err
		}
	case int64Value:
		if err := d.int(8, rv); err != nil {
			return err
		}
	case uint8Value:
		if err := d.uint(1, rv); err != nil {
			return err
		}
	case uint16Value:
		if err := d.uint(2, rv); err != nil {
			return err
		}
	case uint32Value:
		if err := d.uint(4, rv); err != nil {
			return err
		}
	case uint64Value:
		if err := d.uint(8, rv); err != nil {
			return err
		}
	case float32Value:
		if err := d.float(4, rv); err != nil {
			return err
		}
	case float64Value:
		if err := d.float(8, rv); err != nil {
			return err
		}
	case str8Value:
		if err := d.string(8, rv); err != nil {
			return err
		}
	case str16Value:
		if err := d.string(16, rv); err != nil {
			return err
		}
	case str32Value:
		if err := d.string(32, rv); err != nil {
			return err
		}
	case bin8Value:
		if err := d.blob(8, rv); err != nil {
			return err
		}
	case bin16Value:
		if err := d.blob(16, rv); err != nil {
			return err
		}
	case bin32Value:
		if err := d.blob(32, rv); err != nil {
			return err
		}
	case array16Value:
		if err := d.array(16, rv); err != nil {
			return err
		}
	case array32Value:
		if err := d.array(32, rv); err != nil {
			return err
		}
	case map16Value:
		if err := d.object(16, rv); err != nil {
			return err
		}
	case map32Value:
		if err := d.object(32, rv); err != nil {
			return err
		}
	}

	return nil
}

func (d *decodeState) object(bitSize int, rv reflect.Value) error {
	n, err := d.readN(bitSize)
	if err != nil {
		return err
	}

	var fields structFields

	switch rv.Kind() {
	case reflect.Struct:
		fields = cachedTypeFields(rv.Type())
	case reflect.Interface, reflect.Map:
		if rv.Kind() == reflect.Interface {
			if rv.NumMethod() != 0 {
				d.saveError(&UnmarshalTypeError{Value: "object", Type: rv.Type(), Offset: int64(d.off)})
				break
			}
			m := reflect.MakeMap(reflect.TypeOf(map[string]any{}))
			rv.Set(m)
			rv = m
		} else {
			if rv.IsNil() {
				m := reflect.MakeMap(rv.Type())
				rv.Set(m)
			}
		}
	}

	for i := 0; i < int(n); i++ {
		var key reflect.Value
		if rv.Kind() == reflect.Map {
			key = reflect.New(rv.Type().Key()).Elem()
		} else {
			key = reflect.New(reflect.TypeOf("")).Elem()
		}

		if err := d.value(key); err != nil {
			return err
		}

		if rv.Kind() == reflect.Map {
			elem := reflect.New(rv.Type().Elem()).Elem()
			if err := d.value(elem); err != nil {
				return err
			}
			rv.SetMapIndex(key, elem)
		} else {
			var f *field

			if i, ok := fields.nameIndex[key.String()]; ok {
				// Found an exact name match.
				f = &fields.list[i]
			}
			if f != nil {
				subv := rv
				for _, i := range f.index {
					if subv.Kind() == reflect.Pointer {
						if subv.IsNil() {
							if !subv.CanSet() {
								d.saveError(fmt.Errorf("bitewise: cannot set embedded pointer to unexported struct: %v", subv.Type().Elem()))
								subv = reflect.Value{}
								break
							}
							subv.Set(reflect.New(subv.Type().Elem()))
						}
						subv = subv.Elem()
					}
					subv = subv.Field(i)
				}

				if d.errorContext == nil {
					d.errorContext = new(errorContext)
				}
				d.errorContext.FieldStack = append(d.errorContext.FieldStack, f.name)
				d.errorContext.Struct = rv.Type()

				if err := d.value(subv); err != nil {
					return err
				}
			} else if d.disallowUnknownFields {
				d.saveError(fmt.Errorf("bitewise: unknown field %q", key.String()))
			}
		}
	}

	return nil
}

func (d *decodeState) array(bitSize int, rv reflect.Value) error {
	n, err := d.readN(bitSize)
	if err != nil {
		return err
	}

	switch rv.Kind() {
	case reflect.Interface:
		if rv.NumMethod() != 0 {
			d.saveError(&UnmarshalTypeError{Value: "array", Type: rv.Type(), Offset: int64(d.off)})
			break
		}
		rv.Set(reflect.MakeSlice(reflect.TypeOf([]any{}), int(n), int(n)))
	case reflect.Slice:
		rv.Set(reflect.MakeSlice(rv.Type(), int(n), int(n)))
	}

	for i := 0; i < int(n); i++ {
		if err := d.value(rv.Index(i)); err != nil {
			return err
		}
	}

	return nil
}

func (d *decodeState) bool(b bool, rv reflect.Value) {
	switch rv.Kind() {
	default:
		d.saveError(&UnmarshalTypeError{Value: "bool", Type: rv.Type(), Offset: int64(d.off)})
	case reflect.Interface:
		if rv.NumMethod() != 0 {
			d.saveError(&UnmarshalTypeError{Value: "bool", Type: rv.Type(), Offset: int64(d.off)})
			break
		}
		rv.Set(reflect.ValueOf(b))
	case reflect.Bool:
		rv.SetBool(b)
	}
}

func (d *decodeState) string(bitSize int, rv reflect.Value) error {
	n, err := d.readN(bitSize)
	if err != nil {
		return err
	}
	b, err := d.read(int(n))
	if err != nil {
		return err
	}

	switch rv.Kind() {
	default:
		d.saveError(&UnmarshalTypeError{Value: "text", Type: rv.Type(), Offset: int64(d.off)})
	case reflect.Interface:
		if rv.NumMethod() != 0 {
			d.saveError(&UnmarshalTypeError{Value: "text", Type: rv.Type(), Offset: int64(d.off)})
			break
		}
		s := *(*string)(unsafe.Pointer(&b))
		rv.Set(reflect.ValueOf(s))
	case reflect.String:
		s := *(*string)(unsafe.Pointer(&b))
		rv.SetString(s)
	}
	return nil
}

func (d *decodeState) blob(bitSize int, rv reflect.Value) error {
	n, err := d.readN(bitSize)
	if err != nil {
		return err
	}
	b, err := d.read(int(n))
	if err != nil {
		return err
	}

	switch rv.Kind() {
	default:
		d.saveError(&UnmarshalTypeError{Value: "blob", Type: rv.Type(), Offset: int64(d.off)})
	case reflect.Interface:
		if rv.NumMethod() != 0 {
			d.saveError(&UnmarshalTypeError{Value: "blob", Type: rv.Type(), Offset: int64(d.off)})
			break
		}
		rv.Set(reflect.ValueOf(b))
	case reflect.Slice:
		if rv.Type().Elem().Kind() != reflect.Uint8 {
			d.saveError(&UnmarshalTypeError{Value: "blob", Type: rv.Type(), Offset: int64(d.off)})
			break
		}
		rv.SetBytes(b)
	}
	return nil
}

func (d *decodeState) float(n int, rv reflect.Value) error {
	b, err := d.read(n)
	if err != nil {
		return err
	}

	var f float64
	var ff any
	var tpe = "float"

	switch len(b) {
	case 4:
		x := binary.BigEndian.Uint32(b)
		if (x & (1 << 31)) != 0 {
			x ^= 1 << 31
		} else {
			x ^= 1<<32 - 1
		}
		n := math.Float32frombits(x)

		f = float64(n)
		ff = n
		tpe = "float32"
	case 8:
		x := binary.BigEndian.Uint64(b)
		if (x & (1 << 63)) != 0 {
			x ^= 1 << 63
		} else {
			x ^= 1<<64 - 1
		}
		n := math.Float64frombits(x)

		f = n
		ff = n
		tpe = "float64"
	}

	switch rv.Kind() {
	default:
		d.saveError(&UnmarshalTypeError{Value: tpe, Type: rv.Type(), Offset: int64(d.off)})
	case reflect.Interface:
		if rv.NumMethod() != 0 {
			d.saveError(&UnmarshalTypeError{Value: tpe, Type: rv.Type(), Offset: int64(d.off)})
			break
		}
		rv.Set(reflect.ValueOf(ff))
	case reflect.Float32:
		if rv.OverflowFloat(f) {
			d.saveError(&UnmarshalTypeError{Value: tpe, Type: rv.Type(), Offset: int64(d.off)})
			break
		}
		rv.SetFloat(f)
	case reflect.Float64:
		rv.SetFloat(f)
	}

	return nil
}

func (d *decodeState) int(n int, rv reflect.Value) error {
	b, err := d.read(n)
	if err != nil {
		return err
	}

	var x int64
	var xx any
	var tpe = "int"

	switch len(b) {
	case 1:
		n := int8(uint8(b[0]) - (math.MaxInt8 + 1))
		x = int64(n)
		xx = n
		tpe = "int8"
	case 2:
		n := int16(binary.BigEndian.Uint16(b) - (math.MaxInt16 + 1))
		x = int64(n)
		xx = n
		tpe = "int16"
	case 4:
		n := int32(binary.BigEndian.Uint32(b) - (math.MaxInt32 + 1))
		x = int64(n)
		xx = n
		tpe = "int32"
	case 8:
		n := int64(binary.BigEndian.Uint64(b) - (math.MaxInt64 + 1))
		x = n
		xx = n
		tpe = "int64"
	}

	switch rv.Kind() {
	default:
		d.saveError(&UnmarshalTypeError{Value: tpe, Type: rv.Type(), Offset: int64(d.off)})
	case reflect.Interface:
		if rv.NumMethod() != 0 {
			d.saveError(&UnmarshalTypeError{Value: tpe, Type: rv.Type(), Offset: int64(d.off)})
			break
		}
		rv.Set(reflect.ValueOf(xx))
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		if rv.OverflowInt(x) {
			d.saveError(&UnmarshalTypeError{Value: tpe, Type: rv.Type(), Offset: int64(d.off)})
			break
		}
		rv.SetInt(x)
	}
	return nil
}

func (d *decodeState) uint(n int, rv reflect.Value) error {
	b, err := d.read(n)
	if err != nil {
		return err
	}

	var x uint64
	var xx any
	var tpe = "uint"

	switch len(b) {
	case 1:
		n := b[0]
		x = uint64(n)
		xx = uint8(n)
		tpe = "uint8"
	case 2:
		n := binary.BigEndian.Uint16(b)
		x = uint64(n)
		xx = uint16(n)
		tpe = "uint16"
	case 4:
		n := binary.BigEndian.Uint32(b)
		x = uint64(n)
		xx = uint32(n)
		tpe = "uint32"
	case 8:
		n := binary.BigEndian.Uint64(b)
		x = uint64(n)
		xx = uint64(n)
		tpe = "uint64"
	}

	switch rv.Kind() {
	default:
		d.saveError(&UnmarshalTypeError{Value: tpe, Type: rv.Type(), Offset: int64(d.off)})
	case reflect.Interface:
		if rv.NumMethod() != 0 {
			d.saveError(&UnmarshalTypeError{Value: tpe, Type: rv.Type(), Offset: int64(d.off)})
			break
		}
		rv.Set(reflect.ValueOf(xx))
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		if rv.OverflowUint(x) {
			d.saveError(&UnmarshalTypeError{Value: tpe, Type: rv.Type(), Offset: int64(d.off)})
			break
		}
		rv.SetUint(x)
	}
	return nil
}

func (d *decodeState) readCode() (byte, error) {
	b, err := d.read(1)
	if err != nil {
		return 0, err
	}
	return b[0], nil
}

func (d *decodeState) read(n int) ([]byte, error) {
	part := make([]byte, n)
	nn, err := d.Read(part)
	if err != nil {
		return nil, err
	}
	d.off += nn
	return part, nil
}

func (d *decodeState) readN(bitSize int) (uint64, error) {
	switch bitSize {
	case 64:
		b, err := d.read(8)
		if err != nil {
			return 0, err
		}
		return binary.BigEndian.Uint64(b), nil
	case 32:
		b, err := d.read(4)
		if err != nil {
			return 0, err
		}
		return uint64(binary.BigEndian.Uint32(b)), nil
	case 16:
		b, err := d.read(2)
		if err != nil {
			return 0, err
		}
		return uint64(binary.BigEndian.Uint16(b)), nil
	}

	b, err := d.read(1)
	if err != nil {
		return 0, err
	}
	return uint64(b[0]), nil
}

func (d *decodeState) init(r io.Reader) *decodeState {
	d.Reader = r

	d.savedError = nil
	if d.errorContext != nil {
		d.errorContext.Struct = nil
		// Reuse the allocated space for the FieldStack slice.
		d.errorContext.FieldStack = d.errorContext.FieldStack[:0]
	}
	return d
}

type errorContext struct {
	Struct     reflect.Type
	FieldStack []string
}

func (d *decodeState) saveError(err error) {
	if d.savedError == nil {
		d.savedError = d.addErrorContext(err)
	}
}

func (d *decodeState) addErrorContext(err error) error {
	if d.errorContext != nil && (d.errorContext.Struct != nil || len(d.errorContext.FieldStack) > 0) {
		switch err := err.(type) {
		case *UnmarshalTypeError:
			err.Struct = d.errorContext.Struct.Name()
			err.Field = strings.Join(d.errorContext.FieldStack, ".")
		}
	}
	return err
}
