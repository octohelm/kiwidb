package msgp

import "reflect"

type UnsupportedTypeError struct {
	Type reflect.Type
}

func (e *UnsupportedTypeError) Error() string {
	return "msgp: unsupported type: " + e.Type.String()
}

type UnsupportedValueError struct {
	Value reflect.Value
	Str   string
}

func (e *UnsupportedValueError) Error() string {
	return "msgp: unsupported value: " + e.Str
}

type InvalidUnmarshalError struct {
	Type reflect.Type
}

func (e *InvalidUnmarshalError) Error() string {
	if e.Type == nil {
		return "msgp: Unmarshal(nil)"
	}

	if e.Type.Kind() != reflect.Pointer {
		return "msgp: Unmarshal(non-pointer " + e.Type.String() + ")"
	}
	return "msgp: Unmarshal(nil " + e.Type.String() + ")"
}

type UnmarshalTypeError struct {
	Value  string       // description of JSON value - "bool", "array", "number -5"
	Type   reflect.Type // type of Go value it could not be assigned to
	Offset int64        // error occurred after reading Offset bytes
	Struct string       // name of the struct type containing the field
	Field  string       // the full path from root node to the field
}

func (e *UnmarshalTypeError) Error() string {
	if e.Struct != "" || e.Field != "" {
		return "msgp: cannot unmarshal " + e.Value + " into Go struct field " + e.Struct + "." + e.Field + " of type " + e.Type.String()
	}
	return "msgp: cannot unmarshal " + e.Value + " into Go value of type " + e.Type.String()
}
