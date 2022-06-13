package msgp

// follow https://github.com/msgpack/msgpack/blob/master/spec.md#overview
// negative fixed int and float
const (
	nullValue byte = 0xC0

	falseValue byte = 0xC2
	trueValue  byte = 0xC3

	bin8Value  byte = 0xC4
	bin16Value byte = 0xC5
	bin32Value byte = 0xC6

	float32Value byte = 0xCA
	float64Value byte = 0xCB

	uint8Value  byte = 0xCC
	uint16Value byte = 0xCD
	uint32Value byte = 0xCE
	uint64Value byte = 0xCF

	int8Value  byte = 0xD0
	int16Value byte = 0xD1
	int32Value byte = 0xD2
	int64Value byte = 0xD3

	str8Value  byte = 0xD9
	str16Value byte = 0xDA
	str32Value byte = 0xDB

	array16Value byte = 0xDC
	array32Value byte = 0xDD

	map16Value byte = 0xDE
	map32Value byte = 0xDF
)
