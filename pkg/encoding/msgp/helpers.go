package msgp

import (
	"fmt"
	"math"
)

func MinValueForType(tp any) any {
	switch tp.(type) {
	case bool:
		return false
	case int8:
		return math.MinInt8
	case int16:
		return math.MinInt16
	case int32, int:
		return math.MinInt32
	case int64:
		return math.MinInt64
	case uint, uint8, uint16, uint32, uint64:
		return 0
	case float32:
		return math.SmallestNonzeroFloat32
	case float64:
		return math.SmallestNonzeroFloat64
	case []byte:
		return make([]byte, 0)
	case string:
		return ""
	case []any:
		return make([]any, 0)
	case map[string]any:
		return make(map[string]any, 0)
	default:
		// TODO handle named type
		panic(fmt.Sprintf("unsupported type %v", tp))
	}
}

func MaxTypeCodeForType(tp any) byte {
	if tp == nil {
		return nullValue + 1
	}
	switch tp.(type) {
	case bool:
		return trueValue + 1
	case int8:
		return int8Value + 1
	case int16:
		return int16Value + 1
	case int32, int:
		return int32Value + 1
	case int64:
		return int64Value + 1
	case uint, uint8, uint16, uint32, uint64:
		return uint64Value + 1
	case float32, float64:
		return float32Value + 1
	case []byte:
		return bin32Value + 1
	case string:
		return str32Value + 1
	case []any:
		return array32Value + 1
	case map[string]any:
		return map32Value + 1
	default:
		// TODO handle named type
		panic(fmt.Sprintf("unsupported type %v", tp))
	}
}
