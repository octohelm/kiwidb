package msgp

import (
	"math"
	"sort"
	"testing"

	textingx "github.com/octohelm/x/testing"
)

func fuzz[T any](list []T) []T {
	f := map[int]T{}
	for i := range list {
		f[i] = list[i]
	}
	final := make([]T, 0, len(list))
	for k := range f {
		final = append(final, f[k])
	}
	return final
}

func TestOrdering(t *testing.T) {
	ordered := []any{
		// null first
		nil,

		// then bool
		false,
		true,

		// then floats
		math.SmallestNonzeroFloat64,
		math.SmallestNonzeroFloat32,
		float64(100),

		// then integers
		int64(math.MinInt64),
		int64(math.MinInt32),
		int64(math.MinInt16),
		int64(math.MinInt8),
		int64(-33),
		int64(-19),
		int64(-2),
		int64(0),
		int64(12),
		int64(42),
		int64(math.MaxInt8),
		int64(128),
		int64(279),
		int64(math.MaxInt16),
		int64(math.MaxInt32),
		int64(math.MaxInt64),

		// then text
		"1",
		"2",
		"3",
		"中文",
		"测",
		"试",
		"😀",
	}

	fuzzed := fuzz(ordered)

	if fuzzed[0] != ordered[0] {
		sort.Slice(fuzzed, func(i, j int) bool {
			a, _ := Marshal(fuzzed[i])
			b, _ := Marshal(fuzzed[j])
			return Compare(a, b) < 0
		})

		for i := range fuzzed {
			textingx.Expect(t, fuzzed[i], textingx.Equal(ordered[i]))
		}
	}
}
