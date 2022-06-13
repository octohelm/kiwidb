package msgp

import (
	"fmt"
	"testing"

	testingx "github.com/octohelm/x/testing"
)

func TestOperator(t *testing.T) {
	var v = map[string]any{
		"a": []any{
			map[int8]int32{
				1: 1,
				2: 2,
			},
			map[string]int32{
				"1": 1,
				"2": 2,
			},
		},
		"b": map[string]any{
			"c": map[string]string{
				"d": "1",
			},
		},
	}

	data, _ := Marshal(v)

	t.Run("Get", func(t *testing.T) {
		tests := []struct {
			path []any
			want any
		}{
			{
				path: []any{"b", "c", "d"},
				want: "1",
			},
			{
				path: []any{"a", 0, int8(1)},
				want: int32(1),
			},
			{
				path: []any{"a", 0, 1},
				want: nil,
			},
			{
				path: []any{"a", 1, "1"},
				want: int32(1),
			},
		}

		for i := range tests {
			c := tests[i]

			if c.want != nil {
				t.Run(fmt.Sprintf("key path %v should got %v", c.path, c.want), func(t *testing.T) {
					v, err := Get(data, c.path)
					testingx.Expect(t, err, testingx.Be[error](nil))
					var value any
					err = Unmarshal(v, &value)
					testingx.Expect(t, err, testingx.Be[error](nil))
					testingx.Expect(t, value, testingx.Be(c.want))
				})
			} else {
				t.Run(fmt.Sprintf("key path %v should not exists", c.path), func(t *testing.T) {
					_, err := Get(data, c.path)
					testingx.Expect(t, err, testingx.Be[error](ErrKeyPathNotExists))
				})
			}
		}
	})

	t.Run("Set", func(t *testing.T) {
		upgrade, err := Set(data, []any{"b", "c", "d"}, func(cur []byte) ([]byte, error) {
			return Marshal("2222")
		})
		testingx.Expect(t, err, testingx.Be[error](nil))

		ret, err := Get(upgrade, []any{"b", "c", "d"})
		var value string
		err = Unmarshal(ret, &value)
		testingx.Expect(t, err, testingx.Be[error](nil))
		testingx.Expect(t, value, testingx.Be("2222"))
	})
}
