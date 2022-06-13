package tree_test

import (
	"testing"

	"github.com/octohelm/kiwidb/internal/tree"
	"github.com/octohelm/kiwidb/pkg/testutil"
	. "github.com/octohelm/x/testing"
)

func TestTree(t *testing.T) {
	t.Run("Given namespace 10", func(t *testing.T) {
		tt := testutil.NewTree(t, 10)

		t.Run("Given put key1, but not set key1", func(t *testing.T) {
			key1 := tree.NewKey(true, 12345678)
			key2 := tree.NewKey(true, 2)

			err := tt.Put(key1, []byte{1})
			Expect(t, err, Be[error](nil))

			t.Run("key1 could found", func(t *testing.T) {
				v, err := tt.Get(key1)
				Expect(t, err, Be[error](nil))
				Expect(t, v, Equal([]byte{1}))
			})

			t.Run("key2 could not found", func(t *testing.T) {
				_, err := tt.Get(key2)
				Expect(t, err, Not(Be[error](nil)))
			})
		})
	})

	t.Run("Given namespace 10 and put 10 values", func(t *testing.T) {
		tt := testutil.NewTree(t, 10)

		for i := -3; i <= 3; i++ {
			key := tree.NewKey(i)
			err := tt.Put(key, []byte{1})
			Expect(t, err, Be[error](nil))
		}

		t.Run("range exists", func(t *testing.T) {
			rng := tree.NewRange(tree.NewKey(2), tree.NewKey(2), false)
			expectRangeGot(t, tt, rng, false, []int32{2})
		})

		t.Run("range all", func(t *testing.T) {
			expectRangeGot(t, tt, nil, false, []int32{-3, -2, -1, 0, 1, 2, 3})
		})

		t.Run("reverse range all", func(t *testing.T) {
			expectRangeGot(t, tt, nil, true, []int32{3, 2, 1, 0, -1, -2, -3})
		})

		t.Run("range from < -1", func(t *testing.T) {
			rng := tree.NewRange(nil, tree.NewKey(-1), true)
			expectRangeGot(t, tt, rng, false, []int32{-3, -2})
		})

		t.Run("reverse range from < -1", func(t *testing.T) {
			rng := tree.NewRange(nil, tree.NewKey(-1), true)
			expectRangeGot(t, tt, rng, true, []int32{-2, -3})
		})

		t.Run("range from <= -1", func(t *testing.T) {
			rng := tree.NewRange(nil, tree.NewKey(-1), false)
			expectRangeGot(t, tt, rng, false, []int32{-3, -2, -1})
		})

		t.Run("range from > 1", func(t *testing.T) {
			rng := tree.NewRange(tree.NewKey(1), nil, true)
			expectRangeGot(t, tt, rng, false, []int32{2, 3})
		})

		t.Run("range from >= 1", func(t *testing.T) {
			rng := tree.NewRange(tree.NewKey(1), nil, false)
			expectRangeGot(t, tt, rng, false, []int32{1, 2, 3})
		})

		t.Run("range from between -1 to 1", func(t *testing.T) {
			rng := tree.NewRange(tree.NewKey(-1), tree.NewKey(1), false)
			expectRangeGot(t, tt, rng, false, []int32{-1, 0, 1})
		})

		t.Run("range from between -1 to 1 exclusive", func(t *testing.T) {
			rng := tree.NewRange(tree.NewKey(-1), tree.NewKey(1), true)
			expectRangeGot(t, tt, rng, false, []int32{0})
		})
	})
}

func expectRangeGot(t testing.TB, tt *tree.Tree, rng tree.Range, reverse bool, got []int32) {
	values := make([]int32, 0)
	err := tt.Range(rng, reverse, func(key tree.Key, data []byte) error {
		values = append(values, key.Values()[0].(int32))
		return nil
	})
	Expect(t, err, Be[error](nil))
	Expect(t, values, Equal(got))
}
