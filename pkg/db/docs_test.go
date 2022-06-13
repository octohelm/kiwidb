package db

import (
	"context"
	"testing"

	"github.com/octohelm/kiwidb/internal/database"
	testingx "github.com/octohelm/x/testing"
)

func TestStream(t *testing.T) {
	s := database.Pipe(
		Omit(
			database.DocumentFrom(map[string]any{"a": 1, "b": 1}),
			database.DocumentFrom(map[string]any{"a": 2, "b": 1}),
			database.DocumentFrom(map[string]any{"a": 1, "b": 2}),
		),
		Filter("a", Eq[int32](1)),
		Offset(1),
		Limit(1),
	)

	t.Log(database.Stringify(s))

	count := 0

	err := s.Iterate(database.NewStateWithContext(context.Background()), func(state State) error {
		d := state.Document()
		var m = map[string]any{}
		_ = d.Unmarshal(&m)
		testingx.Expect(t, m, testingx.Equal(map[string]any{"a": int32(1), "b": int32(2)}))
		count++
		return nil
	})
	testingx.Expect(t, err, testingx.Be[error](nil))
	testingx.Expect(t, count, testingx.Be(1))
}
