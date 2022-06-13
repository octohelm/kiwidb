package testutil

import (
	"encoding/json"
	"os"
	"testing"

	. "github.com/octohelm/x/testing"
)

func PrintJSON(t testing.TB, v any) {
	t.Helper()

	e := json.NewEncoder(os.Stdout)
	e.SetIndent("", "  ")
	err := e.Encode(v)
	Expect(t, err, Be[error](nil))
}
