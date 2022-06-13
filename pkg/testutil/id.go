package testutil

import (
	"testing"

	"github.com/octohelm/kiwidb/pkg/id"
	. "github.com/octohelm/x/testing"
)

func NewIDGen(t testing.TB) id.Gen {
	t.Helper()
	gen, err := id.New()
	Expect(t, err, Be[error](nil))
	return gen
}
