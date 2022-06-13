package id

import (
	"time"

	"github.com/go-courier/snowflakeid"
	"github.com/go-courier/snowflakeid/workeridutil"
)

var startTime, _ = time.Parse(time.RFC3339, "2020-01-01T00:00:00Z")
var sff = snowflakeid.NewSnowflakeFactory(16, 8, 5, startTime)

func New() (Gen, error) {
	return sff.NewSnowflake(workeridutil.WorkerIDFromIP(ResolveExposedIP()))
}

type Gen interface {
	ID() (uint64, error)
}
