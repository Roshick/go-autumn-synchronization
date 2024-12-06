package periodictask

import (
	"time"

	"github.com/Roshick/go-autumn-synchronisation/pkg/locker"
	"golang.org/x/net/context"
)

var StackTraceLogField = "stack-trace"

type Coordinator interface {
	locker.Locker

	LastRunTimestamp(
		ctx context.Context,
		key string,
	) (*time.Time, error)

	UpdateLastRunTimestamp(
		ctx context.Context,
		key string,
	) error
}
