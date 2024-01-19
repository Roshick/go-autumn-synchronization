package periodictask

import (
	"time"

	"github.com/Roshick/go-autumn-synchronisation/pkg/locker"
	"golang.org/x/net/context"
)

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
