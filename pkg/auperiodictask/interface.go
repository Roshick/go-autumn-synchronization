package auperiodictask

import (
	"time"

	"github.com/Roshick/go-autumn-synchronisation/pkg/aulocker"
	"golang.org/x/net/context"
)

type Coordinator interface {
	aulocker.Locker

	LastRunTimestamp(
		ctx context.Context,
		key string,
	) (*time.Time, error)

	UpdateLastRunTimestamp(
		ctx context.Context,
		key string,
	) error
}
