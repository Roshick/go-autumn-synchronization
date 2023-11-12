package periodictask

import (
	"time"

	"golang.org/x/net/context"
)

type Coordinator interface {
	ObtainLock(
		ctx context.Context,
		key string,
	) (Lock, error)

	LastRunDate(
		ctx context.Context,
		key string,
	) (*time.Time, error)

	UpdateLastRunDate(
		ctx context.Context,
		key string,
	) error
}

type Lock interface {
	Release(
		ctx context.Context,
	) error
}
