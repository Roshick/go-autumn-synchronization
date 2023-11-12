package locker

import (
	"golang.org/x/net/context"
)

type Locker interface {
	ObtainLock(
		ctx context.Context,
		key string,
	) (Lock, error)
}

type Lock interface {
	Release(
		ctx context.Context,
	) error
}
