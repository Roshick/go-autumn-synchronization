package locker

import (
	"golang.org/x/net/context"
)

type Locker interface {
	ObtainLock(
		ctx context.Context,
		key string,
	) (context.Context, context.CancelFunc, error)
}
