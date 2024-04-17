package cache

import (
	"time"

	"golang.org/x/net/context"
)

type Cache[Entity any] interface {
	Entries(
		ctx context.Context,
	) (map[string]Entity, error)

	Keys(
		ctx context.Context,
	) ([]string, error)

	Values(
		ctx context.Context,
	) ([]Entity, error)

	Set(
		ctx context.Context,
		key string,
		value Entity,
		retention time.Duration,
	) error

	Get(
		ctx context.Context,
		key string,
	) (*Entity, error)

	Remove(
		ctx context.Context,
		key string,
	) error

	RemainingRetention(
		ctx context.Context,
		key string,
	) (time.Duration, error)

	Flush(
		ctx context.Context,
	) error
}
