package cache

import (
	"time"

	"golang.org/x/net/context"
)

type Cache[E any] interface {
	Entries(
		ctx context.Context,
	) (map[string]E, error)

	Keys(
		ctx context.Context,
	) ([]string, error)

	Values(
		ctx context.Context,
	) ([]E, error)

	Set(
		ctx context.Context,
		key string,
		value E,
		retention time.Duration,
	) error

	Get(
		ctx context.Context,
		key string,
	) (*E, error)

	Remove(
		ctx context.Context,
		key string,
	) error

	RemainingRetention(
		ctx context.Context,
		key string,
	) (time.Duration, error)
}
