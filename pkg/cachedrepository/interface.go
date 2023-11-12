package cachedrepository

import (
	"golang.org/x/net/context"
)

type Repository[E any, C any] interface {
	Create(
		ctx context.Context,
		name string,
		entity E,
		changeContext *C,
	) error

	ReadAll(
		ctx context.Context,
	) (map[string]E, error)

	Read(
		ctx context.Context,
		name string,
	) (E, error)

	Update(
		ctx context.Context,
		name string,
		entity E,
		changeContext *C,
	) error

	Delete(
		ctx context.Context,
		name string,
		changeContext *C,
	) error
}

type CacheActionCause int64

const (
	CacheActionCauseRepositoryAction CacheActionCause = iota
	CacheActionCauseReconciliation
)

type Hooks[E any] interface {
	OnRepositoryEntityCreated(
		ctx context.Context,
		name string,
		entity E,
	) error

	OnRepositoryEntityUpdated(
		ctx context.Context,
		name string,
		entity E,
	) error

	OnRepositoryEntityDeleted(
		ctx context.Context,
		name string,
	) error

	OnCacheEntityAdded(
		ctx context.Context,
		name string,
		entity E,
		cause CacheActionCause,
	) error

	OnCacheEntityUpdated(
		ctx context.Context,
		name string,
		entity E,
		cause CacheActionCause,
	) error

	OnCacheEntityRemoved(
		ctx context.Context,
		name string,
		cause CacheActionCause,
	) error
}

type Processor[B any, P any] interface {
	ProcessEntity(
		ctx context.Context,
		entity B,
	) (P, error)

	InverseProcessEntity(
		ctx context.Context,
		entity P,
	) (B, error)
}

// ToDo: Use later for custom hash
type CustomHash interface {
	Hash() (string, error)
}

// ToDo: Use later for custom compare
type CustomCompare interface {
	IsEqual(other any) (bool, error)
}
