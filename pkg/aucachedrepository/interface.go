package aucachedrepository

import (
	"golang.org/x/net/context"
)

type Repository[Entity any, ChangeContext any] interface {
	Create(
		ctx context.Context,
		name string,
		entity Entity,
		changeContext *ChangeContext,
	) error

	ReadAll(
		ctx context.Context,
	) (map[string]Entity, error)

	Read(
		ctx context.Context,
		name string,
	) (Entity, error)

	Update(
		ctx context.Context,
		name string,
		entity Entity,
		changeContext *ChangeContext,
	) error

	Delete(
		ctx context.Context,
		name string,
		changeContext *ChangeContext,
	) error
}

type CacheActionCause int64

const (
	CacheActionCauseRepositoryAction CacheActionCause = iota
	CacheActionCauseReconciliation
)

type Hooks[Entity any] interface {
	OnRepositoryEntityCreated(
		ctx context.Context,
		name string,
		entity Entity,
	) error

	OnRepositoryEntityUpdated(
		ctx context.Context,
		name string,
		entity Entity,
	) error

	OnRepositoryEntityDeleted(
		ctx context.Context,
		name string,
	) error

	OnCacheEntityAdded(
		ctx context.Context,
		name string,
		entity Entity,
		cause CacheActionCause,
	) error

	OnCacheEntityUpdated(
		ctx context.Context,
		name string,
		entity Entity,
		cause CacheActionCause,
	) error

	OnCacheEntityRemoved(
		ctx context.Context,
		name string,
		cause CacheActionCause,
	) error
}

type Processor[BaseEntity any, ProcessedEntity any] interface {
	ProcessEntity(
		ctx context.Context,
		entity BaseEntity,
	) (ProcessedEntity, error)

	InverseProcessEntity(
		ctx context.Context,
		entity ProcessedEntity,
	) (BaseEntity, error)
}

// ToDo: Use later for custom hash
type CustomHash interface {
	Hash() (string, error)
}

// ToDo: Use later for custom compare
type CustomCompare interface {
	IsEqual(other any) (bool, error)
}
