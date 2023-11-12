package cachedrepository

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/Roshick/go-autumn-synchronisation/pkg/cache"
	"github.com/Roshick/go-autumn-synchronisation/pkg/locker"
	aulogging "github.com/StephanHCB/go-autumn-logging"
	"golang.org/x/net/context"
)

type CachedRepository[BaseEntity any, ProcessedEntity any, ChangeContext any] struct {
	key        string
	repository Repository[BaseEntity, ChangeContext]
	cache      cache.Cache[ProcessedEntity]
	processor  Processor[BaseEntity, ProcessedEntity]
	locker     locker.Locker
	hooks      Hooks[ProcessedEntity]
}

func NewCachedRepository[BaseEntity any, ProcessedEntity any, ChangeContext any](
	key string,
	repository Repository[BaseEntity, ChangeContext],
	cache cache.Cache[ProcessedEntity],
	processor Processor[BaseEntity, ProcessedEntity],
	locker locker.Locker,
	hooks Hooks[ProcessedEntity],
) *CachedRepository[BaseEntity, ProcessedEntity, ChangeContext] {
	return &CachedRepository[BaseEntity, ProcessedEntity, ChangeContext]{
		key:        key,
		cache:      cache,
		repository: repository,
		processor:  processor,
		locker:     locker,
		hooks:      hooks,
	}
}

func (c *CachedRepository[BaseEntity, ProcessedEntity, ChangeContext]) ReconcileAll(
	ctx context.Context,
) error {
	callback := func() error {
		entities, err := c.repository.ReadAll(ctx)
		if err != nil {
			return err
		}
		cachedEntities, err := c.cache.Entries(ctx)
		if err != nil {
			return err
		}
		names := make(map[string]bool)
		for name := range entities {
			names[name] = true
		}
		for name := range cachedEntities {
			names[name] = true
		}
		errs := make([]error, 0)
		for name := range names {
			if entity, ok := entities[name]; !ok {
				errs = append(errs, c.performCacheAction(ctx, name, nil, CacheActionCauseReconciliation))
			} else {
				var processedEntity ProcessedEntity
				processedEntity, err = c.processor.ProcessEntity(ctx, entity)
				if err != nil {
					aulogging.Logger.Ctx(ctx).Warn().WithErr(err).
						Printf("failed to post-process %s entity %s during full reconciliation, " +
							"cache will be out of date until reconciliation")
					errs = append(errs, err)
				}
				errs = append(errs, c.performCacheAction(ctx, name, &processedEntity, CacheActionCauseReconciliation))
			}
		}
		return errors.Join(errs...)
	}

	return c.synchronised(ctx, callback)
}

func (c *CachedRepository[BaseEntity, ProcessedEntity, ChangeContext]) Reconcile(
	ctx context.Context,
	name string,
) error {
	callback := func() error {
		entity, err := c.repository.Read(ctx, name)
		if err != nil {
			if errors.Is(err, &ErrRepositoryEntityNotFound{}) {
				return c.performCacheAction(ctx, name, nil, CacheActionCauseReconciliation)
			}
			return err
		}
		var processedEntity ProcessedEntity
		processedEntity, err = c.processor.ProcessEntity(ctx, entity)
		if err != nil {
			aulogging.Logger.Ctx(ctx).Warn().WithErr(err).
				Printf("failed to post-process %s entity %s during reconciliation, " +
					"cache will be out of date until reconciliation")
			return err
		}
		return c.performCacheAction(ctx, name, &processedEntity, CacheActionCauseReconciliation)
	}

	return c.synchronised(ctx, callback)
}

func (c *CachedRepository[BaseEntity, ProcessedEntity, ChangeContext]) Create(
	ctx context.Context,
	name string,
	entity BaseEntity,
	changeContext *ChangeContext,
) error {
	callback := func() error {
		if err := c.repository.Create(ctx, name, entity, changeContext); err != nil {
			return err
		}
		processedEntity, err := c.processor.ProcessEntity(ctx, entity)
		if err != nil {
			aulogging.Logger.Ctx(ctx).Warn().WithErr(err).
				Printf("failed to post-process %s entity %s after creation, " +
					"cache will be out of date until reconciliation and hook cannot be performed")
			return err
		}
		if c.hooks != nil {
			if err = c.hooks.OnRepositoryEntityCreated(ctx, name, processedEntity); err != nil {
				aulogging.Logger.Ctx(ctx).Warn().WithErr(err).
					Printf("failed to perform hook for %s entity %s after creation, " +
						"cache will be out of date until reconciliation")
				return err
			}
		}
		return c.performCacheAction(ctx, name, &processedEntity, CacheActionCauseRepositoryAction)
	}

	return c.synchronised(ctx, callback)
}

func (c *CachedRepository[BaseEntity, ProcessedEntity, ChangeContext]) ReadAll(
	ctx context.Context,
) (map[string]ProcessedEntity, error) {
	cachedEntities, err := c.cache.Entries(ctx)
	if err != nil {
		return nil, err
	}
	if len(cachedEntities) > 0 {
		return cachedEntities, nil
	}

	if err = c.ReconcileAll(ctx); err != nil {
		return nil, err
	}
	return c.cache.Entries(ctx)
}

func (c *CachedRepository[BaseEntity, ProcessedEntity, ChangeContext]) Read(
	ctx context.Context,
	name string,
) (ProcessedEntity, string, error) {
	cachedEntity, err := c.cache.Get(ctx, name)
	if err != nil || cachedEntity == nil {
		if err = c.Reconcile(ctx, name); err != nil {
			return *new(ProcessedEntity), "", err
		}
		cachedEntity, err = c.cache.Get(ctx, name)
		if err != nil {
			return *new(ProcessedEntity), "", err
		}
		if cachedEntity == nil {
			return *new(ProcessedEntity), "", NewErrRepositoryEntityNotFound(name)
		}
	}

	baseEntity, err := c.processor.InverseProcessEntity(ctx, *cachedEntity)
	if err != nil {
		return *new(ProcessedEntity), "", err
	}
	// IMPORTANT: compute hash of _base_ entity
	hash, err := defaultHash(baseEntity)
	if err != nil {
		return *new(ProcessedEntity), "", err
	}
	return *cachedEntity, hash, nil
}

func (c *CachedRepository[BaseEntity, ProcessedEntity, ChangeContext]) Update(
	ctx context.Context,
	name string,
	entity BaseEntity,
	modifyIfMatchHash string,
	changeContext *ChangeContext,
) error {
	callback := func() error {
		persistedEntity, err := c.repository.Read(ctx, name)
		if err != nil {
			return err
		}
		currentHash, err := defaultHash(persistedEntity)
		if err != nil {
			return err
		}
		if currentHash != modifyIfMatchHash {
			return NewErrHashMismatch(modifyIfMatchHash, currentHash)
		}

		if err = c.repository.Update(ctx, name, entity, changeContext); err != nil {
			return err
		}
		processedEntity, err := c.processor.ProcessEntity(ctx, entity)
		if err != nil {
			aulogging.Logger.Ctx(ctx).Warn().WithErr(err).
				Printf("failed to post-process %s entity %s after update, " +
					"cache will be out of date until reconciliation and hook cannot be performed")
			return err
		}
		if c.hooks != nil {
			if err = c.hooks.OnRepositoryEntityUpdated(ctx, name, processedEntity); err != nil {
				aulogging.Logger.Ctx(ctx).Warn().WithErr(err).
					Printf("failed to perform hook for %s entity %s after update, " +
						"cache will be out of date until reconciliation")
				return err
			}
		}
		return c.performCacheAction(ctx, name, &processedEntity, CacheActionCauseRepositoryAction)
	}

	return c.synchronised(ctx, callback)
}

func (c *CachedRepository[BaseEntity, ProcessedEntity, ChangeContext]) Delete(
	ctx context.Context,
	name string,
	changeContext *ChangeContext,
) error {
	callback := func() error {
		if err := c.repository.Delete(ctx, name, changeContext); err != nil {
			return err
		}
		if c.hooks != nil {
			if err := c.hooks.OnRepositoryEntityDeleted(ctx, name); err != nil {
				aulogging.Logger.Ctx(ctx).Warn().WithErr(err).
					Printf("failed to perform hook for %s entity %s after deletion, " +
						"cache will be out of date until reconciliation")
			}
		}
		return c.performCacheAction(ctx, name, nil, CacheActionCauseRepositoryAction)
	}

	return c.synchronised(ctx, callback)
}

func (c *CachedRepository[BaseEntity, ProcessedEntity, ChangeContext]) performCacheAction(
	ctx context.Context,
	name string,
	entity *ProcessedEntity,
	cause CacheActionCause,
) error {
	cachedEntity, err := c.cache.Get(ctx, name)
	if err != nil {
		return err
	}
	if entity == nil && cachedEntity == nil {
		return nil
	} else if entity == nil && cachedEntity != nil {
		if err = c.cache.Remove(ctx, name); err != nil {
			aulogging.Logger.Ctx(ctx).Warn().WithErr(err).
				Printf("failed to remove %s entity %s from cache, "+
					"cache will be out of date until reconciliation", c.key, name)
			return err
		}
		aulogging.Logger.Ctx(ctx).Info().Printf("successfully removed %s entity %s from cache")
		if c.hooks != nil {
			if err = c.hooks.OnCacheEntityRemoved(ctx, name, cause); err != nil {
				aulogging.Logger.Ctx(ctx).Warn().WithErr(err).
					Printf("failed to perform cache hook for %s entity %s after removal", c.key, name)
				return err
			}
		}
	} else if entity != nil && cachedEntity == nil {
		if err = c.cache.Set(ctx, name, *entity, 0); err != nil {
			aulogging.Logger.Ctx(ctx).Warn().WithErr(err).
				Printf("failed to cache %s entity %s", c.key, name)
		}
		aulogging.Logger.Ctx(ctx).Info().Printf("successfully added %s entity %s to cache", c.key, name)
		if c.hooks != nil {
			if err = c.hooks.OnCacheEntityAdded(ctx, name, *entity, cause); err != nil {
				aulogging.Logger.Ctx(ctx).Warn().WithErr(err).
					Printf("failed to perform cache hook for %s entity %s after addition", c.key, name)
			}
		}
	} else if !defaultCompareEqual(*entity, *cachedEntity) {
		if err = c.cache.Set(ctx, name, *entity, 0); err != nil {
			aulogging.Logger.Ctx(ctx).Warn().WithErr(err).
				Printf("failed to update %s entity %s in cache", c.key, name)
		}
		aulogging.Logger.Ctx(ctx).Info().Printf("successfully updated %s entity %s in cache")
		if c.hooks != nil {
			if err = c.hooks.OnCacheEntityUpdated(ctx, name, *entity, cause); err != nil {
				aulogging.Logger.Ctx(ctx).Warn().WithErr(err).
					Printf("failed to perform cache hook for %s entity %s after update", c.key, name)
			}
		}
	}
	return nil
}

func (c *CachedRepository[BaseEntity, ProcessedEntity, ChangeContext]) synchronised(
	ctx context.Context,
	callback func() error,
) error {
	if c.locker == nil {
		return callback()
	}

	lock, err := c.locker.ObtainLock(ctx, c.lockerKey())
	if err != nil {
		return err
	}
	defer func(lock locker.Lock, ctx context.Context) {
		err := lock.Release(ctx)
		if err != nil {
			aulogging.Logger.Ctx(ctx).Warn().WithErr(err).Printf("failed to unlock %s", c.lockerKey())
		}
	}(lock, ctx)
	return callback()
}

func (c *CachedRepository[BaseEntity, ProcessedEntity, ChangeContext]) lockerKey() string {
	return fmt.Sprintf("%s-locker", c.key)
}

func defaultCompareEqual(
	a any,
	b any,
) bool {
	jsonBytesA, err := json.Marshal(a)
	if err != nil {
		return false
	}
	jsonBytesB, err := json.Marshal(b)
	if err != nil {
		return false
	}
	return bytes.Equal(jsonBytesA, jsonBytesB)
}

func defaultHash(
	data any,
) (string, error) {
	jsonBytes, err := json.Marshal(data)
	if err != nil {
		return "", err
	}
	algorithm := sha256.New()
	_, err = algorithm.Write(jsonBytes)
	if err != nil {
		return "", err
	}
	return hex.EncodeToString(algorithm.Sum(nil)), nil
}
