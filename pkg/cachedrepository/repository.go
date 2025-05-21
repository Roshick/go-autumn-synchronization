package cachedrepository

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"

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
	if reflect.ValueOf(locker).IsNil() {
		locker = nil
	}
	if reflect.ValueOf(hooks).IsNil() {
		hooks = nil
	}

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
		errs = append(errs, c.Reconcile(ctx, name))
	}
	return errors.Join(errs...)
}

func (c *CachedRepository[BaseEntity, ProcessedEntity, ChangeContext]) Reconcile(
	ctx context.Context,
	name string,
) error {
	callback := func(cCtx context.Context) error {
		aulogging.Logger.Ctx(cCtx).Debug().Printf("reconciling %s entity %s", c.key, name)

		entity, err := c.repository.Read(cCtx, name)
		if err != nil {
			if errors.As(err, &ErrRepositoryEntityNotFound{}) {
				aulogging.Logger.Ctx(cCtx).Debug().Printf("%s entity %s has been removed from the repository and will be removed from the cache", c.key, name)

				return c.performCacheAction(cCtx, name, nil, CacheActionCauseReconciliation)
			}
			return err
		}
		var processedEntity ProcessedEntity
		processedEntity, err = c.processor.ProcessEntity(cCtx, entity)
		if err != nil {
			aulogging.Logger.Ctx(cCtx).Warn().WithErr(err).
				Printf("failed to post-process %s entity '%s' during reconciliation, cache will be out of date until reconciliation", c.key, name)
			return err
		}
		return c.performCacheAction(cCtx, name, &processedEntity, CacheActionCauseReconciliation)
	}

	return c.synchronised(ctx, callback, name)
}

func (c *CachedRepository[BaseEntity, ProcessedEntity, ChangeContext]) Create(
	ctx context.Context,
	name string,
	entity BaseEntity,
	changeContext *ChangeContext,
) error {
	callback := func(cCtx context.Context) error {
		if err := c.repository.Create(cCtx, name, entity, changeContext); err != nil {
			return err
		}
		// read persisted entity again in case of changes done during write operation
		persistedEntity, err := c.repository.Read(ctx, name)
		if err != nil {
			return err
		}
		processedEntity, err := c.processor.ProcessEntity(cCtx, persistedEntity)
		if err != nil {
			aulogging.Logger.Ctx(cCtx).Warn().WithErr(err).
				Printf("failed to post-process %s entity '%s' after creation, cache will be out of date until reconciliation and hook cannot be performed", c.key, name)
			return err
		}
		if c.hooks != nil {
			if err = c.hooks.OnRepositoryEntityCreated(cCtx, name, processedEntity); err != nil {
				aulogging.Logger.Ctx(cCtx).Warn().WithErr(err).
					Printf("failed to perform hook for %s entity '%s' after creation, cache will be out of date until reconciliation", c.key, name)
				return err
			}
		}
		return c.performCacheAction(cCtx, name, &processedEntity, CacheActionCauseRepositoryAction)
	}

	return c.synchronised(ctx, callback, name)
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
	if cachedEntity, err := c.cache.Get(ctx, name); err != nil {
		return err
	} else if cachedEntity != nil {
		cachedBaseEntity, innerErr := c.processor.InverseProcessEntity(ctx, *cachedEntity)
		if innerErr != nil {
			return innerErr
		}
		if defaultCompareEqual(entity, cachedBaseEntity) {
			return nil
		}
	}

	callback := func(cCtx context.Context) error {
		persistedEntity, innerErr := c.repository.Read(cCtx, name)
		if innerErr != nil {
			return innerErr
		}
		currentHash, innerErr := defaultHash(persistedEntity)
		if innerErr != nil {
			return innerErr
		}
		if currentHash != modifyIfMatchHash {
			return NewErrHashMismatch(modifyIfMatchHash, currentHash)
		}

		if innerErr = c.repository.Update(cCtx, name, entity, changeContext); innerErr != nil {
			return innerErr
		}
		// read persisted entity again in case of changes done during write operation
		persistedEntity, innerErr = c.repository.Read(ctx, name)
		if innerErr != nil {
			return innerErr
		}
		processedEntity, innerErr := c.processor.ProcessEntity(cCtx, persistedEntity)
		if innerErr != nil {
			aulogging.Logger.Ctx(cCtx).Warn().WithErr(innerErr).
				Printf("failed to post-process %s entity '%s' after update, cache will be out of date until reconciliation and hook cannot be performed", c.key, name)
			return innerErr
		}
		if c.hooks != nil {
			if innerErr = c.hooks.OnRepositoryEntityUpdated(cCtx, name, processedEntity); innerErr != nil {
				aulogging.Logger.Ctx(cCtx).Warn().WithErr(innerErr).
					Printf("failed to perform hook for %s entity '%s' after update, cache will be out of date until reconciliation", c.key, name)
				return innerErr
			}
		}
		return c.performCacheAction(cCtx, name, &processedEntity, CacheActionCauseRepositoryAction)
	}

	if err := c.synchronised(ctx, callback, name); err != nil {
		if errors.As(err, new(ErrHashMismatch)) {
			if innerErr := c.Reconcile(ctx, name); innerErr != nil {
				aulogging.Logger.Ctx(ctx).Warn().WithErr(innerErr).
					Printf("failed to reconcile %s entity '%s' after hash mismatch, cache will be out of date until reconciliation", c.key, name)
			}
		}
		return err
	}
	return nil
}

func (c *CachedRepository[BaseEntity, ProcessedEntity, ChangeContext]) Delete(
	ctx context.Context,
	name string,
	changeContext *ChangeContext,
) error {
	callback := func(cCtx context.Context) error {
		if err := c.repository.Delete(cCtx, name, changeContext); err != nil {
			return err
		}
		if c.hooks != nil {
			if err := c.hooks.OnRepositoryEntityDeleted(cCtx, name); err != nil {
				aulogging.Logger.Ctx(cCtx).Warn().WithErr(err).
					Printf("failed to perform hook for %s entity '%s' after deletion, cache will be out of date until reconciliation", c.key, name)
			}
		}
		return c.performCacheAction(cCtx, name, nil, CacheActionCauseRepositoryAction)
	}

	return c.synchronised(ctx, callback, name)
}

func (c *CachedRepository[BaseEntity, ProcessedEntity, ChangeContext]) performCacheAction(
	ctx context.Context,
	name string,
	entity *ProcessedEntity,
	cause CacheActionCause,
) error {
	aulogging.Logger.Ctx(ctx).Debug().Printf("performing cache action caused by %s for %s entity %s", cause, c.key, name)

	cachedEntity, err := c.cache.Get(ctx, name)
	if err != nil {
		return err
	}
	if entity == nil && cachedEntity == nil {
		aulogging.Logger.Ctx(ctx).Debug().Printf("%s entity %s was removed from repository and is not part of cache, nothing to do", c.key, name)

		return nil
	} else if entity == nil && cachedEntity != nil {
		aulogging.Logger.Ctx(ctx).Debug().Printf("%s entity %s was removed from repository and is part of cache, removing from cache", c.key, name)

		if err = c.cache.Remove(ctx, name); err != nil {
			aulogging.Logger.Ctx(ctx).Warn().WithErr(err).
				Printf("failed to remove %s entity '%s' from cache, cache will be out of date until reconciliation", c.key, name)
			return err
		}
		aulogging.Logger.Ctx(ctx).Info().Printf("successfully removed %s entity '%s' from cache", c.key, name)
		if c.hooks != nil {
			if err = c.hooks.OnCacheEntityRemoved(ctx, name, cause); err != nil {
				aulogging.Logger.Ctx(ctx).Warn().WithErr(err).
					Printf("failed to perform cache hook for %s entity '%s' after removal", c.key, name)
				return err
			}
		}
	} else if entity != nil && cachedEntity == nil {
		aulogging.Logger.Ctx(ctx).Debug().Printf("%s entity %s is part of repository but currently not in cache, adding to cache", c.key, name)

		if err = c.cache.Set(ctx, name, *entity, 0); err != nil {
			aulogging.Logger.Ctx(ctx).Warn().WithErr(err).
				Printf("failed to cache %s entity %s", c.key, name)
		}
		aulogging.Logger.Ctx(ctx).Info().Printf("successfully added %s entity '%s' to cache", c.key, name)
		if c.hooks != nil {
			if err = c.hooks.OnCacheEntityAdded(ctx, name, *entity, cause); err != nil {
				aulogging.Logger.Ctx(ctx).Warn().WithErr(err).
					Printf("failed to perform cache hook for %s entity '%s' after addition", c.key, name)
			}
		}
	} else if !defaultCompareEqual(*entity, *cachedEntity) {
		aulogging.Logger.Ctx(ctx).Debug().Printf("%s entity %s is part of repository and part of cache but was changed, modifying in cache", c.key, name)

		if err = c.cache.Set(ctx, name, *entity, 0); err != nil {
			aulogging.Logger.Ctx(ctx).Warn().WithErr(err).
				Printf("failed to update %s entity %s in cache", c.key, name)
		}
		aulogging.Logger.Ctx(ctx).Info().Printf("successfully updated %s entity '%s' in cache", c.key, name)
		if c.hooks != nil {
			if err = c.hooks.OnCacheEntityUpdated(ctx, name, *entity, cause); err != nil {
				aulogging.Logger.Ctx(ctx).Warn().WithErr(err).
					Printf("failed to perform cache hook for %s entity '%s' after update", c.key, name)
			}
		}
	}
	return nil
}

func (c *CachedRepository[BaseEntity, ProcessedEntity, ChangeContext]) synchronised(
	ctx context.Context,
	callback func(context.Context) error,
	key string,
) error {
	if c.locker == nil {
		return callback(ctx)
	}

	lCtx, cancel, err := c.locker.ObtainLock(ctx, c.lockerKey(key))
	if err != nil {
		return err
	}
	defer cancel()

	return callback(lCtx)
}

func (c *CachedRepository[BaseEntity, ProcessedEntity, ChangeContext]) lockerKey(key string) string {
	return fmt.Sprintf("%s-%s-locker", c.key, key)
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
