package cache

import (
	"context"
	"math"
	"sync"
	"time"
)

type memoryCache[Entity any] struct {
	store sync.Map
}

func NewMemoryCache[Entity any]() Cache[Entity] {
	return &memoryCache[Entity]{store: sync.Map{}}
}

func (c *memoryCache[Entity]) Entries(
	_ context.Context,
) (map[string]Entity, error) {
	entries := make(map[string]Entity)
	c.store.Range(func(key, value any) bool {
		entries[key.(string)] = value.(Entity)
		return true
	})
	return entries, nil
}

func (c *memoryCache[Entity]) Keys(
	_ context.Context,
) ([]string, error) {
	keys := make([]string, 0)
	c.store.Range(func(key, value any) bool {
		keys = append(keys, key.(string))
		return true
	})
	return keys, nil
}

func (c *memoryCache[Entity]) Values(
	_ context.Context,
) ([]Entity, error) {
	values := make([]Entity, 0)
	c.store.Range(func(key, value any) bool {
		values = append(values, value.(Entity))
		return true
	})
	return values, nil
}

func (c *memoryCache[Entity]) Set(
	_ context.Context,
	key string,
	value Entity,
	_ time.Duration,
) error {
	c.store.Store(key, value)
	return nil
}

func (c *memoryCache[Entity]) Get(
	_ context.Context,
	key string,
) (*Entity, error) {
	value, ok := c.store.Load(key)
	if !ok {
		return nil, nil
	}
	castValue := value.(Entity)
	return &castValue, nil
}

func (c *memoryCache[Entity]) Remove(
	_ context.Context,
	key string,
) error {
	c.store.Delete(key)
	return nil
}

func (c *memoryCache[Entity]) RemainingRetention(
	_ context.Context,
	key string,
) (time.Duration, error) {
	_, ok := c.store.Load(key)
	if !ok {
		return 0, nil
	}
	return math.MaxInt64, nil
}
