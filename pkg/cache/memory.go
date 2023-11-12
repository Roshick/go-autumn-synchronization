package cache

import (
	"context"
	"math"
	"sync"
	"time"
)

type memoryCache[E any] struct {
	store sync.Map
}

func NewMemoryCache[E any]() Cache[E] {
	return &memoryCache[E]{store: sync.Map{}}
}

func (c *memoryCache[E]) Entries(
	_ context.Context,
) (map[string]E, error) {
	entries := make(map[string]E)
	c.store.Range(func(key, value any) bool {
		entries[key.(string)] = value.(E)
		return true
	})
	return entries, nil
}

func (c *memoryCache[E]) Keys(
	_ context.Context,
) ([]string, error) {
	keys := make([]string, 0)
	c.store.Range(func(key, value any) bool {
		keys = append(keys, key.(string))
		return true
	})
	return keys, nil
}

func (c *memoryCache[E]) Values(
	_ context.Context,
) ([]E, error) {
	values := make([]E, 0)
	c.store.Range(func(key, value any) bool {
		values = append(values, value.(E))
		return true
	})
	return values, nil
}

func (c *memoryCache[E]) Set(
	_ context.Context,
	key string,
	value E,
	_ time.Duration,
) error {
	c.store.Store(key, value)
	return nil
}

func (c *memoryCache[E]) Get(
	_ context.Context,
	key string,
) (*E, error) {
	value, ok := c.store.Load(key)
	if !ok {
		return nil, nil
	}
	castValue := value.(E)
	return &castValue, nil
}

func (c *memoryCache[E]) Remove(
	_ context.Context,
	key string,
) error {
	c.store.Delete(key)
	return nil
}

func (c *memoryCache[E]) RemainingRetention(
	_ context.Context,
	key string,
) (time.Duration, error) {
	_, ok := c.store.Load(key)
	if !ok {
		return 0, nil
	}
	return math.MaxInt64, nil
}
