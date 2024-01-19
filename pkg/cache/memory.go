package aucache

import (
	"context"
	"encoding/json"
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
	var firstError error
	c.store.Range(func(key, value any) bool {
		vPtr, err := unmarshal[Entity](value.(string))
		if err != nil {
			firstError = err
			return false
		}
		entries[key.(string)] = *vPtr
		return true
	})
	return entries, firstError
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
	var firstError error
	c.store.Range(func(key, value any) bool {
		vPtr, err := unmarshal[Entity](value.(string))
		if err != nil {
			firstError = err
			return false
		}
		values = append(values, *vPtr)
		return true
	})
	return values, firstError
}

func (c *memoryCache[Entity]) Set(
	_ context.Context,
	key string,
	value Entity,
	_ time.Duration,
) error {
	jsonBytes, err := json.Marshal(value)
	if err != nil {
		return err
	}
	c.store.Store(key, string(jsonBytes))
	return nil
}

func (c *memoryCache[Entity]) Get(
	_ context.Context,
	key string,
) (*Entity, error) {
	jsonString, ok := c.store.Load(key)
	if !ok {
		return nil, nil
	}
	return unmarshal[Entity](jsonString.(string))
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

func unmarshal[Entity any](jsonString string) (*Entity, error) {
	var value Entity
	if err := json.Unmarshal([]byte(jsonString), &value); err != nil {
		return nil, err
	}
	return &value, nil
}
