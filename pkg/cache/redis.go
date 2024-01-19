package cache

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

type redisCache[Entity any] struct {
	rdb *redis.Client
	key string
}

func NewRedisCache[Entity any](
	redisURL string,
	redisPassword string,
	key string,
) Cache[Entity] {
	rdb := redis.NewClient(&redis.Options{
		Addr:     redisURL,
		Password: redisPassword,
		DB:       0,
	})

	return &redisCache[Entity]{
		rdb: rdb,
		key: key,
	}
}

func (c *redisCache[Entity]) Entries(
	ctx context.Context,
) (map[string]Entity, error) {
	keys, err := c.Keys(ctx)
	if err != nil {
		return nil, err
	}
	entries := make(map[string]Entity)
	for _, key := range keys {
		value, err := c.Get(ctx, key)
		if err != nil {
			return nil, err
		}
		if value != nil {
			entries[key] = *value
		}
	}
	return entries, nil
}

func (c *redisCache[Entity]) Keys(
	ctx context.Context,
) ([]string, error) {
	keysWithPrefix, err := c.rdb.Keys(ctx, c.entryKeyPattern()).Result()
	if errors.Is(err, redis.Nil) {
		return nil, nil
	} else if err != nil {
		return nil, err
	}

	keys := make([]string, 0)
	for _, keyWithPrefix := range keysWithPrefix {
		keys = append(keys, strings.TrimPrefix(keyWithPrefix, c.entryKeyPrefix()))
	}
	return keys, nil
}

func (c *redisCache[Entity]) Values(
	ctx context.Context,
) ([]Entity, error) {
	entries, err := c.Entries(ctx)
	if err != nil {
		return nil, err
	}
	values := make([]Entity, 0)
	for _, value := range entries {
		values = append(values, value)
	}
	return values, nil
}

func (c *redisCache[Entity]) Set(
	ctx context.Context,
	key string,
	value Entity,
	retention time.Duration,
) error {
	jsonBytes, err := json.Marshal(value)
	if err != nil {
		return err
	}

	return c.rdb.Set(ctx, c.entryKey(key), string(jsonBytes), retention).Err()
}

func (c *redisCache[Entity]) Get(
	ctx context.Context,
	key string,
) (*Entity, error) {
	jsonString, err := c.rdb.Get(ctx, c.entryKey(key)).Result()
	if errors.Is(err, redis.Nil) {
		return nil, nil
	} else if err != nil {
		return nil, err
	}

	var value Entity
	if err = json.Unmarshal([]byte(jsonString), &value); err != nil {
		return nil, err
	}
	return &value, nil
}

func (c *redisCache[Entity]) Remove(
	ctx context.Context,
	key string,
) error {
	return c.rdb.Del(ctx, c.entryKey(key)).Err()
}

func (c *redisCache[Entity]) RemainingRetention(
	ctx context.Context,
	key string,
) (time.Duration, error) {
	return c.rdb.TTL(ctx, c.entryKey(key)).Result()
}

func (c *redisCache[Entity]) entryKeyPrefix() string {
	return fmt.Sprintf("%s|", c.key)
}

func (c *redisCache[Entity]) entryKeyPattern() string {
	return fmt.Sprintf("%s*", c.entryKeyPrefix())
}

func (c *redisCache[Entity]) entryKey(key string) string {
	return fmt.Sprintf("%s%s", c.entryKeyPrefix(), key)
}
