package cache

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

type redisCache[E any] struct {
	rdb *redis.Client
	key string
}

func NewRedisCache[E any](
	redisURL string,
	redisPassword string,
	key string,
) Cache[E] {
	rdb := redis.NewClient(&redis.Options{
		Addr:     redisURL,
		Password: redisPassword,
		DB:       0,
	})

	return &redisCache[E]{
		rdb: rdb,
		key: key,
	}
}

func (c *redisCache[E]) Entries(
	ctx context.Context,
) (map[string]E, error) {
	keys, err := c.Keys(ctx)
	if err != nil {
		return nil, err
	}
	entries := make(map[string]E)
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

func (c *redisCache[E]) Keys(
	ctx context.Context,
) ([]string, error) {
	keysWithPrefix, err := c.rdb.Keys(ctx, c.entryKeyPattern()).Result()
	if err == redis.Nil {
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

func (c *redisCache[E]) Values(
	ctx context.Context,
) ([]E, error) {
	entries, err := c.Entries(ctx)
	if err != nil {
		return nil, err
	}
	values := make([]E, 0)
	for _, value := range entries {
		values = append(values, value)
	}
	return values, nil
}

func (c *redisCache[E]) Set(
	ctx context.Context,
	key string,
	value E,
	retention time.Duration,
) error {
	jsonBytes, err := json.Marshal(value)
	if err != nil {
		return err
	}

	return c.rdb.Set(ctx, c.entryKey(key), string(jsonBytes), retention).Err()
}

func (c *redisCache[E]) Get(
	ctx context.Context,
	key string,
) (*E, error) {
	jsonString, err := c.rdb.Get(ctx, c.entryKey(key)).Result()
	if err == redis.Nil {
		return nil, nil
	} else if err != nil {
		return nil, err
	}

	var value E
	if err = json.Unmarshal([]byte(jsonString), &value); err != nil {
		return nil, err
	}
	return &value, nil
}

func (c *redisCache[E]) Remove(
	ctx context.Context,
	key string,
) error {
	return c.rdb.Del(ctx, key).Err()
}

func (c *redisCache[E]) RemainingRetention(
	ctx context.Context,
	key string,
) (time.Duration, error) {
	return c.rdb.TTL(ctx, key).Result()
}

func (c *redisCache[E]) entryKeyPrefix() string {
	return fmt.Sprintf("%s|", c.key)
}

func (c *redisCache[E]) entryKeyPattern() string {
	return fmt.Sprintf("%s*", c.entryKeyPrefix())
}

func (c *redisCache[E]) entryKey(key string) string {
	return fmt.Sprintf("%s%s", c.entryKeyPrefix(), key)
}
