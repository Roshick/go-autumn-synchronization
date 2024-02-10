package cache

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	aulogging "github.com/StephanHCB/go-autumn-logging"
	"github.com/redis/rueidis"
)

type rueidisCache[Entity any] struct {
	client rueidis.Client
	key    string
}

func NewRueidisCache[Entity any](
	redisURL string,
	redisPassword string,
	key string,
) (Cache[Entity], error) {
	client, err := rueidis.NewClient(rueidis.ClientOption{
		InitAddress: []string{redisURL},
		Password:    redisPassword,
	})
	if err != nil {
		return nil, err
	}

	return &rueidisCache[Entity]{
		client: client,
		key:    key,
	}, nil
}

func (c *rueidisCache[Entity]) Entries(
	ctx context.Context,
) (map[string]Entity, error) {
	aulogging.Logger.Ctx(ctx).Debug().Printf("fetching all entries from cache '%s'", c.key)
	keys, err := c.Keys(ctx)
	if err != nil {
		return nil, err
	}
	entries := make(map[string]Entity)
	for _, key := range keys {
		value, innerErr := c.Get(ctx, key)
		if innerErr != nil {
			return nil, innerErr
		}
		if value != nil {
			entries[key] = *value
		}
	}
	return entries, nil
}

func (c *rueidisCache[Entity]) Keys(
	ctx context.Context,
) ([]string, error) {
	aulogging.Logger.Ctx(ctx).Debug().Printf("fetching all keys from cache '%s'", c.key)
	result := c.client.Do(ctx, c.client.B().Keys().Pattern(c.entryKeyPattern()).Build())
	if err := result.Error(); err != nil {
		if rueidis.IsRedisNil(err) {
			return nil, nil
		} else if err != nil {
			return nil, err
		}
	}

	keysWithPrefix, err := result.AsStrSlice()
	if err != nil {
		return nil, err
	}

	keys := make([]string, 0)
	for _, keyWithPrefix := range keysWithPrefix {
		keys = append(keys, strings.TrimPrefix(keyWithPrefix, c.entryKeyPrefix()))
	}
	return keys, nil
}

func (c *rueidisCache[Entity]) Values(
	ctx context.Context,
) ([]Entity, error) {
	aulogging.Logger.Ctx(ctx).Debug().Printf("fetching all values from cache '%s'", c.key)
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

func (c *rueidisCache[Entity]) Set(
	ctx context.Context,
	key string,
	value Entity,
	retention time.Duration,
) error {
	aulogging.Logger.Ctx(ctx).Debug().Printf("setting value of '%s' in cache '%s'", key, c.key)
	jsonBytes, err := json.Marshal(value)
	if err != nil {
		return err
	}

	cmd := c.client.B().Set().Key(c.entryKey(key)).Value(string(jsonBytes))
	if retention > 0 {
		cmd.Ex(retention)
	}

	return c.client.Do(ctx, cmd.Build()).Error()
}

func (c *rueidisCache[Entity]) Get(
	ctx context.Context,
	key string,
) (*Entity, error) {
	aulogging.Logger.Ctx(ctx).Debug().Printf("fetching value of '%s' from cache '%s'", key, c.key)
	result := c.client.Do(ctx, c.client.B().Get().Key(c.entryKey(key)).Build())
	if err := result.Error(); err != nil {
		if rueidis.IsRedisNil(err) {
			return nil, nil
		} else if err != nil {
			return nil, err
		}
	}

	jsonString, err := result.ToString()
	if err != nil {
		return nil, err
	}

	var value Entity
	if err := json.Unmarshal([]byte(jsonString), &value); err != nil {
		return nil, err
	}
	return &value, nil
}

func (c *rueidisCache[Entity]) Remove(
	ctx context.Context,
	key string,
) error {
	aulogging.Logger.Ctx(ctx).Debug().Printf("removing value of '%s' from cache '%s'", key, c.key)
	return c.client.Do(ctx, c.client.B().Del().Key(c.entryKey(key)).Build()).Error()
}

func (c *rueidisCache[Entity]) RemainingRetention(
	ctx context.Context,
	key string,
) (time.Duration, error) {
	aulogging.Logger.Ctx(ctx).Debug().Printf("fetching remaining retention of '%s' cache '%s'", key, c.key)
	result := c.client.Do(ctx, c.client.B().Ttl().Key(c.entryKey(key)).Build())
	if err := result.Error(); err != nil {
		return 0, err
	}

	ttlInMillis, err := result.AsInt64()
	if err != nil {
		return 0, err
	}
	return time.Millisecond * time.Duration(ttlInMillis), nil
}

func (c *rueidisCache[Entity]) entryKeyPrefix() string {
	return fmt.Sprintf("%s|", c.key)
}

func (c *rueidisCache[Entity]) entryKeyPattern() string {
	return fmt.Sprintf("%s*", c.entryKeyPrefix())
}

func (c *rueidisCache[Entity]) entryKey(key string) string {
	return fmt.Sprintf("%s%s", c.entryKeyPrefix(), key)
}
