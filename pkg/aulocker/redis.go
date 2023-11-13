package aulocker

import (
	"errors"
	"time"

	"github.com/bsm/redislock"
	"github.com/redis/go-redis/v9"
	"golang.org/x/net/context"
)

type redisLocker struct {
	locker        *redislock.Client
	lockRetention time.Duration
}

type redisLock struct {
	lock *redislock.Lock
}

func NewRedisLocker(
	redisURL string,
	redisPassword string,
) Locker {
	rdb := redis.NewClient(&redis.Options{
		Addr:     redisURL,
		Password: redisPassword,
		DB:       0,
	})
	locker := redislock.New(rdb)

	return &redisLocker{
		locker: locker,
	}
}

func (l *redisLocker) ObtainLock(
	ctx context.Context,
	key string,
) (Lock, error) {
	lock, err := l.locker.Obtain(ctx, key, l.lockRetention, &redislock.Options{
		RetryStrategy: redislock.LinearBackoff(10 * time.Second),
	})
	if err != nil {
		if errors.Is(err, redislock.ErrNotObtained) {
			return nil, nil
		}
		return nil, err
	}

	return &redisLock{
		lock: lock,
	}, nil
}

func (l *redisLock) Release(
	ctx context.Context,
) error {
	return l.lock.Release(ctx)
}
