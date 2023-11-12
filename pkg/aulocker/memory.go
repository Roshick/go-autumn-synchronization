package locker

import (
	"sync"
	"time"

	"github.com/bsm/redislock"
	"golang.org/x/net/context"
)

type memoryLocker struct {
	locks         sync.Map
	lockRetention time.Duration
}

type memoryLock struct {
	key            string
	retentionTimer *time.Timer
	locker         *memoryLocker
}

func NewMemoryLocker() Locker {
	return &memoryLocker{}
}

func (l *memoryLocker) Setup(
	_ context.Context,
) error {
	return nil
}

func (l *memoryLocker) ObtainLock(
	ctx context.Context,
	key string,
) (Lock, error) {
	retry := redislock.LinearBackoff(10 * time.Second)
	var ticker *time.Ticker
	for {
		if _, locked := l.locks.LoadOrStore(key, true); !locked {
			return newMemoryLock(key, l), nil
		}

		backoff := retry.NextBackoff()
		if backoff < 1 {
			return nil, nil
		}

		if ticker == nil {
			ticker = time.NewTicker(backoff)
			defer ticker.Stop()
		} else {
			ticker.Reset(backoff)
		}

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-ticker.C:
		}
	}
}

func newMemoryLock(
	key string,
	locker *memoryLocker,
) *memoryLock {
	retentionTimer := time.AfterFunc(locker.lockRetention, func() {
		locker.locks.Delete(key)
	})

	return &memoryLock{
		key:            key,
		retentionTimer: retentionTimer,
		locker:         locker,
	}
}

func (l *memoryLock) Release(
	_ context.Context,
) error {
	l.retentionTimer.Stop()
	l.locker.locks.Delete(l.key)
	return nil
}
