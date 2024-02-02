package locker

import (
	"golang.org/x/net/context"
	"sync"
)

type memoryLocker struct {
	locks sync.Map
}

func NewMemoryLocker() Locker {
	return &memoryLocker{}
}

func (l *memoryLocker) ObtainLock(ctx context.Context, key string) (context.Context, context.CancelFunc, error) {
	ctx, cancel := context.WithCancel(ctx)
	for {
		if _, locked := l.locks.LoadOrStore(key, true); !locked {
			go l.lock(ctx, key, cancel)
			break
		}
		select {
		case <-ctx.Done():
			return ctx, cancel, nil
		default:
		}
	}
	return ctx, cancel, nil
}

func (l *memoryLocker) lock(ctx context.Context, key string, cancel context.CancelFunc) {
	<-ctx.Done()
	cancel()
	l.locks.Delete(key)
}
