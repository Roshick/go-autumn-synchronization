package locker

import (
	"github.com/redis/rueidis"
	"github.com/redis/rueidis/rueidislock"
	"golang.org/x/net/context"
)

type redisLocker struct {
	locker rueidislock.Locker
}

func NewRedisLocker(
	redisURL string,
	redisPassword string,
) (Locker, error) {
	locker, err := rueidislock.NewLocker(rueidislock.LockerOption{
		ClientOption: rueidis.ClientOption{
			InitAddress: []string{redisURL},
			Password:    redisPassword},
		KeyMajority: 2,
	})

	if err != nil {
		return nil, err
	}

	return &redisLocker{
		locker: locker,
	}, nil
}

func (l *redisLocker) ObtainLock(
	ctx context.Context,
	key string,
) (context.Context, context.CancelFunc, error) {
	return l.locker.WithContext(ctx, key)
}
