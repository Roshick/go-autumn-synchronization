package auperiodictask

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Roshick/go-autumn-synchronisation/pkg/aulocker"
	aulogging "github.com/StephanHCB/go-autumn-logging"
	"golang.org/x/net/context"
)

type PeriodicSingleTaskRunner struct {
	taskKey     string
	taskFunc    func(context.Context) error
	coordinator Coordinator
	config      Config

	mu   sync.Mutex
	done atomic.Value
}

type Config struct {
	taskInterval    time.Duration
	taskTimeout     time.Duration
	runnerFrequency time.Duration
}

func NewSingleTaskRunner(
	ctx context.Context,
	taskKey string,
	taskFunc func(context.Context) error,
	coordinator Coordinator,
	config *Config,
) *PeriodicSingleTaskRunner {
	var vConfig Config
	if config != nil {
		vConfig = *config
	} else {
		vConfig = CreateDefaultConfig()
	}

	runner := &PeriodicSingleTaskRunner{
		taskKey:     taskKey,
		taskFunc:    taskFunc,
		coordinator: coordinator,
		config:      vConfig,
	}

	go runner.start(ctx)
	return runner
}

func CreateDefaultConfig() Config {
	return Config{
		taskInterval:    60 * time.Minute,
		taskTimeout:     10 * time.Minute,
		runnerFrequency: 10 * time.Second,
	}
}

// Done copied from context.Context
func (r *PeriodicSingleTaskRunner) Done() <-chan struct{} {
	d := r.done.Load()
	if d != nil {
		return d.(chan struct{})
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	d = r.done.Load()
	if d == nil {
		d = make(chan struct{})
		r.done.Store(d)
	}
	return d.(chan struct{})
}

func (r *PeriodicSingleTaskRunner) start(ctx context.Context) {
	defer r.terminate()
	defer func() {
		if err, ok := recover().(error); ok {
			aulogging.Logger.Ctx(ctx).Error().WithErr(err).Printf("periodic-task runner '%s' exited due to panic: %+v", r.taskKey, errors.New(fmt.Sprintf("%v", err)))
		}
	}()
	r.performTask(ctx)
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(r.config.runnerFrequency):
			r.performTask(ctx)
		}
	}
}

func (r *PeriodicSingleTaskRunner) performTask(
	ctx context.Context,
) {
	callback := func() error {
		rTime, err := r.coordinator.LastRunTimestamp(ctx, r.taskKey)
		if err != nil {
			return err
		}
		if rTime != nil && time.Now().Sub(*rTime) < r.config.taskInterval {
			return nil
		}

		aulogging.Logger.Ctx(ctx).Info().Printf("periodic-task '%s' last-run time threshold exceeded, executing task", r.taskKey)
		if err = r.taskFunc(ctx); err != nil {
			return err
		}

		if err = r.coordinator.UpdateLastRunTimestamp(ctx, r.taskKey); err != nil {
			return err
		}
		return nil
	}

	lock, err := r.coordinator.ObtainLock(ctx, r.taskKey)
	if err != nil {
		aulogging.Logger.Ctx(ctx).Warn().WithErr(err).Printf("failed to obtain lock for periodic-task '%s'", r.taskKey)
		return
	}
	if lock == nil {
		aulogging.Logger.Ctx(ctx).Warn().Printf("failed to obtain lock for periodic-task '%s' in time", r.taskKey)
		return
	}
	defer func(lock aulocker.Lock, ctx context.Context) {
		err := lock.Release(ctx)
		if err != nil {
			aulogging.Logger.Ctx(ctx).Warn().WithErr(err).Printf("failed to release lock for periodic-task '%s'", r.taskKey)
		}
	}(lock, ctx)
	if err := callback(); err != nil {
		aulogging.Logger.Ctx(ctx).Warn().WithErr(err).Printf("failed to perform periodic-task '%s'", r.taskKey)
	}
}

func (r *PeriodicSingleTaskRunner) terminate() {
	r.mu.Lock()
	defer r.mu.Unlock()
	d, _ := r.done.Load().(chan struct{})
	if d == nil {
		r.done.Store(make(chan struct{}))
	} else {
		close(d)
	}
}
