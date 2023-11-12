package periodictask

import (
	"errors"
	"fmt"
	"time"

	aulogging "github.com/StephanHCB/go-autumn-logging"
	"golang.org/x/net/context"
)

type PeriodicSingleTaskRunner struct {
	taskKey     string
	taskFunc    func(context.Context) error
	coordinator Coordinator
	config      Config
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
) PeriodicSingleTaskRunner {
	var vConfig Config
	if config != nil {
		vConfig = *config
	} else {
		vConfig = CreateDefaultConfig()
	}

	runner := PeriodicSingleTaskRunner{
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

func (r *PeriodicSingleTaskRunner) start(ctx context.Context) {
	defer func() {
		if err, ok := recover().(error); ok {
			aulogging.Logger.Ctx(ctx).Error().WithErr(err).Printf("periodic-task runner %s exited due to panic: %+v", r.taskKey, errors.New(fmt.Sprintf("%v", err)))
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

		aulogging.Logger.Ctx(ctx).Info().Printf("periodic-task %s last-run time threshold exceeded, executing task", r.taskKey)
		if err = r.taskFunc(ctx); err != nil {
			return err
		}

		if err = r.coordinator.UpdateLastTimestamp(ctx, r.taskKey); err != nil {
			return err
		}
		return nil
	}

	lock, err := r.coordinator.ObtainLock(ctx, r.taskKey)
	if err != nil {
		aulogging.Logger.Ctx(ctx).Warn().WithErr(err).Printf("failed to obtain lock for periodic-task %s due to error", r.taskKey)
		return
	}
	if lock == nil {
		aulogging.Logger.Ctx(ctx).Warn().Print("failed to obtain lock for periodic-task %s in time", r.taskKey)
		return
	}
	defer func(lock Lock, ctx context.Context) {
		err := lock.Release(ctx)
		if err != nil {
			aulogging.Logger.Ctx(ctx).Warn().WithErr(err).Printf("failed to release lock for periodic-task %s", r.taskKey)
		}
	}(lock, ctx)
	if err := callback(); err != nil {
		aulogging.Logger.Ctx(ctx).Warn().WithErr(err).Printf("failed to perform periodic-task %s", r.taskKey)
	}
}
