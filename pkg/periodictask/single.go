package periodictask

import (
	"errors"
	"fmt"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"

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
	TaskInterval    time.Duration
	TaskTimeout     time.Duration
	RunnerFrequency time.Duration
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
		TaskInterval:    60 * time.Minute,
		TaskTimeout:     10 * time.Minute,
		RunnerFrequency: 10 * time.Second,
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
			aulogging.Logger.Ctx(ctx).Error().
				WithErr(err).
				With(StackTraceLogField, string(debug.Stack())).
				Printf("periodic-task runner '%s' exited due to panic: %+v", r.taskKey, errors.New(fmt.Sprintf("%v", err)))
		}
	}()
	r.performTask(ctx)
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(r.config.RunnerFrequency):
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
		if rTime != nil && time.Now().Sub(*rTime) < r.config.TaskInterval {
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

	tCtx, cancel := context.WithTimeout(ctx, r.config.TaskTimeout)
	defer cancel()
	lCtx, _, err := r.coordinator.ObtainLock(tCtx, r.taskKey)
	if err != nil {
		aulogging.Logger.Ctx(lCtx).Warn().WithErr(err).Printf("failed to obtain lock for periodic-task '%s'", r.taskKey)
		return
	}

	if err = callback(); err != nil {
		aulogging.Logger.Ctx(lCtx).Warn().WithErr(err).Printf("failed to perform periodic-task '%s'", r.taskKey)
	}
}

func (r *PeriodicSingleTaskRunner) terminate() {
	r.mu.Lock()
	defer r.mu.Unlock()
	d, _ := r.done.Load().(chan struct{})
	if d == nil {
		d = make(chan struct{})
		close(d)
		r.done.Store(d)
	} else {
		close(d)
	}
}
