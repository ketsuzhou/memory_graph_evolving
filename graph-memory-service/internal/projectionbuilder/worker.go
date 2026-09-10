package projectionbuilder

import (
	"context"
	"fmt"
	"sync"
	"time"

	"river2.dev/graph-memory-service/internal/consolidation"
	"river2.dev/graph-memory-service/internal/domain"
)

type WorkerConfig struct {
	PollInterval  time.Duration
	Consolidation domain.Config
	OnError       func(error)
}

func DefaultWorkerConfig() WorkerConfig {
	return WorkerConfig{PollInterval: time.Minute, Consolidation: consolidation.DefaultConfig()}
}

// Worker polls every shared and private space, starts at most one build for a
// space at a time, and can be stopped without abandoning in-flight builds.
type Worker struct {
	store   Store
	builder *Builder
	config  WorkerConfig

	mu      sync.Mutex
	running map[string]struct{}
	cancel  context.CancelFunc
	wg      sync.WaitGroup
}

func NewWorker(store Store, builder *Builder, config WorkerConfig) (*Worker, error) {
	if store == nil || builder == nil {
		return nil, fmt.Errorf("projectionbuilder: worker store and builder are required")
	}
	defaults := DefaultWorkerConfig()
	if config.PollInterval == 0 {
		config.PollInterval = defaults.PollInterval
	}
	if config.Consolidation == (domain.Config{}) {
		config.Consolidation = defaults.Consolidation
	}
	if config.PollInterval <= 0 || config.Consolidation.TriggerCommittedBatches <= 0 || config.Consolidation.TriggerQueries <= 0 {
		return nil, fmt.Errorf("projectionbuilder: worker interval and activity thresholds must be positive")
	}
	return &Worker{store: store, builder: builder, config: config, running: make(map[string]struct{})}, nil
}

func (w *Worker) Start(parent context.Context) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.cancel != nil {
		return fmt.Errorf("projectionbuilder: worker is already running")
	}
	ctx, cancel := context.WithCancel(parent)
	w.cancel = cancel
	w.wg.Add(1)
	go w.loop(ctx)
	return nil
}

func (w *Worker) Stop() {
	w.mu.Lock()
	cancel := w.cancel
	w.cancel = nil
	w.mu.Unlock()
	if cancel != nil {
		cancel()
		w.wg.Wait()
	}
}

func (w *Worker) loop(ctx context.Context) {
	defer w.wg.Done()
	w.poll(ctx)
	ticker := time.NewTicker(w.config.PollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			w.poll(ctx)
		}
	}
}

func (w *Worker) poll(ctx context.Context) {
	spaces, err := w.store.ListProjectionSpaces(ctx)
	if err != nil {
		w.report(err)
		return
	}
	for _, space := range spaces {
		state, err := w.store.ProjectionBuildState(ctx, space.TenantID, space.ID)
		if err != nil {
			w.report(err)
			continue
		}
		if !consolidation.ShouldConsolidate(state.Activity.CommittedBatchOrdinal, state.Activity.QueryOrdinal, w.config.Consolidation) {
			continue
		}
		w.startBuild(ctx, space)
	}
}

func (w *Worker) startBuild(ctx context.Context, space domain.Space) {
	key := string(space.TenantID) + "\x00" + string(space.ID)
	w.mu.Lock()
	if _, exists := w.running[key]; exists {
		w.mu.Unlock()
		return
	}
	w.running[key] = struct{}{}
	w.wg.Add(1)
	w.mu.Unlock()

	go func() {
		defer w.wg.Done()
		defer func() {
			w.mu.Lock()
			delete(w.running, key)
			w.mu.Unlock()
		}()
		if _, _, err := w.builder.BuildSpace(ctx, space.TenantID, space.ID); err != nil && ctx.Err() == nil {
			w.report(err)
		}
	}()
}

func (w *Worker) report(err error) {
	if w.config.OnError != nil {
		w.config.OnError(err)
	}
}
