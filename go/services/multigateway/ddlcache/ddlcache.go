package ddlcache

import (
	"context"
	"fmt"
	"github.com/multigres/multigres/go/common/topoclient"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"
)

type ViewDefinition struct {
	Name       string
	SQL        string
	TableGroup string
}

type CacheStatus struct {
	Version         int64
	ViewCount       int
	LastInvalidated *time.Time
}

type DDLCache struct {
	mu              sync.RWMutex
	views           map[string]*ViewDefinition
	version         atomic.Int64
	lastInvalidated atomic.Pointer[time.Time]
	topoStore       topoclient.Store
	logger          *slog.Logger
	cancel          context.CancelFunc
	wg              sync.WaitGroup
	ready           chan struct{}
}

func New(store topoclient.Store, logger *slog.Logger) *DDLCache {
	if logger == nil {
		logger = slog.Default()
	}
	return &DDLCache{
		views:     make(map[string]*ViewDefinition),
		topoStore: store,
		logger:    logger,
		ready:     make(chan struct{}),
	}
}

func (c *DDLCache) Start(ctx context.Context) {
	ctx, cancel := context.WithCancel(ctx)
	c.cancel = cancel

	// Start watch FIRST before reading version,
	// so we don't miss events between read and watch registration.
	c.wg.Add(1)
	go c.watchLoop(ctx)

	// Now read the baseline — if version already advanced, the watch
	// will also deliver the new value; we take the max.
	v, err := c.topoStore.GetDDLSchemaVersion(ctx)
	if err != nil {
		c.logger.Warn("failed to get initial DDL version", "err", err)
	} else {
		c.version.Store(v)
	}

	c.wg.Add(1)
	go c.periodicCheck(ctx)
}

func (c *DDLCache) Shutdown() {
	if c.cancel != nil {
		c.cancel()
	}
	c.wg.Wait()
}

func (c *DDLCache) watchLoop(ctx context.Context) {
	defer c.wg.Done()

	backoff := 100 * time.Millisecond
	const maxBackoff = 30 * time.Second

	for {
		if ctx.Err() != nil {
			return
		}
		if err := c.runWatch(ctx); err != nil {
			if ctx.Err() != nil {
				return // normal shutdown
			}
			c.logger.Error("DDL watch failed, retrying",
				"err", err, "backoff", backoff)
			select {
			case <-time.After(backoff):
			case <-ctx.Done():
				return
			}
			backoff = min(backoff*2, maxBackoff)
		} else {
			backoff = 100 * time.Millisecond // reset on clean exit
		}
	}
}

func (c *DDLCache) runWatch(ctx context.Context) error {
	ch, err := c.topoStore.WatchDDLSchemaVersion(ctx)
	if err != nil {
		return fmt.Errorf("WatchDDLSchemaVersion: %w", err)
	}
	
	// Signal ready after watch is registered
	select {
	case <-c.ready: // already closed
	default:
		close(c.ready)
	}

	for {
		select {
		case <-ctx.Done():
			return nil
		case newVersion, ok := <-ch:
			if !ok {
				return fmt.Errorf("watch channel closed unexpectedly")
			}
			if newVersion > c.version.Load() {
				c.InvalidateAll()
				c.version.Store(newVersion)
				c.logger.Info("DDL cache invalidated via watch",
					"new_version", newVersion)
			}
		}
	}
}

// WaitReady blocks until the watch goroutine is registered.
// Use in tests instead of time.Sleep.
func (c *DDLCache) WaitReady(ctx context.Context) error {
	select {
	case <-c.ready:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (c *DDLCache) periodicCheck(ctx context.Context) {
	defer c.wg.Done()
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			c.refresh(ctx)
		}
	}
}

func (c *DDLCache) refresh(ctx context.Context) {
	v, err := c.topoStore.GetDDLSchemaVersion(ctx)
	if err != nil {
		c.logger.Warn("failed to get version", "err", err)
		return
	}
	if v > c.version.Load() {
		c.InvalidateAll()
		c.version.Store(v)
	}
}

func (c *DDLCache) InvalidateAll() {
	c.mu.Lock()
	c.views = make(map[string]*ViewDefinition)
	c.mu.Unlock()
	now := time.Now()
	c.lastInvalidated.Store(&now)
}

func (c *DDLCache) InvalidateObject(name string) {
	if name == "" {
		c.InvalidateAll()
		return
	}
	c.mu.Lock()
	delete(c.views, name)
	c.mu.Unlock()
	now := time.Now()
	c.lastInvalidated.Store(&now)
}

func (c *DDLCache) GetView(name string) *ViewDefinition {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.views[name]
}

func (c *DDLCache) SetView(name string, def *ViewDefinition) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.views[name] = def
}

func (c *DDLCache) GetStatus() CacheStatus {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return CacheStatus{
		Version:         c.version.Load(),
		ViewCount:       len(c.views),
		LastInvalidated: c.lastInvalidated.Load(),
	}
}
