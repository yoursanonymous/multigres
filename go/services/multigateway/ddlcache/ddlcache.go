package ddlcache

import (
	"context"
	"github.com/multigres/multigres/go/common/topoclient"
	"log/slog"
	"sync"
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
	version         int64
	lastInvalidated *time.Time
	topoStore       topoclient.Store
	logger          *slog.Logger
	cancel          context.CancelFunc
	wg              sync.WaitGroup
}

func New(store topoclient.Store, logger *slog.Logger) *DDLCache {
	return &DDLCache{
		views:     make(map[string]*ViewDefinition),
		topoStore: store,
		logger:    logger,
	}
}
func (c *DDLCache) Start(ctx context.Context) {
	v, err := c.topoStore.GetDDLSchemaVersion(ctx)
	if err != nil {
		c.logger.Warn("failed to get ddl version", "err", err)
	}
	c.version = v

	ctx, cancel := context.WithCancel(ctx)
	c.cancel = cancel

	c.wg.Add(1)
	go c.watchLoop(ctx)

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

	ch, err := c.topoStore.WatchDDLSchemaVersion(ctx)
	if err != nil {
		c.logger.Error("failed to start DDL watch", "err", err)
		return
	}

	for {
		select {
		case <-ctx.Done():
			return
		case newVersion, ok := <-ch:
			if !ok {
				c.logger.Warn("watch closed")
				return
			}
			if newVersion > c.version {
				c.InvalidateAll()
				c.version = newVersion
			}
		}
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
	if v > c.version {
		c.InvalidateAll()
		c.version = v
	}
}
func (c *DDLCache) InvalidateAll() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.views = make(map[string]*ViewDefinition)
	now := time.Now()
	c.lastInvalidated = &now
}

func (c *DDLCache) InvalidateObject(name string) {
	if name == "" {
		c.InvalidateAll()
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.views, name)
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
		Version:         c.version,
		ViewCount:       len(c.views),
		LastInvalidated: c.lastInvalidated,
	}
}
