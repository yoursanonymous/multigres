package ddlcache_test

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/services/multigateway/ddlcache"
	"github.com/multigres/multigres/go/services/multigateway/internal/ddlmock"
)

func noopLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func TestGetView_CacheMissReturnsNil(t *testing.T) {
	c := ddlcache.New(ddlmock.NewTopoStore(), noopLogger())
	assert.Nil(t, c.GetView("nonexistent"))
}

func TestSetAndGetView(t *testing.T) {
	c := ddlcache.New(ddlmock.NewTopoStore(), noopLogger())
	def := &ddlcache.ViewDefinition{
		Name:       "public.my_view",
		SQL:        "SELECT 1 AS val",
		TableGroup: "default",
	}
	c.SetView("public.my_view", def)
	got := c.GetView("public.my_view")
	require.NotNil(t, got)
	assert.Equal(t, "SELECT 1 AS val", got.SQL)
}

func TestInvalidateObject_ReadYourWrites(t *testing.T) {
	c := ddlcache.New(ddlmock.NewTopoStore(), noopLogger())
	c.SetView("public.v", &ddlcache.ViewDefinition{SQL: "SELECT 2"})
	require.NotNil(t, c.GetView("public.v"), "precondition: view must be cached")
	c.InvalidateObject("public.v")
	assert.Nil(t, c.GetView("public.v"), "view should be evicted immediately after InvalidateObject")
}

func TestInvalidateAll(t *testing.T) {
	c := ddlcache.New(ddlmock.NewTopoStore(), noopLogger())
	c.SetView("v1", &ddlcache.ViewDefinition{SQL: "SELECT 1"})
	c.SetView("v2", &ddlcache.ViewDefinition{SQL: "SELECT 2"})
	c.InvalidateAll()
	assert.Nil(t, c.GetView("v1"), "v1 should be evicted")
	assert.Nil(t, c.GetView("v2"), "v2 should be evicted")
}

func TestWatchInvalidatesOnVersionBump(t *testing.T) {
	store := ddlmock.NewTopoStore()
	c := ddlcache.New(store, noopLogger())
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	c.Start(ctx)
	defer c.Shutdown()

	c.SetView("public.my_view", &ddlcache.ViewDefinition{SQL: "SELECT 1"})
	require.NotNil(t, c.GetView("public.my_view"), "precondition: view must be cached")

	require.NoError(t, c.WaitReady(ctx)) // wait for watch loop

	err := store.IncrDDLSchemaVersion(ctx)
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		return c.GetView("public.my_view") == nil
	}, 2*time.Second, 10*time.Millisecond)
}

func TestWatchDoesNotInvalidateOnSameVersion(t *testing.T) {
	store := ddlmock.NewTopoStore()
	store.SetDDLSchemaVersion(5)
	c := ddlcache.New(store, noopLogger())
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	c.Start(ctx)
	defer c.Shutdown()

	c.SetView("v", &ddlcache.ViewDefinition{SQL: "SELECT 1"})
	require.NoError(t, c.WaitReady(ctx)) // wait for watch loop

	store.EmitDDLVersionEvent(ctx, 5)

	time.Sleep(50 * time.Millisecond)
	assert.NotNil(t, c.GetView("v"))
}

func TestColdStartRaceCondition(t *testing.T) {
	store := ddlmock.NewTopoStore()
	store.SetDDLSchemaVersion(5)
	c := ddlcache.New(store, noopLogger())
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	c.Start(ctx)
	defer c.Shutdown()

	require.NoError(t, c.WaitReady(ctx)) // wait for watch loop to start

	// Set the stale view BEFORE bumping the version. The mock's
	// IncrDDLSchemaVersion notifies watchers synchronously, so if we bumped
	// first the watchLoop could call InvalidateAll before SetView runs,
	// leaving the cache permanently empty and the Eventually check trivially
	// true for the wrong reason — or the view could sneak in after the
	// eviction and never be cleared.
	c.SetView("public.v", &ddlcache.ViewDefinition{SQL: "STALE_DEFINITION"})
	require.NoError(t, store.IncrDDLSchemaVersion(ctx))

	require.Eventually(t, func() bool {
		return c.GetView("public.v") == nil
	}, 2*time.Second, 10*time.Millisecond)
}
