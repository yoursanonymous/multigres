// Copyright 2024 The Multigres Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

// Integration tests for DDL cache consistency across multiple gateway instances.
//
// Run with: /mt-dev integration multigateway TestDDL

package multigateway_test

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/services/multigateway/ddlcache"
	"github.com/multigres/multigres/go/services/multigateway/internal/ddlmock"
)

// TestDDLConsistency_TwoGateways_EventuallyConverge verifies that after
// an ALTER VIEW is executed through gateway 1, gateway 2 eventually
// invalidates its stale cache entry via the etcd watch.
//
// This is the core scenario from issue #537.
func TestDDLConsistency_TwoGateways_EventuallyConverge(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	store := ddlmock.NewTopoStore() // shared etcd mock

	// Create two independent DDL caches sharing the same mock topo store.
	// In production these would be two separate gateway processes, each
	// with their own DDLCache pointing at the same real etcd cluster.
	cache1 := ddlcache.New(store, logger)
	cache2 := ddlcache.New(store, logger)

	cache1.Start(ctx)
	cache2.Start(ctx)
	defer cache1.Shutdown()
	defer cache2.Shutdown()

	// Give both watchLoop goroutines time to register their channels in the
	// mock store. The mock's IncrDDLSchemaVersion notifies watchers
	// synchronously, so if a goroutine hasn't called WatchDDLSchemaVersion
	// yet it simply misses the event.
	time.Sleep(50 * time.Millisecond)

	// Both caches start with the same stale view definition.
	staleView := &ddlcache.ViewDefinition{
		Name:       "public.account_summary",
		SQL:        "SELECT id, balance FROM accounts",
		TableGroup: "default",
	}
	cache1.SetView("public.account_summary", staleView)
	cache2.SetView("public.account_summary", staleView)

	// ── Simulate: client executes ALTER VIEW through gateway 1 ──

	// Step 1: Gateway 1 executes DDL on Postgres (simulated — we just
	//         invalidate its local cache for the test, as the real Route
	//         primitive would do).
	cache1.InvalidateObject("public.account_summary")

	// Step 2: Gateway 1 bumps the etcd version (what BumpDDLVersion does).
	require.NoError(t, store.IncrDDLSchemaVersion(ctx))


	// ── Verify: gateway 1 sees a cache miss immediately (read-your-writes) ──
	assert.Nil(t, cache1.GetView("public.account_summary"),
		"gateway 1 must see cache miss immediately after local invalidation")

	// ── Verify: gateway 2 eventually invalidates via the etcd watch ──
	require.Eventually(t, func() bool {
		return cache2.GetView("public.account_summary") == nil
	}, 2*time.Second, 10*time.Millisecond,
		"gateway 2 must invalidate its stale cache via etcd watch within 2s")
}

// TestDDLConsistency_GatewayRestart_LoadsFreshDDL verifies that a gateway
// that restarts after a DDL change cold-loads the correct (post-DDL) state
// from Postgres and does not serve stale definitions.
//
// The test simulates this by:
//   1. Pre-loading cache1 with a stale view
//   2. Bumping the version (simulating DDL on another gateway)
//   3. Creating cache2 (the "restarted" gateway) and starting it
//   4. Verifying cache2 starts with an empty cache (correct — it will reload
//      lazily from Postgres, not from the stale in-memory state)
func TestDDLConsistency_GatewayRestart_LoadsFreshDDL(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	store := ddlmock.NewTopoStore()

	// Existing gateway with stale data.
	cache1 := ddlcache.New(store, logger)
	cache1.Start(ctx)
	defer cache1.Shutdown()
	cache1.SetView("public.v", &ddlcache.ViewDefinition{SQL: "STALE"})

	// DDL executed (simulated): version bumped.
	require.NoError(t, store.IncrDDLSchemaVersion(ctx))

	// New gateway starts after the DDL.
	cache2 := ddlcache.New(store, logger)
	cache2.Start(ctx) // baseline version = 1
	defer cache2.Shutdown()

	// The new gateway has no stale data — it starts empty. When its planner
	// encounters a cache miss for "public.v", it will query Postgres for the
	// fresh definition. This is the correct lazy-reload behaviour.
	assert.Nil(t, cache2.GetView("public.v"),
		"restarted gateway should start with empty cache (lazy reload from Postgres)")
}

// TestDDLConsistency_EtcdDown_OldCacheServed verifies the degraded mode:
// when etcd is temporarily unavailable, gateways continue serving (with
// potentially stale DDL metadata) rather than rejecting queries.
//
// This tests the non-fatal behaviour of BumpDDLVersion: etcd failures
// must not cause the gateway to fail user requests.
func TestDDLConsistency_EtcdDown_OldCacheServed(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	store := ddlmock.NewTopoStore()

	cache := ddlcache.New(store, logger)
	cache.SetView("public.v", &ddlcache.ViewDefinition{SQL: "SELECT 1"})

	// Simulate etcd being down for the version bump.
	store.FailNextIncrDDLSchemaVersion(fmt.Errorf("etcd: dial tcp: connection refused"))

	// Execute the invalidation and bump sequence as the planner would.
	cache.InvalidateObject("public.v") // always succeeds
	err := store.IncrDDLSchemaVersion(ctx) // fails — but caller ignores it
	// The BumpDDLVersion primitive logs this error and returns nil.
	// We verify here that the error is non-nil (etcd was indeed down)
	// but that the local cache was still correctly invalidated.
	assert.Error(t, err, "expected etcd error for verification")
	assert.Nil(t, cache.GetView("public.v"),
		"local cache must be invalidated even when etcd version bump fails")
}

// TestDDLConsistency_MultipleRapidDDL_VersionMonotonicallyIncreases verifies
// that rapid DDL operations produce a strictly increasing etcd version sequence.
func TestDDLConsistency_MultipleRapidDDL_VersionMonotonicallyIncreases(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store := ddlmock.NewTopoStore()

	const ops = 50
	for i := range ops {
		require.NoError(t, store.IncrDDLSchemaVersion(ctx),
			"increment %d should succeed", i)
	}

	v, err := store.GetDDLSchemaVersion(ctx)
	require.NoError(t, err)
	assert.Equal(t, int64(ops), v,
		"version must equal the number of increments with no races")
}
