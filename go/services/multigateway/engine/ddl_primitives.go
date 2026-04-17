// Copyright 2026 The Multigres Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

package engine

// NEW FILE: go/services/multigateway/engine/ddl_primitives.go
//
// This file adds two engine primitives for DDL cache consistency:
//
//   InvalidateDDLCache — evicts a single named object from the local cache.
//                        Used for the read-your-writes path: runs synchronously
//                        on the gateway that executed the DDL.
//
//   BumpDDLVersion     — increments the etcd DDL schema version key.
//                        Triggers InvalidateAll on all other gateways via their
//                        etcd watch goroutines.
//                        NON-FATAL: etcd failures are logged, never returned.
//
// Both implement the engine.Primitive interface and are composed via the
// existing Sequence primitive in the planner's ddl_stmt.go.

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/multigres/multigres/go/common/pgprotocol/server"
	"github.com/multigres/multigres/go/services/multigateway/ddlcache"
	"github.com/multigres/multigres/go/services/multigateway/handler"
	"github.com/multigres/multigres/go/common/topoclient"
	"github.com/multigres/multigres/go/common/sqltypes"
)

// DDLCacheInvalidator is a Primitive that evicts a named DDL object from the
// local gateway cache after the DDL statement has been successfully executed
// on Postgres.
//
// This is the read-your-writes primitive: it runs synchronously on the
// issuing gateway so that subsequent queries on the same connection
// immediately see the updated DDL definition.
//
// It never returns an error. A cache eviction failure is impossible (it is
// just a map delete under a mutex), and a cache miss is safe because callers
// reload from Postgres on nil.
type DDLCacheInvalidator struct {
	cache      *ddlcache.DDLCache
	objectName string
}

// NewInvalidateDDLCache creates a DDLCacheInvalidator for the given object name.
// objectName should be the fully-qualified name (e.g. "public.my_view").
func NewInvalidateDDLCache(cache *ddlcache.DDLCache, objectName string) *DDLCacheInvalidator {
	return &DDLCacheInvalidator{
		cache:      cache,
		objectName: objectName,
	}
}

// StreamExecute implements Primitive. It evicts the named object from the
// local DDL cache. No rows are produced; the callback is never called.
func (d *DDLCacheInvalidator) StreamExecute(
	_ context.Context,
	_ IExecute,
	_ *server.Conn,
	_ *handler.MultiGatewayConnectionState,
	_ func(context.Context, *sqltypes.Result) error,
) error {
	d.cache.InvalidateObject(d.objectName)
	return nil
}

// GetTableGroup implements Primitive. DDL cache operations are not
// table-group-scoped at this level.
func (d *DDLCacheInvalidator) GetTableGroup() string { return "" }

// GetQuery implements Primitive.
func (d *DDLCacheInvalidator) GetQuery() string {
	return fmt.Sprintf("/* invalidate ddl cache: %s */", d.objectName)
}

// String implements Primitive.
func (d *DDLCacheInvalidator) String() string {
	return fmt.Sprintf("InvalidateDDLCache(%s)", d.objectName)
}

// DDLVersionBumper is a Primitive that increments the global etcd DDL schema
// version key after a DDL statement has been executed on Postgres.
//
// Incrementing the version key causes all other gateways watching it to
// call InvalidateAll on their local DDL caches.
//
// This primitive is INTENTIONALLY NON-FATAL. If the etcd write fails, the
// error is logged as a warning but not returned to the caller. The rationale:
//
//   - The DDL itself has already succeeded on Postgres — failing the user's
//     request at this point would be incorrect and confusing.
//   - Other gateways may temporarily serve stale results, but the window is
//     bounded: the next successful DDL bump will trigger invalidation.
//   - The alternative (blocking DDL on etcd availability) would couple gateway
//     availability to etcd availability, violating the design principle that
//     gateways must not participate in the 2PC commit path.
type DDLVersionBumper struct {
	topoStore topoclient.Store
	logger    *slog.Logger
}

// NewBumpDDLVersion creates a DDLVersionBumper.
func NewBumpDDLVersion(topoStore topoclient.Store, logger *slog.Logger) *DDLVersionBumper {
	return &DDLVersionBumper{
		topoStore: topoStore,
		logger:    logger,
	}
}

// StreamExecute implements Primitive. It increments the etcd DDL schema
// version key. Errors are logged but not returned.
func (d *DDLVersionBumper) StreamExecute(
	ctx context.Context,
	_ IExecute,
	_ *server.Conn,
	_ *handler.MultiGatewayConnectionState,
	_ func(context.Context, *sqltypes.Result) error,
) error {
	if err := d.topoStore.IncrDDLSchemaVersion(ctx); err != nil {
		// Non-fatal: log a warning. Other gateways may briefly serve stale DDL
		// definitions. The window closes on the next successful version bump.
		d.logger.WarnContext(ctx,
			"ddl version bump failed; remote gateway caches may be temporarily stale",
			"err", err)
	}
	return nil
}

// GetTableGroup implements Primitive.
func (d *DDLVersionBumper) GetTableGroup() string { return "" }

// GetQuery implements Primitive.
func (d *DDLVersionBumper) GetQuery() string { return "/* bump ddl schema version */" }

// String implements Primitive.
func (d *DDLVersionBumper) String() string { return "BumpDDLVersion" }
