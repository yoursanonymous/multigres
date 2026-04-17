// Copyright 2026 The Multigres Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

package engine_test

import (
	"context"
	"testing"
	

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/pgprotocol/server"
	"github.com/multigres/multigres/go/services/multigateway/ddlcache"
	"github.com/multigres/multigres/go/services/multigateway/engine"
	"github.com/multigres/multigres/go/services/multigateway/handler"
	"github.com/multigres/multigres/go/common/sqltypes"
	"github.com/multigres/multigres/go/common/preparedstatement"
	multipoolerpb "github.com/multigres/multigres/go/pb/multipoolerservice"
	querypb "github.com/multigres/multigres/go/pb/query"
	"github.com/multigres/multigres/go/services/multigateway/internal/ddlmock"
	"log/slog"
	"io"
)

type fakeIExecute struct{
	RouteCalled bool
	TxnCalled   bool
	Queries     []string
}

func (f *fakeIExecute) StreamExecute(ctx context.Context, conn *server.Conn, tableGroup string, shard string, sql string, state *handler.MultiGatewayConnectionState, callback func(context.Context, *sqltypes.Result) error) error {
	f.RouteCalled = true
	f.Queries = append(f.Queries, sql)
	return nil
}

func (f *fakeIExecute) ConcludeTransaction(context.Context, *server.Conn, *handler.MultiGatewayConnectionState, multipoolerpb.TransactionConclusion, func(context.Context, *sqltypes.Result) error) error { return nil }
func (f *fakeIExecute) DiscardTempTables(context.Context, *server.Conn, *handler.MultiGatewayConnectionState, func(context.Context, *sqltypes.Result) error) error { return nil }
func (f *fakeIExecute) ReleaseAllReservedConnections(context.Context, *server.Conn, *handler.MultiGatewayConnectionState) error { return nil }
func (f *fakeIExecute) PortalStreamExecute(context.Context, string, string, *server.Conn, *handler.MultiGatewayConnectionState, *preparedstatement.PortalInfo, int32, func(context.Context, *sqltypes.Result) error) error { return nil }
func (f *fakeIExecute) Describe(context.Context, string, string, *server.Conn, *handler.MultiGatewayConnectionState, *preparedstatement.PortalInfo, *preparedstatement.PreparedStatementInfo) (*querypb.StatementDescription, error) { return nil, nil }
func (f *fakeIExecute) CopyInitiate(context.Context, *server.Conn, string, string, string, *handler.MultiGatewayConnectionState, func(context.Context, *sqltypes.Result) error) (int16, []int16, error) { return 0, nil, nil }
func (f *fakeIExecute) CopySendData(context.Context, *server.Conn, string, string, *handler.MultiGatewayConnectionState, []byte) error { return nil }
func (f *fakeIExecute) CopyFinalize(context.Context, *server.Conn, string, string, *handler.MultiGatewayConnectionState, []byte, func(context.Context, *sqltypes.Result) error) error { return nil }
func (f *fakeIExecute) CopyAbort(context.Context, *server.Conn, string, string, *handler.MultiGatewayConnectionState) error { return nil }

func TestDDLPrimitives_SequenceExecutesAll(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	store := ddlmock.NewTopoStore()
	cache := ddlcache.New(store, logger)
	
	cache.SetView("public.v", &ddlcache.ViewDefinition{Name: "public.v"})
	require.NotNil(t, cache.GetView("public.v"))

	route := engine.NewRoute("default", "", "CREATE VIEW public.v AS SELECT 1")
	invalidate := engine.NewInvalidateDDLCache(cache, "public.v")
	bump := engine.NewBumpDDLVersion(store, logger)

	seq := engine.NewSequence([]engine.Primitive{route, invalidate, bump})
	exec := &fakeIExecute{}
	
	err := seq.StreamExecute(context.Background(), exec, nil, nil, nil)
	require.NoError(t, err)

	assert.True(t, exec.RouteCalled)
	assert.Nil(t, cache.GetView("public.v"), "Cache should be invalidated")
	
	// Check the version bumped
	version, _ := store.GetDDLSchemaVersion(context.Background())
	assert.Equal(t, int64(1), version)
}
