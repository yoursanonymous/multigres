// Copyright 2026 The Multigres Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0


package planner

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/parser/ast"
	"github.com/multigres/multigres/go/common/pgprotocol/server"
	"github.com/multigres/multigres/go/services/multigateway/ddlcache"
	"github.com/multigres/multigres/go/services/multigateway/engine"
	"github.com/multigres/multigres/go/services/multigateway/handler"
	"github.com/multigres/multigres/go/common/sqltypes"
	"github.com/multigres/multigres/go/services/multigateway/internal/ddlmock"
	"github.com/multigres/multigres/go/common/preparedstatement"
	multipoolerpb "github.com/multigres/multigres/go/pb/multipoolerservice"
	querypb "github.com/multigres/multigres/go/pb/query"
	"log/slog"
	"io"
)

type fakeIExecute struct{}

func (f *fakeIExecute) StreamExecute(context.Context, *server.Conn, string, string, string, *handler.MultiGatewayConnectionState, func(context.Context, *sqltypes.Result) error) error { return nil }
func (f *fakeIExecute) ConcludeTransaction(context.Context, *server.Conn, *handler.MultiGatewayConnectionState, multipoolerpb.TransactionConclusion, func(context.Context, *sqltypes.Result) error) error { return nil }
func (f *fakeIExecute) DiscardTempTables(context.Context, *server.Conn, *handler.MultiGatewayConnectionState, func(context.Context, *sqltypes.Result) error) error { return nil }
func (f *fakeIExecute) ReleaseAllReservedConnections(context.Context, *server.Conn, *handler.MultiGatewayConnectionState) error { return nil }
func (f *fakeIExecute) PortalStreamExecute(context.Context, string, string, *server.Conn, *handler.MultiGatewayConnectionState, *preparedstatement.PortalInfo, int32, func(context.Context, *sqltypes.Result) error) error { return nil }
func (f *fakeIExecute) Describe(context.Context, string, string, *server.Conn, *handler.MultiGatewayConnectionState, *preparedstatement.PortalInfo, *preparedstatement.PreparedStatementInfo) (*querypb.StatementDescription, error) { return nil, nil }
func (f *fakeIExecute) CopyInitiate(context.Context, *server.Conn, string, string, string, *handler.MultiGatewayConnectionState, func(context.Context, *sqltypes.Result) error) (int16, []int16, error) { return 0, nil, nil }
func (f *fakeIExecute) CopySendData(context.Context, *server.Conn, string, string, *handler.MultiGatewayConnectionState, []byte) error { return nil }
func (f *fakeIExecute) CopyFinalize(context.Context, *server.Conn, string, string, *handler.MultiGatewayConnectionState, []byte, func(context.Context, *sqltypes.Result) error) error { return nil }
func (f *fakeIExecute) CopyAbort(context.Context, *server.Conn, string, string, *handler.MultiGatewayConnectionState) error { return nil }

func newTestPlanner(t *testing.T) (*Planner, *ddlcache.DDLCache, *ddlmock.MockTopoStore) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	store := ddlmock.NewTopoStore()
	cache := ddlcache.New(store, logger)
	return NewPlanner("default", logger, nil, cache, store), cache, store
}

func TestPlan_View_IsHandled(t *testing.T) {
	t.Parallel()
	p, _, _ := newTestPlanner(t)
	stmt := &ast.ViewStmt{
		View: &ast.RangeVar{RelName: "public.my_view"},
	}
	plan, err := p.Plan("CREATE VIEW public.my_view AS SELECT 1", stmt, &server.Conn{})
	require.NoError(t, err)
	_, ok := plan.Primitive.(*engine.Sequence)
	assert.True(t, ok, "CREATE VIEW must produce a Sequence plan")
}

func TestPlan_AlterView_RenameStmt(t *testing.T) {
	t.Parallel()
	p, _, _ := newTestPlanner(t)
	stmt := ast.NewRenameStmt(ast.OBJECT_VIEW, "new_view")
	stmt.Relation = &ast.RangeVar{RelName: "public.my_view"}
	plan, err := p.Plan("ALTER VIEW public.my_view RENAME TO new_view", stmt, &server.Conn{})
	require.NoError(t, err)
	_, ok := plan.Primitive.(*engine.Sequence)
	assert.True(t, ok, "ALTER VIEW RENAME TO must produce a Sequence plan")
}

func TestPlan_AlterView_AlterTableStmt(t *testing.T) {
	t.Parallel()
	p, _, _ := newTestPlanner(t)
	stmt := ast.NewAlterTableStmt(&ast.RangeVar{RelName: "public.my_view"}, &ast.NodeList{})
	stmt.Objtype = ast.OBJECT_VIEW
	plan, err := p.Plan("ALTER VIEW public.my_view OWNER TO my_user", stmt, &server.Conn{})
	require.NoError(t, err)
	_, ok := plan.Primitive.(*engine.Sequence)
	assert.True(t, ok, "ALTER VIEW OWNER TO must produce a Sequence plan")
}
