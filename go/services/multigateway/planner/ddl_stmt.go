// Copyright 2024 The Multigres Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

// NEW FILE: go/services/multigateway/planner/ddl_stmt.go
//
// Handles DDL statements that affect objects the gateway caches for
// cross-shard query planning, following the utility.c dispatch pattern
// established in planner.go.
//
// Objects currently cached at the gateway layer:
//   - Views: needed for derived-table expansion in cross-shard SELECT queries
//   - Functions: needed for query rewriting (future, not yet implemented)
//
// Procedures are not cached by the gateway today and fall through to the
// default Route path (plain passthrough to Postgres).
//
// Consistency guarantee: eventual across gateways; read-your-writes for the
// issuing gateway. See docs/ddl-consistency.md for the full model.

package planner

import (
	"github.com/multigres/multigres/go/common/parser/ast"
	"github.com/multigres/multigres/go/common/pgprotocol/server"
	"github.com/multigres/multigres/go/services/multigateway/engine"
)

// isDDLWithCachedObject reports whether stmt is a DDL operation that creates,
// modifies, or drops an object the gateway caches for query planning.
//
// When this returns true, the planner wraps the plain Route with
// InvalidateDDLCache and BumpDDLVersion primitives so that:
//   - The local cache is invalidated synchronously (read-your-writes).
//   - Other gateways are signalled to invalidate via the etcd version key.
func isDDLWithCachedObject(stmt ast.Stmt) bool {
	switch s := stmt.(type) {
	case *ast.ViewStmt:
		return true
	case *ast.DropStmt:
		return s.RemoveType == ast.OBJECT_VIEW
	case *ast.RenameStmt:
		return s.RenameType == ast.OBJECT_VIEW || s.RelationType == ast.OBJECT_VIEW
	case *ast.AlterTableStmt:
		return s.Objtype == ast.OBJECT_VIEW
	}
	return false
}

func qualifiedName(schema, rel string) string {
    if schema != "" {
        return schema + "." + rel
    }
    return rel
}

// extractDDLObjectName returns the fully-qualified name of the object
// affected by stmt, suitable for use as the DDL cache key.
//
// Returns an empty string for statements where the name cannot be
// statically determined (e.g. DROP FUNCTION with multiple targets);
// callers should treat an empty name as a signal to call InvalidateAll
// rather than InvalidateObject.
func extractDDLObjectName(stmt ast.Stmt) string {
    switch s := stmt.(type) {
    case *ast.ViewStmt:
        if s.View == nil {
            return ""
        }
        return qualifiedName(s.View.SchemaName, s.View.RelName)
    case *ast.RenameStmt:
        if s.Relation != nil {
            return qualifiedName(s.Relation.SchemaName, s.Relation.RelName)
        }
        return ""
    case *ast.AlterTableStmt:
        if s.Relation != nil {
            return qualifiedName(s.Relation.SchemaName, s.Relation.RelName)
        }
        return ""
    case *ast.DropStmt:
        if s.RemoveType == ast.OBJECT_VIEW &&
            s.Objects != nil && s.Objects.Len() == 1 {
            if str, ok := s.Objects.Items[0].(*ast.String); ok {
                return str.SVal
            }
        }
        return "" // multi-object drop → InvalidateAll
    }
    return ""
}

func (p *Planner) planDDLWithCacheInvalidation(
	sql string,
	stmt ast.Stmt,
	_ *server.Conn,
) (*engine.Plan, error) {
	tableGroup := p.GetDefaultTableGroup()

	// Step 1: execute the DDL on Postgres.
	route := engine.NewRoute(tableGroup, "", sql)

	// Step 2: evict the affected object from the local cache.
	objectName := extractDDLObjectName(stmt)
	invalidate := engine.NewInvalidateDDLCache(p.ddlCache, objectName)

	// Step 3: bump the etcd version to notify all other gateways.
	bump := engine.NewBumpDDLVersion(p.topoStore, p.logger)

	primitive := engine.NewSequence([]engine.Primitive{route, invalidate, bump})
	return engine.NewPlan(sql, primitive), nil
}
