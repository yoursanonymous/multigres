// Copyright 2026 The Multigres Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

package ddlmock

import (
	"context"
	"sync"

	"github.com/multigres/multigres/go/common/topoclient"
)

type watcher struct {
	ch   chan int64
	done chan struct{}
}

// ensure it implements Store
type MockTopoStore struct {
	topoclient.Store // embed to satisfy all other methods implicitly
	
	ddlMu           sync.Mutex
	ddlVersion      int64
	ddlWatchers     []*watcher
	failNextIncrDDL error
}

func NewTopoStore() *MockTopoStore {
	return &MockTopoStore{}
}

func (m *MockTopoStore) SetDDLSchemaVersion(v int64) {
	m.ddlMu.Lock()
	m.ddlVersion = v
	m.ddlMu.Unlock()
}

func (m *MockTopoStore) GetDDLSchemaVersion(_ context.Context) (int64, error) {
	m.ddlMu.Lock()
	defer m.ddlMu.Unlock()
	return m.ddlVersion, nil
}

func (m *MockTopoStore) IncrDDLSchemaVersion(ctx context.Context) error {
	m.ddlMu.Lock()
	if m.failNextIncrDDL != nil {
		err := m.failNextIncrDDL
		m.failNextIncrDDL = nil
		m.ddlMu.Unlock()
		return err
	}
	m.ddlVersion++
	newVersion := m.ddlVersion
	watchers := make([]*watcher, len(m.ddlWatchers))
	copy(watchers, m.ddlWatchers)
	m.ddlMu.Unlock()

	for _, w := range watchers {
		select {
		case w.ch <- newVersion:
		case <-w.done:  // watcher already closed, skip safely
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}

func (m *MockTopoStore) EmitDDLVersionEvent(ctx context.Context, version int64) {
	m.ddlMu.Lock()
	watchers := make([]*watcher, len(m.ddlWatchers))
	copy(watchers, m.ddlWatchers)
	m.ddlMu.Unlock()

	for _, w := range watchers {
		select {
		case w.ch <- version:
		case <-w.done:
		case <-ctx.Done():
			return
		}
	}
}

func (m *MockTopoStore) WatchDDLSchemaVersion(ctx context.Context) (<-chan int64, error) {
	ch := make(chan int64, 16)
	w := &watcher{ch: ch, done: make(chan struct{})}

	m.ddlMu.Lock()
	m.ddlWatchers = append(m.ddlWatchers, w)
	m.ddlMu.Unlock()

	go func() {
		<-ctx.Done()
		m.ddlMu.Lock()
		for i, existing := range m.ddlWatchers {
			if existing == w {
				m.ddlWatchers = append(m.ddlWatchers[:i], m.ddlWatchers[i+1:]...)
				break
			}
		}
		m.ddlMu.Unlock()
		close(w.done)
		close(ch)
	}()

	return ch, nil
}

func (m *MockTopoStore) FailNextIncrDDLSchemaVersion(err error) {
	m.ddlMu.Lock()
	m.failNextIncrDDL = err
	m.ddlMu.Unlock()
}
