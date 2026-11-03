// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package mysql

import (
	"context"
	"sync"

	"github.com/KarakoramDev/Karakoram/internal/domain"
)

// clusterMutationLocks follows the context-aware, refcounted gate pattern used
// by pgbinding.Manager. The registry mutex never spans native or storage I/O.
// References include both the holder and waiters, including canceled waiters
// until they finish unregistering, so an entry cannot be replaced while in use.
type clusterMutationLocks struct {
	mu      sync.Mutex
	entries map[domain.ClusterRef]*clusterMutationLock
}
type clusterMutationLock struct {
	held chan struct{}
	refs int
	ref  domain.ClusterRef
}

func (m *clusterMutationLocks) acquire(ctx context.Context, ref domain.ClusterRef) (*clusterMutationLock, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	ref.Namespace = domain.NormalizedNamespace(ref.Namespace)
	m.mu.Lock()
	if m.entries == nil {
		m.entries = make(map[domain.ClusterRef]*clusterMutationLock)
	}
	lock := m.entries[ref]
	if lock == nil {
		lock = &clusterMutationLock{held: make(chan struct{}, 1), ref: ref}
		m.entries[ref] = lock
	}
	lock.refs++
	m.mu.Unlock()
	select {
	case lock.held <- struct{}{}:
		if err := ctx.Err(); err != nil {
			m.release(lock)
			return nil, err
		}
		return lock, nil
	case <-ctx.Done():
		m.unreference(lock)
		return nil, ctx.Err()
	}
}
func (m *clusterMutationLocks) release(lock *clusterMutationLock) {
	<-lock.held
	m.unreference(lock)
}
func (m *clusterMutationLocks) unreference(lock *clusterMutationLock) {
	m.mu.Lock()
	defer m.mu.Unlock()
	lock.refs--
	if lock.refs == 0 {
		delete(m.entries, lock.ref)
	}
}
