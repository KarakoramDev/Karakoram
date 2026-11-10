// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package nomad

import (
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/infra"
	"github.com/KarakoramDev/Karakoram/internal/store"
	"sync"
	"testing"
	"time"
)

type configPullClock func() time.Time

func (c configPullClock) Now() time.Time { return c() }

// ExportConfigPullFixture exposes existing provider fake transports to the
// external-package production HTTP/manager integration test without an import cycle.
func ExportConfigPullFixture(t *testing.T, kind string) (engine.Provider, *engine.Context, func(time.Duration), func() int) {
	t.Helper()
	f := newFakeNomad(t)
	b := fakeBackend(t, f, nil)
	counts := countProviderGETs(f)
	x := newSteadyBudgetFixture(t, kind, b, f)
	f.mu.Lock()
	f.projectAllocations = true
	f.mu.Unlock()
	var mu sync.Mutex
	now := time.Now()
	clock := func() time.Time { mu.Lock(); defer mu.Unlock(); return now }
	advance := func(d time.Duration) { mu.Lock(); now = now.Add(d); mu.Unlock() }
	b.c.http = withConfigListingLatency(b.c.http, advance)
	t.Cleanup(infra.SetSnapshotClockForTest(clock))
	x.rc.Clock = configPullClock(clock)
	st := x.rc.Store.(*store.Store)
	for _, name := range x.members {
		for _, key := range []string{"tls/agents/" + name + ".crt", "tls/agents/" + name + ".key", "tls/servers/" + name + ".crt", "tls/servers/" + name + ".key"} {
			if err := st.SetClusterSecret(t.Context(), x.rc.Cluster.Ref(), key, []byte("fixture")); err != nil {
				t.Fatal(err)
			}
		}
	}
	for _, key := range []string{"tls/ca.crt", "users/root", "users/admin", "users/postgres", "users/kk_agent", "users/kk_repl", "users/kk_replicator", "users/kk_rewind", "users/kk_monitor", "users/kk_backup", "users/kk_admin", "users/kk_proxy_admin"} {
		if err := st.SetClusterSecret(t.Context(), x.rc.Cluster.Ref(), key, []byte("fixture")); err != nil {
			t.Fatal(err)
		}
	}
	return x.p, x.rc, advance, func() int { return requestTotal(counts.snapshotRequests()) }
}
