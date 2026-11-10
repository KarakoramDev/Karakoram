// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package dokploy

import (
	"fmt"
	model "github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/infra"
	"github.com/KarakoramDev/Karakoram/internal/store"
	"strings"
	"sync"
	"testing"
	"time"
)

// ExportConfigPullFixture exposes existing provider fake transports to the
// external-package production HTTP/manager integration test without an import cycle.
func ExportConfigPullFixture(t *testing.T, kind string) (engine.Provider, *engine.Context, func(time.Duration), func() int) {
	t.Helper()
	f := newFakeAPI(t)
	b := fakeBackend(t, f, nil)
	counts := countProviderGETs(f)
	x := newSteadyBudgetFixture(t, kind, b)
	var mu sync.Mutex
	now := time.Now()
	clock := func() time.Time { mu.Lock(); defer mu.Unlock(); return now }
	advance := func(d time.Duration) { mu.Lock(); now = now.Add(d); mu.Unlock() }
	b.c.http = withConfigListingLatency(b.c.http, advance)
	t.Cleanup(infra.SetSnapshotClockForTest(clock))
	x.rc.Clock = steadyBudgetClock(clock)
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

// ExportSharedTargetConfigFixture keeps fake transport internals out of the
// external-package real HTTP test, matching ExportConfigPullFixture.
func ExportSharedTargetConfigFixture(t *testing.T, p engine.Provider) (*engine.Context, []string, func(time.Duration), func(int), func(bool), func() int) {
	t.Helper()
	rc, f, b := providerBudgetContext(t, p)
	st := rc.Store.(*store.Store)
	var mu sync.Mutex
	now := time.Now()
	clock := func() time.Time { mu.Lock(); defer mu.Unlock(); return now }
	advance := func(d time.Duration) { mu.Lock(); now = now.Add(d); mu.Unlock() }
	t.Cleanup(infra.SetSnapshotClockForTest(clock))
	rc.Clock = steadyBudgetClock(clock)
	foreignID := providerBudgetRuntime(t, b, "samplepx", "", 1, true)
	// Secret values change outside the listing fingerprint; the fixed key is
	// seeded before the baseline so only explicit status changes cause drift.
	if err := b.SetEnvSecrets(t.Context(), "samplepx", map[string]string{"CONFIG_BUDGET_STEP": "0"}); err != nil {
		t.Fatal(err)
	}
	if err := st.EnqueueOperation(t.Context(), &model.Operation{Cluster: "sample", Kind: model.OperationUpdate, Phase: model.PhaseRunning}); err != nil {
		t.Fatal(err)
	}
	var members []string
	for index := range 10 {
		c := &model.Cluster{Name: fmt.Sprintf("pg%d", index), Namespace: model.DefaultNamespace, Generation: 1}
		c.Spec.Topology.Type = "ha"
		p.ApplyDefaults(&c.Spec)
		if err := st.CreateCluster(t.Context(), c); err != nil {
			t.Fatal(err)
		}
		local := *rc
		local.Cluster = c
		var state []map[string]any
		for peer := range 3 {
			name := fmt.Sprintf("%sp%d", c.Name, peer)
			id := providerBudgetRuntime(t, b, name, name+"v", 1, false)
			state = append(state, map[string]any{"name": name, "role": "data", "stage": "joined", "serviceId": id, "version": c.Spec.Version, "configVersion": 1, "replicas": 1})
		}
		proxy := c.Name + "px"
		id := providerBudgetRuntime(t, b, proxy, "", 2, true)
		state = append(state, map[string]any{"name": proxy, "role": "proxy", "stage": "joined", "serviceId": id, "version": c.Spec.Version, "configVersion": 1, "replicas": 2})
		providerBudgetState(t, &local, "postgres", map[string]any{"uid": c.Name + "-identity", "configVersion": 1, "topology": "ha", "formed": true, "members": state})
		c.Status = model.ClusterStatus{Phase: model.PhaseReady, ObservedGeneration: c.Generation, ObservedAt: clock(), ObservationEpoch: rc.ObservationEpoch}
		if err := st.UpdateClusterState(t.Context(), c); err != nil {
			t.Fatal(err)
		}
		for _, key := range []string{"tls/ca.crt", "tls/servers/" + proxy + ".crt", "tls/servers/" + proxy + ".key", "tls/agents/" + proxy + ".crt", "tls/agents/" + proxy + ".key", "users/postgres", "users/kk_monitor", "users/kk_admin", "users/kk_proxy_admin", "users/" + engine.DataAccessUser(engine.DataAccessRead), "users/" + engine.DataAccessUser(engine.DataAccessReadWrite)} {
			if err := st.SetClusterSecret(t.Context(), c.Ref(), key, []byte("fixture")); err != nil {
				t.Fatal(err)
			}
		}
		if err := st.SetAgentToken(t.Context(), &model.AgentToken{Cluster: c.Name, Member: proxy, Token: proxy + "-token"}); err != nil {
			t.Fatal(err)
		}
		members = append(members, proxy)
	}
	counts := countProviderGETs(f)
	mutate := func(step int) {
		t.Helper()
		if err := b.SetEnvSecrets(infra.WithRequestCaller(t.Context(), infra.CallerReconcile), "samplepx", map[string]string{"CONFIG_BUDGET_STEP": fmt.Sprint(step)}); err != nil {
			t.Fatal(err)
		}
	}
	drift := func(active bool) {
		f.mu.Lock()
		defer f.mu.Unlock()
		if active {
			f.apps[foreignID].row["applicationStatus"] = "error"
		} else {
			f.apps[foreignID].row["applicationStatus"] = "done"
		}
	}
	gets := func() int {
		total := 0
		for request, count := range counts.snapshotRequests() {
			if strings.HasPrefix(request, "GET ") {
				total += count
			}
		}
		return total
	}
	return rc, members, advance, mutate, drift, gets
}
