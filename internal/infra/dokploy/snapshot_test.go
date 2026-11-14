// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package dokploy

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/postgres"
	"github.com/KarakoramDev/Karakoram/internal/infra"
)

func snapshotSteadyContext(t *testing.T) context.Context {
	t.Helper()
	return infra.WithSteadyObservation(infra.ObservationContext(t.Context()))
}

func snapshotReadNodes(t *testing.T, b *Backend, ctx context.Context) {
	t.Helper()
	var nodes []node
	if err := b.c.get(ctx, "cluster.getNodes", nil, &nodes); err != nil {
		t.Fatal(err)
	}
	if len(nodes) != 3 {
		t.Fatalf("cached body lost nodes: got %d", len(nodes))
	}
}

func TestPlatformSnapshotSteadyAndFreshContexts(t *testing.T) {
	f := newFakeAPI(t)
	b := fakeBackend(t, f, nil)
	steady := snapshotSteadyContext(t)
	snapshotReadNodes(t, b, steady)
	before := fakeCalls(f, "cluster.getNodes")
	listing := fakeCalls(f, "environment.one")
	snapshotReadNodes(t, b, steady)
	if fakeCalls(f, "cluster.getNodes") != before || fakeCalls(f, "environment.one") != listing {
		t.Fatal("warm steady read reached the platform")
	}
	for _, ctx := range []context.Context{t.Context(), infra.WithoutObservation(steady), infra.WithFreshTaskInventory(steady)} {
		snapshotReadNodes(t, b, ctx)
		before++
		if got := fakeCalls(f, "cluster.getNodes"); got != before {
			t.Fatalf("fresh context served snapshot: got %d requests, want %d", got, before)
		}
	}
}

func TestSteadyInspectionCannotAuthorizePinnedDial(t *testing.T) {
	f := newFakeAPI(t)
	b := fakeBackend(t, f, nil)
	importFake(t, b, runtimeSpec("runtime", ""))
	deployFake(t, b, "runtime", "")
	app := fakeAppSnapshot(t, f, "runtime")
	name := fakeString(app, "appName")
	// Establish the listing baseline and warm the platform inspection before
	// attaching the pass memo whose two freshness classes we exercise.
	warm := snapshotSteadyContext(t)
	if _, err := b.inspectService(warm, name); err != nil {
		t.Fatal(err)
	}
	before := fakeCalls(f, "docker.getConfig")
	steady := infra.WithObservationCache(warm, new(sync.Map))
	if _, err := b.inspectService(steady, name); err != nil {
		t.Fatal(err)
	}
	if got := fakeCalls(f, "docker.getConfig"); got != before {
		t.Fatalf("steady inspection missed warm snapshot: got %d, want %d", got, before)
	}
	fresh := infra.WithFreshTaskInventory(steady)
	if _, err := b.Containers(fresh, "runtime"); err != nil {
		t.Fatal(err)
	}
	if got := fakeCalls(f, "docker.getConfig"); got != before+1 {
		t.Fatalf("dial authorization reused steady inspection: got %d, want %d", got, before+1)
	}
	// Fresh evidence is reusable by both classes within the read-only pass.
	for _, ctx := range []context.Context{fresh, steady} {
		if _, err := b.inspectService(ctx, name); err != nil {
			t.Fatal(err)
		}
	}
	if got := fakeCalls(f, "docker.getConfig"); got != before+1 {
		t.Fatalf("fresh inspection was not shared: got %d, want %d", got, before+1)
	}
}

func TestPlatformSnapshotSharedTargetAndMutation(t *testing.T) {
	f := newFakeAPI(t)
	first, second := fakeBackend(t, f, nil), fakeBackend(t, f, nil)
	if first.PlatformSnapshot() != second.PlatformSnapshot() {
		t.Fatal("same target does not share snapshot")
	}
	steady := snapshotSteadyContext(t)
	snapshotReadNodes(t, first, steady)
	before := fakeCalls(f, "cluster.getNodes")
	snapshotReadNodes(t, second, steady)
	if fakeCalls(f, "cluster.getNodes") != before {
		t.Fatal("second backend missed shared snapshot")
	}
	if err := second.c.post(t.Context(), "network.create", map[string]any{"name": "snapshot-network"}, nil); err != nil {
		t.Fatal(err)
	}
	snapshotReadNodes(t, first, steady)
	if fakeCalls(f, "cluster.getNodes") != before+1 {
		t.Fatal("sibling mutation did not invalidate snapshot")
	}
	// Failed mutations are just as uncertain as successful ones.
	if err := second.c.post(t.Context(), "missing.procedure", map[string]any{}, nil); err == nil {
		t.Fatal("expected rejected mutation")
	}
	snapshotReadNodes(t, first, steady)
	if fakeCalls(f, "cluster.getNodes") != before+2 {
		t.Fatal("failed sibling mutation did not invalidate snapshot")
	}
}

func TestPlatformSnapshotEnvironmentIsolation(t *testing.T) {
	f := newFakeAPI(t)
	first := fakeBackend(t, f, nil)
	f.mu.Lock()
	f.environments["other"] = map[string]any{"environmentId": "other", "projectId": "project", "name": "other"}
	f.mu.Unlock()
	factory := newFactory(func(key string) string {
		if key == "KARAKORAM_DOKPLOY_ALLOWED_URLS" {
			return f.server.URL
		}
		return ""
	})
	otherBackend, err := factory.New(infra.Target{Kind: "dokploy", ID: "other", Token: fakeAPIKey, Config: map[string]string{"url": f.server.URL}})
	if err != nil {
		t.Fatal(err)
	}
	other := otherBackend.(*Backend)
	if first.PlatformSnapshot() == other.PlatformSnapshot() {
		t.Fatal("different environments share snapshot")
	}
	steady := snapshotSteadyContext(t)
	snapshotReadNodes(t, first, steady)
	before := fakeCalls(f, "cluster.getNodes")
	snapshotReadNodes(t, other, steady)
	if fakeCalls(f, "cluster.getNodes") != before+1 {
		t.Fatal("different environment served first environment's snapshot")
	}
	if err := other.c.post(t.Context(), "network.create", map[string]any{"name": "other-network"}, nil); err != nil {
		t.Fatal(err)
	}
	snapshotReadNodes(t, first, steady)
	if fakeCalls(f, "cluster.getNodes") != before+1 {
		t.Fatal("different environment invalidated first environment")
	}
}

func TestPlatformSnapshotListingDriftAndIdleStability(t *testing.T) {
	now := time.Now().Add(time.Second)
	restore := infra.SetSnapshotClockForTest(func() time.Time { return now })
	defer restore()
	f := newFakeAPI(t)
	f.mu.Lock()
	f.apps["app"] = &fakeApplication{row: map[string]any{"applicationId": "app", "name": "member", "environmentId": "environment", "applicationStatus": "done", "description": "owner", "serverId": "node-a", "createdAt": "first"}}
	f.mu.Unlock()
	b := fakeBackend(t, f, nil)
	steady := snapshotSteadyContext(t)
	snapshotReadNodes(t, b, steady)
	before := fakeCalls(f, "cluster.getNodes")
	listing := fakeCalls(f, "environment.one")
	generation := b.PlatformSnapshot().DriftGeneration()
	f.mu.Lock()
	f.apps["app"].row["createdAt"] = "later"
	f.mu.Unlock()
	now = now.Add(infra.SnapshotListingInterval + time.Second)
	snapshotReadNodes(t, b, steady)
	if fakeCalls(f, "cluster.getNodes") != before || fakeCalls(f, "environment.one") != listing+1 {
		t.Fatal("idle listing was not one fresh GET with a stable fingerprint")
	}
	if b.PlatformSnapshot().DriftGeneration() != generation {
		t.Fatal("volatile timestamp changed listing fingerprint")
	}
	f.mu.Lock()
	f.apps["app"].row["applicationStatus"] = "error"
	f.mu.Unlock()
	now = now.Add(infra.SnapshotListingInterval + time.Second)
	snapshotReadNodes(t, b, steady)
	if fakeCalls(f, "cluster.getNodes") != before+1 || fakeCalls(f, "environment.one") != listing+2 {
		t.Fatal("status drift did not invalidate snapshot after one fresh listing")
	}
	if b.PlatformSnapshot().DriftGeneration() <= generation {
		t.Fatal("status drift did not advance drift generation")
	}
}

func TestPlatformSnapshotHitBypassesReadAdmission(t *testing.T) {
	f := newFakeAPI(t)
	b := fakeBackend(t, f, nil)
	steady := snapshotSteadyContext(t)
	snapshotReadNodes(t, b, steady)
	for range 8 {
		if err := b.c.observations.Acquire(t.Context()); err != nil {
			t.Fatal(err)
		}
		defer b.c.observations.Release()
	}
	ctx, cancel := context.WithTimeout(steady, 100*time.Millisecond)
	defer cancel()
	snapshotReadNodes(t, b, ctx)
}

func TestPlatformSnapshotOptionalAppNameAndOrdering(t *testing.T) {
	now := time.Now().Add(time.Second)
	restore := infra.SetSnapshotClockForTest(func() time.Time { return now })
	defer restore()
	var version atomic.Int32
	var reads atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/environment.one" {
			reads.Add(1)
			_, _ = w.Write([]byte(`{}`))
			return
		}
		switch version.Load() {
		case 0:
			_, _ = w.Write([]byte(`{"applications":[{"applicationId":"a","appName":"original"},{"applicationId":"b"}]}`))
		case 1:
			_, _ = w.Write([]byte(`{"applications":[{"applicationId":"b"},{"applicationId":"a","appName":"original"}]}`))
		default:
			_, _ = w.Write([]byte(`{"applications":[{"applicationId":"a","appName":"changed"},{"applicationId":"b"}]}`))
		}
	}))
	defer server.Close()
	c := newClient(server.URL, "test")
	c.setSnapshot("environment")
	steady := snapshotSteadyContext(t)
	read := func() {
		t.Helper()
		if err := c.get(steady, "application.one", nil, nil); err != nil {
			t.Fatal(err)
		}
	}
	read()
	version.Store(1)
	now = now.Add(infra.SnapshotListingInterval + time.Second)
	read()
	if reads.Load() != 1 {
		t.Fatal("listing order changed fingerprint")
	}
	version.Store(2)
	now = now.Add(infra.SnapshotListingInterval + time.Second)
	read()
	if reads.Load() != 2 {
		t.Fatal("optional appName drift did not invalidate")
	}
}

func TestRuntimeStatesFreshPeerBudget(t *testing.T) {
	for _, mode := range []string{"running", "stopped", "missing", "replaced-running", "replaced-stopped", "recovered", "listing-locator"} {
		t.Run(mode, func(t *testing.T) {
			f := newFakeAPI(t)
			b := fakeBackend(t, f, nil)
			id := providerBudgetRuntime(t, b, "peer", "", 1, false)
			if !b.PlatformSnapshot().EnsureListing(t.Context()) {
				t.Fatal("initial listing failed")
			}
			// Preserve both caches deliberately. The routing gate must read
			// current identity and runtime state despite these older proofs.
			ctx := snapshotSteadyContext(t)
			f.mu.Lock()
			app := f.apps[id]
			f.summaryAppName = mode == "listing-locator"
			if mode == "stopped" || mode == "replaced-stopped" {
				m, _ := parseMetadata(fakeString(app.row, "description"))
				m.Desired = "stopped"
				app.row["description"] = m.description()
			}
			if mode == "missing" {
				delete(f.apps, id)
			}
			if mode == "replaced-running" || mode == "replaced-stopped" {
				delete(f.apps, id)
				id = "replacement-application"
				app.row["applicationId"] = id
				labels := f.services[fakeString(app.row, "appName")]["Spec"].(map[string]any)["TaskTemplate"].(map[string]any)["ContainerSpec"].(map[string]any)["Labels"].(map[string]any)
				labels["karakoram.application"] = id
				f.apps[id] = app
			}
			if mode == "recovered" {
				app.holdTasks = true
				for _, task := range app.tasks {
					task["state"], task["currentState"] = "stopped", "Shutdown"
				}
			}
			f.mu.Unlock()
			if mode == "recovered" {
				stopped, err := b.RuntimeStates(ctx, []string{"peer"})
				if err != nil || stopped["peer"].Status == infra.StatusRunning {
					t.Fatal("stopped tasks were routed", stopped, err)
				}
				f.mu.Lock()
				for _, task := range app.tasks {
					task["state"], task["currentState"] = "running", "Running"
				}
				f.mu.Unlock()
			}
			counts := countProviderGETs(f)
			states, err := b.RuntimeStates(ctx, []string{"peer"})
			if err != nil {
				t.Fatal(err)
			}
			wantRunning := mode == "running" || mode == "replaced-running" || mode == "recovered" || mode == "listing-locator"
			if running := states["peer"].Status == infra.StatusRunning; running != wantRunning {
				t.Fatal("fresh routing gate returned wrong liveness", states)
			}
			if wantRunning && states["peer"].ID != id {
				t.Fatal("routing gate retained the replaced application identity", states)
			}
			wantGETs := 1
			if mode != "missing" && mode != "listing-locator" {
				wantGETs++ // application.one resolves the omitted locator freshly
			}
			if wantRunning {
				wantGETs += 3 // shared service inventory, ID-scoped inspection, tasks
			}
			if got := requestTotal(counts.snapshotRequests()); got != wantGETs {
				t.Fatalf("fresh routing GETs=%d, want %d", got, wantGETs)
			}
		})
	}
}

func TestRuntimeStatesRejectForeignPostgresPeers(t *testing.T) {
	for _, summaryAppName := range []bool{false, true} {
		for _, mode := range []string{"foreign-service", "redirected-locator", "owned-replacement"} {
			t.Run(fmt.Sprintf("%s/summary-locator-%t", mode, summaryAppName), func(t *testing.T) {
				p := postgres.New(postgres.Options{})
				rc, f, b := providerBudgetContext(t, p)
				peer, proxy := "samplep0", "samplepx"
				rc.Cluster.Spec.Topology.Type = "ha"
				id := providerBudgetRuntime(t, b, peer, peer+"v", 1, false)
				proxyID := providerBudgetRuntime(t, b, proxy, "", 1, true)
				foreignID := providerBudgetRuntime(t, b, "foreign", "", 1, false)
				providerBudgetState(t, rc, "postgres", map[string]any{
					"uid": "routing-ownership", "configVersion": 1, "topology": "ha", "formed": true,
					"members": []map[string]any{
						{"name": peer, "role": "data", "stage": "joined", "serviceId": id, "version": rc.Cluster.Spec.Version, "configVersion": 1, "replicas": 1},
						{"name": proxy, "role": "proxy", "stage": "joined", "serviceId": proxyID, "version": rc.Cluster.Spec.Version, "configVersion": 1, "replicas": 1},
					},
				})
				for _, key := range []string{"tls/ca.crt", "tls/servers/" + proxy + ".crt", "tls/servers/" + proxy + ".key", "tls/agents/" + proxy + ".crt", "tls/agents/" + proxy + ".key", "users/postgres", "users/kk_monitor", "users/kk_admin", "users/kk_proxy_admin", "users/" + engine.DataAccessUser(engine.DataAccessRead), "users/" + engine.DataAccessUser(engine.DataAccessReadWrite)} {
					if err := rc.Secrets.SetClusterSecret(t.Context(), rc.Cluster.Ref(), key, []byte("fixture")); err != nil {
						t.Fatal(err)
					}
				}
				hasPeer := func(raw []byte) bool {
					t.Helper()
					if len(raw) == 0 {
						return false
					}
					var spec struct{ Peers []struct{ Name string } }
					if err := json.Unmarshal(raw, &spec); err != nil {
						t.Fatal(err)
					}
					for _, entry := range spec.Peers {
						if entry.Name == peer {
							return true
						}
					}
					return false
				}
				before, err := p.MemberConfig(t.Context(), rc, proxy)
				if err != nil || !hasPeer(before.EngineSpec) {
					t.Fatal("owned peer baseline unavailable", err)
				}
				f.mu.Lock()
				f.summaryAppName = summaryAppName
				app := f.apps[id]
				name := fakeString(app.row, "appName")
				service := f.services[name]
				labels := service["Spec"].(map[string]any)["TaskTemplate"].(map[string]any)["ContainerSpec"].(map[string]any)["Labels"].(map[string]any)
				switch mode {
				case "foreign-service":
					labels["karakoram.owner"] = "another-owner"
				case "redirected-locator":
					app.row["appName"] = f.apps[foreignID].row["appName"]
				case "owned-replacement":
					delete(f.apps, id)
					id = "replacement-application"
					app.row["applicationId"] = id
					f.apps[id] = app
					labels["karakoram.application"] = id
				}
				f.mu.Unlock()
				after, err := p.MemberConfig(snapshotSteadyContext(t), rc, proxy)
				if mode == "owned-replacement" {
					if err != nil || hasPeer(after.EngineSpec) {
						t.Fatal("owned replacement inherited routing from the previously admitted application ID", err)
					}
				} else if err == nil || hasPeer(after.EngineSpec) {
					t.Fatal("foreign runtime authorized PostgreSQL routing", mode, err)
				}
			})
		}
	}
}

func TestRuntimeStatesRejectReplacedPostgresPeers(t *testing.T) {
	for _, summaryAppName := range []bool{false, true} {
		for _, mode := range []string{"replacement", "task-service-id", "task-name", "no-task-identity"} {
			t.Run(fmt.Sprintf("%s/summary-locator-%t", mode, summaryAppName), func(t *testing.T) {
				p := postgres.New(postgres.Options{})
				rc, f, b := providerBudgetContext(t, p)
				peer, proxy := "samplep0", "samplepx"
				rc.Cluster.Spec.Topology.Type = "ha"
				id := providerBudgetRuntime(t, b, peer, peer+"v", 1, false)
				proxyID := providerBudgetRuntime(t, b, proxy, "", 1, true)
				foreignID := providerBudgetRuntime(t, b, "foreign", "", 1, false)
				providerBudgetState(t, rc, "postgres", map[string]any{
					"uid": "routing-replacement", "configVersion": 1, "topology": "ha", "formed": true,
					"members": []map[string]any{
						{"name": peer, "role": "data", "stage": "joined", "serviceId": id, "version": rc.Cluster.Spec.Version, "configVersion": 1, "replicas": 1},
						{"name": proxy, "role": "proxy", "stage": "joined", "serviceId": proxyID, "version": rc.Cluster.Spec.Version, "configVersion": 1, "replicas": 1},
					},
				})
				for _, key := range []string{"tls/ca.crt", "tls/servers/" + proxy + ".crt", "tls/servers/" + proxy + ".key", "tls/agents/" + proxy + ".crt", "tls/agents/" + proxy + ".key", "users/postgres", "users/kk_monitor", "users/kk_admin", "users/kk_proxy_admin", "users/" + engine.DataAccessUser(engine.DataAccessRead), "users/" + engine.DataAccessUser(engine.DataAccessReadWrite)} {
					if err := rc.Secrets.SetClusterSecret(t.Context(), rc.Cluster.Ref(), key, []byte("fixture")); err != nil {
						t.Fatal(err)
					}
				}
				hasPeer := func(raw []byte) bool {
					t.Helper()
					if len(raw) == 0 {
						return false
					}
					var spec struct{ Peers []struct{ Name string } }
					if err := json.Unmarshal(raw, &spec); err != nil {
						t.Fatal(err)
					}
					for _, entry := range spec.Peers {
						if entry.Name == peer {
							return true
						}
					}
					return false
				}
				baseline, err := p.MemberConfig(t.Context(), rc, proxy)
				if err != nil || !hasPeer(baseline.EngineSpec) {
					t.Fatal("owned peer baseline unavailable", err)
				}
				f.mu.Lock()
				f.summaryAppName = summaryAppName
				app, foreign := f.apps[id], f.apps[foreignID]
				name := fakeString(app.row, "appName")
				foreignName := fakeString(foreign.row, "appName")
				owned, replacement := f.services[name], f.services[foreignName]
				ownedID := fakeString(owned, "ID")
				f.mu.Unlock()
				arm := func() {
					f.mu.Lock()
					defer f.mu.Unlock()
					f.services[name] = owned
					f.services[foreignName] = replacement
					app.row["appName"], foreign.row["appName"] = name, foreignName
					fired := false
					f.afterInspect = func(inspectedID string) {
						if inspectedID != ownedID || fired {
							return
						}
						fired = true
						switch mode {
						case "replacement":
							// A has just been serialized. Remove it and let foreign B
							// take its name, leaving the application metadata unchanged.
							delete(f.services, foreignName)
							f.services[name] = replacement
							replacement["Spec"].(map[string]any)["Name"] = name
							app.row["appName"], foreign.row["appName"] = "removed-service", name
							for _, task := range foreign.tasks {
								task["name"] = name + ".1"
								task["state"], task["currentState"] = "running", "Running"
							}
						case "task-service-id":
							for _, task := range app.tasks {
								task["serviceId"] = fakeString(replacement, "ID")
							}
						case "task-name":
							for _, task := range app.tasks {
								task["name"] = foreignName + ".1"
							}
						case "no-task-identity":
							for _, task := range app.tasks {
								delete(task, "name")
								delete(task, "serviceId")
							}
						}
					}
				}
				arm()
				states, stateErr := b.RuntimeStates(snapshotSteadyContext(t), []string{peer})
				wantRunning := mode == "no-task-identity"
				if running := states[peer].Status == infra.StatusRunning; running != wantRunning {
					t.Error("replacement runtime authorized Running state", states, stateErr)
				}
				arm()
				after, configErr := p.MemberConfig(snapshotSteadyContext(t), rc, proxy)
				if hasPeer(after.EngineSpec) != wantRunning {
					t.Error("replacement runtime authorized PostgreSQL routing", configErr)
				}
				if wantRunning && (stateErr != nil || configErr != nil) {
					t.Fatal("ID-scoped tasks without redundant identity were rejected", stateErr, configErr)
				}
			})
		}
	}
}

// A proxy may have just flushed the restarted primary's postmaster epoch.
// Removing an already joined backend during an owned rolling task gap makes
// Apply DELETE that backend and forget its epoch. Keep logical membership
// through shutdown and replacement without treating receipt completion as
// serving evidence or bypassing fresh service/task ownership checks.
func TestRuntimeStatesPostgresRollKeepsServingPeer(t *testing.T) {
	for _, summaryAppName := range []bool{false, true} {
		for _, action := range []string{"deploy", "restart", "scale"} {
			t.Run(fmt.Sprintf("%s/summary-locator-%t", action, summaryAppName), func(t *testing.T) {
				p := postgres.New(postgres.Options{})
				rc, f, b := providerBudgetContext(t, p)
				now := time.Date(2026, time.November, 15, 12, 0, 0, 0, time.UTC)
				b.now = func() time.Time { return now }
				peer, proxy := "samplep0", "samplepx"
				id := providerBudgetRuntime(t, b, peer, peer+"v", 1, false)
				proxyID := providerBudgetRuntime(t, b, proxy, "", 1, true)
				providerBudgetState(t, rc, "postgres", map[string]any{
					"uid": "routing-roll", "configVersion": 1, "topology": "single", "formed": true,
					"members": []map[string]any{
						{"name": peer, "role": "data", "stage": "joined", "serviceId": id, "version": rc.Cluster.Spec.Version, "configVersion": 1, "replicas": 1},
						{"name": proxy, "role": "proxy", "stage": "joined", "serviceId": proxyID, "version": rc.Cluster.Spec.Version, "configVersion": 1, "replicas": 1},
					},
				})
				for _, key := range []string{"tls/ca.crt", "tls/servers/" + proxy + ".crt", "tls/servers/" + proxy + ".key", "tls/agents/" + proxy + ".crt", "tls/agents/" + proxy + ".key", "users/postgres", "users/kk_monitor", "users/kk_admin", "users/kk_proxy_admin", "users/" + engine.DataAccessUser(engine.DataAccessRead), "users/" + engine.DataAccessUser(engine.DataAccessReadWrite)} {
					if err := rc.Secrets.SetClusterSecret(t.Context(), rc.Cluster.Ref(), key, []byte("fixture")); err != nil {
						t.Fatal(err)
					}
				}
				render := func(t *testing.T, want bool) {
					t.Helper()
					cfg, err := p.MemberConfig(snapshotSteadyContext(t), rc, proxy)
					if err != nil {
						t.Fatal(err)
					}
					var spec struct {
						Proxy struct{ Servers []string }
					}
					if err := json.Unmarshal(cfg.EngineSpec, &spec); err != nil {
						t.Fatal(err)
					}
					got := len(spec.Proxy.Servers) == 1 && spec.Proxy.Servers[0] == peer
					if got != want || !want && len(spec.Proxy.Servers) != 0 {
						t.Fatalf("config Apply would remove serving backend: servers=%v want peer=%t", spec.Proxy.Servers, want)
					}
				}
				render(t, true) // Warm the observation paths before the roll.
				f.mu.Lock()
				f.summaryAppName = summaryAppName
				app := f.apps[id]
				m, _ := parseMetadata(fakeString(app.row, "description"))
				m.Pending = &operation{Action: action, AppID: id, AppName: fakeString(app.row, "appName"), Hostname: peer, Issued: b.nowTime()}
				app.holdTasks = true
				for _, task := range app.tasks {
					m.Pending.PreviousTasks = append(m.Pending.PreviousTasks, fakeString(task, "containerId"))
				}
				servingDescription := m.description()
				app.row["description"] = servingDescription
				f.mu.Unlock()
				counts := countProviderGETs(f)
				render(t, true) // Incumbent still serves while its replacement builds.
				f.mu.Lock()
				for _, task := range app.tasks {
					task["state"], task["currentState"] = "stopped", "Shutdown"
				}
				f.mu.Unlock()
				render(t, true) // Shutdown is a rolling task gap, not membership removal.
				f.mu.Lock()
				shutdownTasks := app.tasks
				app.tasks = nil
				f.mu.Unlock()
				render(t, true) // Swarm may report no tasks before replacement appears.
				f.mu.Lock()
				app.tasks = shutdownTasks
				for _, task := range app.tasks {
					task["containerId"] = "replacement-task"
					task["state"], task["currentState"] = "running", "Running"
				}
				f.mu.Unlock()
				render(t, true) // Replacement serves before Pending is cleared.
				render(t, true) // The next unchanged Apply must not undo admission.
				f.mu.Lock()
				persisted := fakeString(app.row, "description")
				f.mu.Unlock()
				if persisted != servingDescription {
					t.Fatal("routing observation mutated lifecycle metadata")
				}
				// Five renders, each retaining the established fresh-read budget.
				wantGETs := 25
				if summaryAppName {
					wantGETs = 20
				}
				if got := requestTotal(counts.snapshotRequests()); got != wantGETs {
					t.Fatalf("routing polled lifecycle or changed read budget: got %d GETs, want %d", got, wantGETs)
				}
				for request := range counts.snapshotRequests() {
					if !strings.HasPrefix(request, "GET ") {
						t.Fatal("routing performed a lifecycle mutation", request)
					}
				}
				for _, phase := range []string{"running", "shutdown", "empty"} {
					t.Run(phase, func(t *testing.T) {
						for _, scenario := range []string{"failed", "uncertain", "stop", "delete", "start", "wipe", "unknown", "desired-stopped", "desired-deleting", "undeployed", "importing", "application-error", "removing", "replacing", "engine-service-id", "task-service-id", "task-name", "foreign", "missing", "missing-service", "zero-replicas", "non-replicated", "no-pending", "wrong-app-id", "wrong-app-name", "missing-app-name", "wrong-hostname", "wrong-service-identity", "missing-issued", "future-issued", "expired-issued", "expiry-boundary"} {
							if phase == "running" && (scenario == "zero-replicas" || scenario == "non-replicated" || scenario == "no-pending" || strings.HasPrefix(scenario, "wrong-") || scenario == "missing-app-name" || strings.HasSuffix(scenario, "-issued") || scenario == "expiry-boundary") {
								continue // These constrain task-gap admission, not running-task evidence.
							}
							t.Run(scenario, func(t *testing.T) {
								f.mu.Lock()
								current := m
								current.Pending = &operation{Action: action, AppID: id, AppName: fakeString(app.row, "appName"), Hostname: peer, Issued: b.nowTime()}
								status := app.row["applicationStatus"]
								service := f.services[fakeString(app.row, "appName")]
								labels := service["Spec"].(map[string]any)["TaskTemplate"].(map[string]any)["ContainerSpec"].(map[string]any)["Labels"].(map[string]any)
								serviceName := fakeString(app.row, "appName")
								originalMode := service["Spec"].(map[string]any)["Mode"]
								owner := labels["karakoram.owner"]
								originalTasks := app.tasks
								originalInternal := rc.Cluster.Internal
								app.tasks = make([]map[string]any, len(originalTasks))
								for i, task := range originalTasks {
									app.tasks[i] = make(map[string]any, len(task))
									for key, value := range task {
										app.tasks[i][key] = value
									}
								}
								if phase != "running" {
									for _, task := range app.tasks {
										task["state"], task["currentState"] = "stopped", "Shutdown"
									}
								}
								// Retain malformed historical rows even in the empty phase:
								// identity validation must happen before any gap admission.
								if phase == "empty" && scenario != "task-service-id" && scenario != "task-name" {
									app.tasks = nil
								}
								t.Cleanup(func() {
									f.mu.Lock()
									defer f.mu.Unlock()
									app.row["description"], app.row["applicationStatus"] = servingDescription, status
									app.tasks = originalTasks
									labels["karakoram.owner"] = owner
									f.apps[id] = app
									f.services[serviceName] = service
									service["Spec"].(map[string]any)["Mode"] = originalMode
									rc.Cluster.Internal = originalInternal
								})
								switch scenario {
								case "failed":
									current.Pending.Failed = true
								case "uncertain":
									current.Pending.Uncertain = true
								case "stop", "delete", "start", "wipe", "unknown":
									current.Pending.Action = scenario
								case "wrong-app-id":
									current.Pending.AppID = "foreign-application"
								case "wrong-app-name":
									current.Pending.AppName = "foreign-service"
								case "missing-app-name":
									current.Pending.AppName = ""
								case "wrong-hostname":
									current.Pending.Hostname = "foreign-member"
								case "wrong-service-identity":
									current.Pending.ServiceIdentity = "foreign-swarm-id"
								case "missing-issued":
									current.Pending.Issued = time.Time{}
								case "future-issued":
									current.Pending.Issued = now.Add(time.Nanosecond)
								case "expired-issued", "expiry-boundary":
									window := taskStartupWindow
									if action == "deploy" {
										window += buildProgressWindow
									}
									current.Pending.Issued = now.Add(-window)
									if scenario == "expired-issued" {
										current.Pending.Issued = current.Pending.Issued.Add(-time.Nanosecond)
									}
								case "desired-stopped":
									current.Desired = "stopped"
								case "desired-deleting":
									current.Desired = "deleting"
								case "undeployed":
									current.Deployed = false
								case "importing":
									current.Importing = true
								case "application-error":
									app.row["applicationStatus"] = "error"
								case "removing", "replacing", "engine-service-id":
									var root map[string]any
									if err := json.Unmarshal(originalInternal, &root); err != nil {
										f.mu.Unlock()
										t.Fatal(err)
									}
									members := root["postgres"].(map[string]any)["members"].([]any)
									member := members[0].(map[string]any)
									switch scenario {
									case "removing":
										// Retirement and scale-down mark the departing peer.
										member["removing"] = true
									case "replacing":
										// A replacement's planned identity must not borrow
										// a still-running task from its former deployment.
										member["stage"], member["serviceId"] = "planned", ""
									case "engine-service-id":
										// Same logical DNS, but not the application's identity
										// whose joined membership was durably admitted.
										member["serviceId"] = "former-application"
									}
									rc.Cluster.Internal, _ = json.Marshal(root)
								case "task-service-id", "task-name":
									for _, task := range app.tasks {
										if scenario == "task-service-id" {
											task["serviceId"] = "foreign-service"
										} else {
											task["name"] = "foreign-service.1"
										}
									}
								case "foreign":
									labels["karakoram.owner"] = "foreign"
								case "missing":
									delete(f.apps, id)
								case "missing-service":
									delete(f.services, serviceName)
								case "zero-replicas":
									service["Spec"].(map[string]any)["Mode"] = map[string]any{"Replicated": map[string]any{"Replicas": 0}}
								case "non-replicated":
									service["Spec"].(map[string]any)["Mode"] = map[string]any{"Global": map[string]any{}}
								case "no-pending":
									current.Pending = nil
								}
								app.row["description"] = current.description()
								f.mu.Unlock()
								if scenario == "foreign" || scenario == "task-service-id" || scenario == "task-name" {
									cfg, err := p.MemberConfig(snapshotSteadyContext(t), rc, proxy)
									if err == nil || len(cfg.EngineSpec) != 0 {
										t.Fatal("pending receipt bypassed fresh ownership gate", err)
									}
								} else {
									render(t, false)
								}
							})
						}
					})
				}
			})
		}
	}
}

// Active preserves routing intent during a bounded roll; it must never claim
// a stopped or absent task is Running. Real replacement tasks still take
// precedence even when the persisted roll receipt has reached its deadline.
func TestRuntimeStatesRollGapStatusAndDeadline(t *testing.T) {
	for _, action := range []string{"deploy", "restart", "scale"} {
		for _, gap := range []string{"shutdown", "empty"} {
			t.Run(action+"/"+gap, func(t *testing.T) {
				f := newFakeAPI(t)
				b := fakeBackend(t, f, nil)
				id := providerBudgetRuntime(t, b, "peer", "peerv", 1, false)
				now := time.Date(2026, time.November, 15, 12, 0, 0, 0, time.UTC)
				b.now = func() time.Time { return now }
				window := taskStartupWindow
				if action == "deploy" {
					window += buildProgressWindow
				}
				f.mu.Lock()
				app := f.apps[id]
				m, _ := parseMetadata(fakeString(app.row, "description"))
				name := fakeString(app.row, "appName")
				m.Pending = &operation{
					Action: action, AppID: id, AppName: name, Hostname: "peer",
					ServiceIdentity: fakeString(f.services[name], "ID"), Issued: now,
				}
				app.row["description"] = m.description()
				app.holdTasks = true
				tasks := app.tasks
				for _, task := range tasks {
					task["state"], task["currentState"] = "stopped", "Shutdown"
				}
				if gap == "empty" {
					app.tasks = nil
				}
				f.mu.Unlock()
				check := func(want string) {
					t.Helper()
					states, err := b.RuntimeStates(snapshotSteadyContext(t), []string{"peer"})
					if err != nil {
						t.Fatal(err)
					}
					service, ok := states["peer"]
					if !ok || service.ID != id || service.Status != want {
						t.Fatalf("roll status at %v: got %+v, want identity=%s status=%s", now, states, id, want)
					}
				}
				check(infra.StatusActive)
				now = m.Pending.Issued.Add(window - time.Nanosecond)
				check(infra.StatusActive)
				now = m.Pending.Issued.Add(window)
				check(infra.StatusStopped)
				now = now.Add(time.Nanosecond)
				check(infra.StatusStopped)
				f.mu.Lock()
				app.tasks = tasks
				for _, task := range app.tasks {
					task["containerId"] = "replacement-task"
					task["state"], task["currentState"] = "running", "Running"
				}
				f.mu.Unlock()
				check(infra.StatusRunning)
			})
		}
	}
}
