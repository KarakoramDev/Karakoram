// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package dokploy

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	model "github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/mysql"
	"github.com/KarakoramDev/Karakoram/internal/engine/mysql/myspec"
	"github.com/KarakoramDev/Karakoram/internal/engine/postgres"
	"github.com/KarakoramDev/Karakoram/internal/engine/supabase"
	"github.com/KarakoramDev/Karakoram/internal/infra"
)

// These engines start with empty Dokploy inventory and converge through native
// data readiness, not merely the first (possibly data/companion) deployment.
func TestCreateWithProxiesBeforeFirstDeploy(t *testing.T) {
	for _, p := range []engine.Provider{
		postgres.New(postgres.Options{}),
		mysql.New(mysql.Options{}),
		supabase.New(postgres.Options{}),
	} {
		t.Run(p.Name(), func(t *testing.T) {
			rc, f := createInventoryContext(t, p)
			requireFirstProxyDeployment(t, p, rc, f, false)
		})
	}
}

// Full Galera formation/certification has its own native fixtures. Keep this
// empty-create smoke honest: a data deployment is not proxy-admission proof.
func TestPXCEmptyCreateReachesDataDeployment(t *testing.T) {
	p := mysql.New(mysql.Options{Provider: "pxc"})
	rc, f := createInventoryContext(t, p)
	for pass := range 80 {
		if _, err := p.Sync(t.Context(), rc); err != nil {
			t.Fatalf("empty PXC create pass %d: %v", pass, err)
		}
		if fakeCalls(f, "application.dropDeployment") > 0 {
			return
		}
	}
	t.Fatalf("empty PXC create never deployed data: %s; state=%s", rc.SyncWait, rc.Cluster.Internal)
}

// Start proxy coverage at a joined-data checkpoint rather than pretending the
// native substitute proves Galera bootstrap, SST and history certification.
// All applications are still created by real Sync; no proxy is seeded.
func TestPXCJoinedDataBeforeFirstProxyDeploy(t *testing.T) {
	p := mysql.New(mysql.Options{Provider: "pxc"})
	rc, f := createInventoryContext(t, p)
	for pass := range 120 {
		if _, err := p.Sync(t.Context(), rc); err != nil {
			t.Fatalf("preparing joined-data checkpoint pass %d: %v", pass, err)
		}
		root, state := createInventoryState(t, rc, "mysql")
		members := state["members"].([]any)
		deployed := 0
		f.mu.Lock()
		for _, raw := range members {
			m := raw.(map[string]any)
			if m["role"] != "data" {
				continue
			}
			if app := f.apps[fmt.Sprint(m["serviceId"])]; app != nil && len(app.deployments) > 0 {
				deployed++
			}
		}
		f.mu.Unlock()
		if deployed != 3 {
			continue
		}
		for _, raw := range members {
			m := raw.(map[string]any)
			if m["role"] == "proxy" {
				if m["stage"] != "planned" || m["serviceId"] != nil && m["serviceId"] != "" {
					t.Fatalf("checkpoint already contains proxy: %+v", m)
				}
				continue
			}
			// Complete the actual platform receipt before supplying the native
			// joined checkpoint. A submitted build alone is not deployed data.
			opRaw, err := json.Marshal(m["deployOp"])
			if err != nil {
				t.Fatal(err)
			}
			var op infra.Op
			if err := json.Unmarshal(opRaw, &op); err != nil {
				t.Fatal(err)
			}
			awaitFake(t, rc.Backend.(*Backend), op)
			m["stage"], m["initialized"] = "joined", true
			m["serverUUID"] = createInventoryUUID(fmt.Sprint(m["name"]))
			delete(m, "deployOp")
		}
		state["formation"].(map[string]any)["state"] = "formed"
		delete(state, "pending")
		root["mysql"] = state
		raw, err := json.Marshal(root)
		if err != nil {
			t.Fatal(err)
		}
		rc.Cluster.Internal = raw
		requireFirstProxyDeployment(t, p, rc, f, true)
		return
	}
	t.Fatalf("PXC data checkpoint not reached: %s; state=%s", rc.SyncWait, rc.Cluster.Internal)
}

func createInventoryContext(t *testing.T, p engine.Provider) (*engine.Context, *fakeAPI) {
	t.Helper()
	t.Setenv("KARAKORAM_ARTIFACT_MIRROR", "https://mirror.example/native")
	rc, f, _ := providerBudgetContext(t, p)
	rc.Artifact = budgetArtifact(renderTestArtifact(t))
	proxy := rc.Cluster.Spec.Components.Extra["proxy"]
	proxy.Replicas = 2
	rc.Cluster.Spec.Components.Extra["proxy"] = proxy
	if p.Name() != "pxc" {
		rc.Cluster.Spec.Topology.Type = "single"
		rc.Cluster.Spec.Components.Engine.Replicas = 1
	}
	rc.Cluster.Spec.DataMcp = &model.DataMcpSpec{Enabled: true}
	if err := p.Validate(t.Context(), nil, rc.Cluster); err != nil {
		t.Fatalf("invalid create fixture: %v", err)
	}
	// The fake retains Dokploy's own service to prove a nonempty global Swarm
	// inventory; there are no cluster applications or application services.
	if len(f.apps) != 0 || len(f.services) != 1 || f.services["dokploy"] == nil {
		t.Fatal("create fixture must start with empty application inventory")
	}
	return rc, f
}

func createInventoryState(t *testing.T, rc *engine.Context, key string) (map[string]any, map[string]any) {
	t.Helper()
	var root map[string]any
	if err := json.Unmarshal(rc.Cluster.Internal, &root); err != nil {
		t.Fatal(err)
	}
	return root, root[key].(map[string]any)
}

func requireFirstProxyDeployment(t *testing.T, p engine.Provider, rc *engine.Context, f *fakeAPI, checkpoint bool) {
	t.Helper()
	apply := func() {}
	key := "postgres"
	if core, ok := p.(*mysql.Core); ok {
		key = "mysql"
		apply = createInventoryMySQLAgents(t, core, rc, f, checkpoint)
	}
	var proxyID string
	imported, receiptSettled := false, false
	for pass := range 160 {
		apply()
		_, err := p.Sync(t.Context(), rc)
		if err != nil {
			t.Fatalf("proxy create pass %d (imported=%t receiptSettled=%t id=%q): %v; state=%s", pass, imported, receiptSettled, proxyID, err, rc.Cluster.Internal)
		}
		_, state := createInventoryState(t, rc, key)
		for _, raw := range state["members"].([]any) {
			m := raw.(map[string]any)
			if m["role"] != "proxy" {
				continue
			}
			id, _ := m["serviceId"].(string)
			f.mu.Lock()
			app := f.apps[id]
			// During a frozen import the provider may not yet persist serviceId.
			if app == nil {
				for candidate, a := range f.apps {
					meta, ok := parseMetadata(fakeString(a.row, "description"))
					if ok && meta.Hostname == m["name"] {
						id, app = candidate, a
						break
					}
				}
			}
			if app == nil {
				f.mu.Unlock()
				continue
			}
			count := len(app.deployments)
			service := f.services[fakeString(app.row, "appName")]
			f.mu.Unlock()
			if !imported {
				if count != 0 || service != nil {
					t.Fatalf("proxy already deployed at first import observation: %s", id)
				}
				imported, proxyID = true, id
			}
			if id != proxyID {
				t.Fatalf("proxy application identity changed: %s -> %s", proxyID, id)
			}
			// observePending must have consumed the import receipt via
			// ObserveProxyRollout, including fresh admission, before deploying.
			if m["stage"] == "imported" && state["pending"] == nil && m["importBatch"] == nil {
				if count != 0 || service != nil {
					t.Fatal("import receipt settled only after proxy deployment")
				}
				receiptSettled = true
			}
			if count > 0 {
				if !receiptSettled || count != 1 {
					t.Fatalf("proxy deployment without settled undeployed import receipt: settled=%t deployments=%d", receiptSettled, count)
				}
				t.Logf("proxy %s imported with no service, import receipt admitted and settled, first proxy deployment submitted at pass %d", proxyID, pass)
				return
			}
		}
	}
	t.Fatalf("proxy never reached first deployment: imported=%t receiptSettled=%t id=%q wait=%s state=%s", imported, receiptSettled, proxyID, rc.SyncWait, rc.Cluster.Internal)
}

func createInventoryUUID(name string) string {
	return fmt.Sprintf("11111111-2222-3333-4444-%012d", 1+int(name[len(name)-1]-'0'))
}

// Only native agents are substituted. Config revisions come from MemberConfig
// for actually deployed data members and reload applies exactly that revision.
func createInventoryMySQLAgents(t *testing.T, p *mysql.Core, rc *engine.Context, f *fakeAPI, galera bool) func() {
	t.Helper()
	var mu sync.Mutex
	configs := map[string]agentapi.MemberConfig{}
	applied := map[string]int64{}
	providerBudgetServer(t, rc, func(w http.ResponseWriter, r *http.Request) {
		name := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
		mu.Lock()
		defer mu.Unlock()
		switch r.URL.Path {
		case "/v1/status":
			_ = json.NewEncoder(w).Encode(agentapi.Status{ConfigVersion: applied[name], Processes: map[string]agentapi.ProcessStatus{"mysqld": {PID: 1}}})
		case "/v1/reload":
			applied[name] = configs[name].ConfigVersion
			_ = json.NewEncoder(w).Encode(agentapi.ActionResponse{Success: true})
		case "/v1/mysql/status":
			uuid := createInventoryUUID(name)
			_ = json.NewEncoder(w).Encode(myspec.StatusResponse{Version: rc.Cluster.Spec.Version, ServerUUID: uuid, MemberState: "ONLINE", MemberRole: "PRIMARY", DataIdentity: myspec.DataIdentity{Initialized: true, ServerUUID: uuid, FormationEpoch: 1}})
		case "/v1/mysql/galera-status":
			if !galera {
				t.Error("single-node MySQL requested Galera status")
			}
			_ = json.NewEncoder(w).Encode(myspec.GaleraStatusResponse{ServerUUID: createInventoryUUID(name), ClusterStatus: "Primary", LocalStateComment: "Synced", Ready: true, Connected: true, ClusterSize: 3, StateUUID: "11111111-2222-3333-4444-555555555555", LastCommitted: 10})
		default:
			t.Errorf("unexpected native agent request %s for %s", r.URL.Path, name)
			http.NotFound(w, r)
		}
	})
	return func() {
		if len(rc.Cluster.Internal) == 0 {
			return
		}
		_, state := createInventoryState(t, rc, "mysql")
		for _, raw := range state["members"].([]any) {
			m := raw.(map[string]any)
			if m["role"] != "data" {
				continue
			}
			f.mu.Lock()
			app := f.apps[fmt.Sprint(m["serviceId"])]
			deployed := app != nil && len(app.deployments) > 0
			f.mu.Unlock()
			if !deployed {
				continue
			}
			name := fmt.Sprint(m["name"])
			cfg, err := p.MemberConfig(t.Context(), rc, name)
			if err != nil {
				t.Fatal(err)
			}
			mu.Lock()
			configs[name], applied[name] = cfg, cfg.ConfigVersion
			mu.Unlock()
		}
	}
}

func TestFreshTaskInventoryWithoutServiceNeverAuthorizesTasks(t *testing.T) {
	f := newFakeAPI(t)
	b := fakeBackend(t, f, nil)
	imported := importFake(t, b, runtimeSpec("runtime", ""))
	f.mu.Lock()
	// Even an inconsistent task listing must not authorize an address without
	// the service. A create has no tasks; a disappeared service may leave rows.
	f.apps[imported.Services["runtime"]].tasks = []map[string]any{{"containerId": "orphan", "name": "runtime.1", "state": "running", "currentState": "Running"}}
	f.mu.Unlock()
	containers, err := b.Containers(infra.WithFreshTaskInventory(t.Context()), "runtime")
	if err != nil || len(containers) != 0 {
		t.Fatalf("absent service must be pending with no authorized tasks: %+v %v", containers, err)
	}
}

func TestCreateWithProxiesBeforeFirstDeployDeleteRecovery(t *testing.T) {
	p := postgres.New(postgres.Options{})
	rc, f, _ := providerBudgetContext(t, p)
	proxy := rc.Cluster.Spec.Components.Extra["proxy"]
	proxy.Replicas = 2
	rc.Cluster.Spec.Components.Extra["proxy"] = proxy
	rc.Cluster.Spec.Topology.Type = "single"
	if err := p.Validate(t.Context(), nil, rc.Cluster); err != nil {
		t.Fatalf("invalid delete-recovery fixture: %v", err)
	}
	// Interrupt create immediately after the proxy application import, before
	// its import receipt has necessarily been consumed. Cleanup must drain it.
	for range 40 {
		_, err := p.Sync(t.Context(), rc)
		f.mu.Lock()
		imported := len(f.apps) >= 3
		f.mu.Unlock()
		if imported {
			break
		}
		if err != nil {
			t.Fatalf("preparing interrupted create: %v", err)
		}
	}
	if fakeCalls(f, "application.dropDeployment") != 0 {
		t.Fatal("fixture deployed before interruption")
	}
	rc.Cluster.Deleting = true
	for range 80 {
		done, err := p.Cleanup(t.Context(), rc, model.DeleteOptions{})
		if err != nil {
			t.Fatalf("cleaning interrupted create: %v", err)
		}
		if done {
			f.mu.Lock()
			defer f.mu.Unlock()
			if len(f.apps) != 0 {
				t.Fatalf("cleanup retained %d applications", len(f.apps))
			}
			return
		}
		// Pending import/delete receipts and cluster state survive a CP
		// upgrade; neither provider nor backend process memory is required.
		rc.Backend = fakeBackend(t, f, nil)
		p = postgres.New(postgres.Options{})
	}
	t.Fatalf("interrupted create cleanup did not finish: %s", rc.Cluster.Status.Message)
}
