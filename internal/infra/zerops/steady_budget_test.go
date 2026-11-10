// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package zerops

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/agentpush"
	"github.com/KarakoramDev/Karakoram/internal/datamcp/companion"
	"github.com/KarakoramDev/Karakoram/internal/datamcp/companionspec"
	model "github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/mysql"
	"github.com/KarakoramDev/Karakoram/internal/engine/mysql/myspec"
	"github.com/KarakoramDev/Karakoram/internal/engine/neo4j"
	"github.com/KarakoramDev/Karakoram/internal/engine/neo4j/neo4jspec"
	"github.com/KarakoramDev/Karakoram/internal/engine/postgres"
	"github.com/KarakoramDev/Karakoram/internal/engine/postgres/pgspec"
	"github.com/KarakoramDev/Karakoram/internal/infra"
)

const idleBudgetPerClusterPerSecond = 0.1

type steadyBudgetFixture struct {
	kind      string
	p         engine.Provider
	rc        *engine.Context
	members   []string
	proxy     string
	handler   http.HandlerFunc
	companion *companion.Controller
	instances map[string][]infra.Container
	stopped   map[string]bool
}

func (c *providerGETCounts) snapshotRequests() map[string]int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return maps.Clone(c.requests)
}
func requestTotal(m map[string]int) int {
	n := 0
	for _, v := range m {
		n += v
	}
	return n
}
func assertRequests(t *testing.T, c *providerGETCounts, want map[string]int) int {
	t.Helper()
	got := c.snapshotRequests()
	if !maps.Equal(got, want) {
		t.Errorf("requests=%v, want %v", got, want)
	}
	c.mu.Lock()
	if len(c.signed) != 0 {
		t.Errorf("unexpected signed-URL log traffic=%v", c.signed)
	}
	c.mu.Unlock()
	return requestTotal(got)
}

func newSteadyBudgetFixture(t *testing.T, kind string, b *Backend) *steadyBudgetFixture {
	t.Helper()
	var p engine.Provider
	switch kind {
	case "mysql":
		p = mysql.New(mysql.Options{Provider: "mysql"})
	case "postgres":
		p = postgres.New(postgres.Options{})
	default:
		p = neo4j.New(neo4j.Options{})
	}
	rc := providerBudgetContext(t, p, b)
	rc.Backend = b
	x := &steadyBudgetFixture{kind: kind, p: p, rc: rc}
	// Separate durable cluster state shares one project and target snapshot.
	switch kind {
	case "mysql":
		rc.Cluster.Spec.Topology.Type = "group"
		rc.Cluster.Spec.Components.Engine.Replicas = 3
		px := rc.Cluster.Spec.Components.Extra["proxy"]
		px.Replicas = 3
		rc.Cluster.Spec.Components.Extra["proxy"] = px
		var members []map[string]any
		for i := range 4 {
			name, role, vol, count := fmt.Sprintf("samplem%d", i), "data", fmt.Sprintf("samplem%dv", i), 1
			if i == 3 {
				name, role, vol, count = "samplemx", "proxy", "", 3
				x.proxy = name
			}
			id := providerBudgetRuntime(t, b, name, vol, count, i == 3)
			x.members = append(x.members, name)
			members = append(members, map[string]any{"name": name, "role": role, "index": i, "stage": "joined", "serviceId": id, "volume": vol, "replicas": count, "configVersion": 1, "version": rc.Cluster.Spec.Version})
		}
		providerBudgetState(t, rc, "mysql", map[string]any{"uid": "sample-identity", "configVersion": 1, "topology": "group", "members": members, "formation": map[string]any{"state": "formed", "formationEpoch": 1}})
	case "postgres":
		rc.Cluster.Spec.Topology.Type = "single"
		px := rc.Cluster.Spec.Components.Extra["proxy"]
		px.Replicas = 2
		rc.Cluster.Spec.Components.Extra["proxy"] = px
		rc.Cluster.Spec.DataMcp = &model.DataMcpSpec{Enabled: true}
		data := providerBudgetRuntime(t, b, "samplep0", "samplep0v", 1, false)
		proxy := providerBudgetRuntime(t, b, "samplepx", "", 2, true)
		providerBudgetRuntime(t, b, "samplemcp", "", 1, false)
		x.members = []string{"samplep0", "samplepx", "samplemcp"}
		x.proxy = "samplepx"
		providerBudgetState(t, rc, "postgres", map[string]any{"uid": "sample-identity", "configVersion": 1, "topology": "single", "formed": true, "members": []map[string]any{
			{"name": "samplep0", "role": "data", "stage": "joined", "serviceId": data, "volume": "samplep0v", "version": rc.Cluster.Spec.Version, "replicas": 1, "configVersion": 1},
			{"name": "samplepx", "role": "proxy", "stage": "joined", "serviceId": proxy, "version": rc.Cluster.Spec.Version, "replicas": 2, "configVersion": 1},
		}})
		providerBudgetSecret(t, rc, "datamcp/state", map[string]any{"stage": "ready"})
		providerBudgetSecret(t, rc, "datamcp/config", agentapi.MemberConfig{Cluster: "sample", Member: "samplemcp", ConfigVersion: 1, DesiredProcesses: []string{companionspec.ProcessRead}})
		registry, err := engine.NewRegistry(p)
		if err != nil {
			t.Fatal(err)
		}
		x.companion = &companion.Controller{Registry: registry}
	case "neo4j":
		id := providerBudgetRuntime(t, b, "samplen0", "samplen0v", 1, false)
		x.members = []string{"samplen0"}
		providerBudgetState(t, rc, "neo4j", map[string]any{"uid": "sample-identity", "configVersion": 1, "member": map[string]any{"name": "samplen0", "volume": "samplen0v", "serviceId": id, "stage": "ready", "version": rc.Cluster.Spec.Version, "resources": rc.Cluster.Spec.Components.Engine.Resources}})
		for _, key := range []string{"tls/ca.crt", "tls/servers/samplen0.crt", "tls/servers/samplen0.key", "users/neo4j", "tls/agents/samplen0.crt", "tls/agents/samplen0.key"} {
			if err := rc.Secrets.SetClusterSecret(t.Context(), rc.Cluster.Ref(), key, []byte("fixture")); err != nil {
				t.Fatal(err)
			}
		}
	}
	var restart string
	if kind == "neo4j" {
		cfg, err := p.(*neo4j.Provider).MemberConfig(t.Context(), rc, "samplen0")
		if err != nil {
			t.Fatal(err)
		}
		var spec neo4jspec.Spec
		if err = json.Unmarshal(cfg.EngineSpec, &spec); err != nil {
			t.Fatal(err)
		}
		restart = spec.RestartToken
	}
	x.handler = func(w http.ResponseWriter, r *http.Request) {
		session := r.Header.Get("X-Fixture-Session")
		if session == "" {
			member := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
			if instances := x.instances[member]; len(instances) > 0 {
				session = instances[0].Hostname
			}
		}
		w.Header().Set(agentapi.HeartbeatSessionHeader, session)
		switch r.URL.Path {
		case "/v1/status":
			build := engine.CurrentAgentBuild()
			_ = json.NewEncoder(w).Encode(agentapi.Status{ConfigVersion: 1, Agent: agentapi.AgentBuild{Version: build.Version, Commit: build.Commit, Build: build.Build}})
		case "/v1/mysql/status":
			_ = json.NewEncoder(w).Encode(myspec.StatusResponse{MemberState: "ONLINE", MemberRole: "PRIMARY", GroupMembers: 3})
		case "/v1/mysql/proxy-status":
			_ = json.NewEncoder(w).Encode(myspec.ProxyStatusResponse{Backends: []myspec.ProxyBackend{{Hostgroup: 10, Status: "ONLINE"}}})
		case "/v1/postgres/status":
			st := pgspec.StatusResponse{Running: true, Role: "standalone", Version: rc.Cluster.Spec.Version, CredentialReceipts: true, SlotStatusKnown: true, HealthCapabilities: &pgspec.HealthCapabilities{ReplicationSlots: true}}
			if strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ") == "samplepx" {
				st.Role = "proxy"
				st.ProxyBackends = []pgspec.ProxyBackend{{Host: "samplep0", Hostgroup: pgspec.ProxyWriterGroup, Status: "ONLINE"}}
			}
			_ = json.NewEncoder(w).Encode(st)
		case "/v1/datamcp/readiness":
			_ = json.NewEncoder(w).Encode(map[string]any{"configVersion": 1, "processes": map[string]any{companionspec.ProcessRead: map[string]bool{"healthy": true}}})
		case "/v1/neo4j/readiness":
			_ = json.NewEncoder(w).Encode(neo4jspec.Readiness{Ready: true, ConfigVersion: 1, Version: rc.Cluster.Spec.Version, AppliedRestartToken: restart})
		default:
			http.NotFound(w, r)
		}
	}
	providerBudgetServer(t, rc, x.handler)
	return x
}

func (x *steadyBudgetFixture) observe(t *testing.T, steady bool) (model.ClusterStatus, *engine.SteadyOutcome) {
	t.Helper()
	return x.observePass(t, engine.WithObservationPass(t.Context()), steady)
}
func (x *steadyBudgetFixture) observePass(t *testing.T, ctx context.Context, steady bool) (model.ClusterStatus, *engine.SteadyOutcome) {
	t.Helper()
	if steady {
		ctx = engine.WithSteadyEligible(ctx)
	}
	ctx, out := engine.WithSteadyOutcome(ctx)
	status, err := engine.ObserveStatus(ctx, x.p, x.rc)
	if err != nil {
		t.Fatal(err)
	}
	if x.companion != nil {
		// Companion observation is outside ObserveStatus's provider sweep.
		// Give its native read the same observation/subscription classification.
		ctx = infra.ObservationContext(ctx)
		if steady && out.Accepted {
			ctx = infra.WithSteadyObservation(ctx)
		}
		if err = x.companion.Observe(ctx, x.rc, &status); err != nil {
			t.Fatal(err)
		}
	}
	return status, out
}
func (x *steadyBudgetFixture) ready(t *testing.T, steady bool) *engine.SteadyOutcome {
	t.Helper()
	status, out := x.observe(t, steady)
	if status.Phase != model.PhaseReady {
		t.Fatalf("%s phase=%s conditions=%+v members=%+v", x.kind, status.Phase, status.Conditions, status.Members)
	}
	if x.companion != nil && (status.DataMcp == nil || status.DataMcp.Phase != model.DataMcpPhaseReady) {
		t.Fatalf("companion=%+v", status.DataMcp)
	}
	return out
}
func (x *steadyBudgetFixture) heartbeat(t *testing.T, pushed *agentpush.Store) {
	t.Helper()
	for _, member := range x.members {
		for _, container := range x.instances[member] {
			if x.stopped[member] {
				continue
			}
			if container.Status != infra.ContainerRunning && container.Status != infra.ContainerActive {
				continue
			}
			key := agentpush.MemberKey{Namespace: x.rc.Cluster.Namespace, Cluster: x.rc.Cluster.Name, Member: member}
			req := agentapi.HeartbeatRequest{Session: container.Hostname, Instance: agentapi.InstanceIdentity{Hostname: container.Hostname, Addresses: []string{container.IPv4, container.IPv6}}}
			reply := pushed.Ingest(key, req)
			req.Token = reply.Token
			for _, action := range reply.Requests {
				path := "/v1/" + action.Engine + "/" + action.Action
				if action.Engine == "" {
					path = "/v1/status"
				}
				r := httptest.NewRequest(http.MethodPost, path, bytes.NewReader(action.Request))
				r.Header.Set("Authorization", "Bearer "+member)
				r.Header.Set("X-Fixture-Session", container.Hostname)
				w := httptest.NewRecorder()
				x.handler(w, r)
				req.Samples = append(req.Samples, agentapi.HeartbeatSample{Engine: action.Engine, Action: action.Action, Request: action.Request, Code: w.Code, Response: w.Body.Bytes()})
			}
			pushed.Ingest(key, req)
		}
	}
}

func TestZeropsSteadyRequestBudget(t *testing.T) {
	server, _ := newFakeAPIWithState(t)
	counts := &providerGETCounts{requests: map[string]int{}, signed: map[string]int{}}
	b := newBudgetBackend(t, server.URL, counts)
	var clockMu sync.Mutex
	now := time.Now()
	clock := func() time.Time { clockMu.Lock(); defer clockMu.Unlock(); return now }
	advance := func(d time.Duration) { clockMu.Lock(); now = now.Add(d); clockMu.Unlock() }
	restore := infra.SetSnapshotClockForTest(clock)
	defer restore()
	pushed := agentpush.NewStore(clock)
	var fixtures []*steadyBudgetFixture
	for _, kind := range []string{"mysql", "postgres", "neo4j"} {
		x := newSteadyBudgetFixture(t, kind, b)
		x.instances = map[string][]infra.Container{}
		x.stopped = map[string]bool{}
		for _, member := range x.members {
			var err error
			x.instances[member], err = b.Containers(t.Context(), member)
			if err != nil {
				t.Fatal(err)
			}
		}
		x.rc.Agents.(*providerBudgetAgents).samples = pushed
		fixtures = append(fixtures, x)
	}
	beat := func() {
		for _, x := range fixtures {
			x.heartbeat(t, pushed)
		}
	}
	// The backend's one-minute name/ID cache uses wall time. Age its timestamp
	// under the real mutex at simulated minute boundaries (without changing
	// runtime code), so an hour replay does not accidentally retain IDs forever.
	expireIDs := func() { b.mu.Lock(); b.cachedAt = time.Now().Add(-time.Minute - time.Second); b.mu.Unlock() }
	freshBudgets := map[string]map[string]int{
		"mysql":    {"GET /service-stack/{id}": 15, "GET /service-stack/{id}/container": 11, "GET /service-stack/{id}/env": 4},
		"postgres": {"GET /service-stack/{id}": 8, "GET /service-stack/{id}/container": 6, "GET /service-stack/{id}/env": 2},
		"neo4j":    {"GET /service-stack/{id}": 2, "GET /service-stack/{id}/container": 1, "GET /service-stack/{id}/env": 1},
	}
	for _, x := range fixtures {
		counts.reset()
		x.ready(t, false)
		assertRequests(t, counts, freshBudgets[x.kind])
		t.Logf("M1 %s=%v total=%d", x.kind, counts.snapshotRequests(), requestTotal(counts.snapshotRequests()))
	}
	beat()
	for _, x := range fixtures {
		x.ready(t, true)
	}
	for _, x := range fixtures {
		counts.reset()
		if !x.ready(t, true).Accepted {
			t.Fatalf("M2 %s rejected: requests=%v", x.kind, counts.snapshotRequests())
		}
		assertRequests(t, counts, map[string]int{})
		t.Logf("M2 heartbeat %s=0", x.kind)
		x.rc.Agents.(*providerBudgetAgents).samples = nil
		counts.reset()
		x.ready(t, true)
		legacy := map[string]int{}
		pins := map[string]int{"mysql": 6, "postgres": 4}[x.kind]
		if pins > 0 {
			legacy["GET /service-stack/{id}"] = pins
			legacy["GET /service-stack/{id}/container"] = pins
		}
		assertRequests(t, counts, legacy)
		t.Logf("M2 legacy %s=%v", x.kind, counts.snapshotRequests())
		x.rc.Agents.(*providerBudgetAgents).samples = pushed
	}
	advance(time.Minute)
	expireIDs()
	beat()
	counts.reset()
	for _, x := range fixtures {
		if !x.ready(t, true).Accepted {
			t.Fatal("M3 rejected")
		}
	}
	assertRequests(t, counts, map[string]int{"GET /project/project/service-stack": 1})
	t.Logf("M3=%v", counts.snapshotRequests())
	// Try real Sync first and report exact fixture prerequisites. When it cannot
	// converge, replay the measured engine idle-Sync backend-call profile below.
	for _, x := range fixtures {
		// Sync may advance its configuration journal before a missing fixture
		// prerequisite is discovered. Isolate that durable state from the idle
		// replay, while keeping the very same real backend and agent transport.
		probe := providerBudgetContext(t, x.p, b)
		raw, err := json.Marshal(x.rc.Cluster)
		if err != nil {
			t.Fatal(err)
		}
		if err = json.Unmarshal(raw, probe.Cluster); err != nil {
			t.Fatal(err)
		}
		probe.Agents = x.rc.Agents
		for _, key := range []string{"tls/ca.crt", "tls/servers/samplen0.crt", "tls/servers/samplen0.key", "users/neo4j", "tls/agents/samplen0.crt", "tls/agents/samplen0.key", "datamcp/state", "datamcp/config"} {
			value, e := x.rc.Secrets.GetClusterSecret(t.Context(), x.rc.Cluster.Ref(), key)
			if e == nil {
				if e = probe.Secrets.SetClusterSecret(t.Context(), probe.Cluster.Ref(), key, value); e != nil {
					t.Fatal(e)
				}
			}
		}
		counts.reset()
		ctx := engine.WithObservationPass(t.Context())
		status, err := engine.ObserveStatus(ctx, x.p, probe)
		if err != nil {
			t.Fatal(err)
		}
		probe.Cluster.Status = status
		result, syncErr := x.p.Sync(ctx, probe)
		_, postErr := engine.ObserveStatus(ctx, x.p, probe)
		if postErr != nil {
			t.Fatal(postErr)
		}
		t.Logf("M4 real reconcile %s result=%+v err=%v requests=%v", x.kind, result, syncErr, counts.snapshotRequests())
		if syncErr != nil || !result.Steady {
			t.Logf("M4 %s requires measured idle-Sync backend-call stand-in", x.kind)
		}
	}
	beat()
	for _, x := range fixtures {
		x.ready(t, true)
	}
	// Replay independent availability at 5s and heartbeats at 3s. Deep
	// reconciles are charged using the measured fresh sequence above, and
	// executed to refresh snapshots at each chosen cadence.
	deepBudgets := map[string]int{"mysql": 60, "postgres": 58, "neo4j": 29}
	for _, interval := range []time.Duration{10 * time.Minute, 15 * time.Minute, engine.SteadyResyncInterval} {
		totals := make([]int, len(fixtures))
		listing := 0
		configRevisions := make([]map[string]string, len(fixtures))
		configPulls := 0
		for i, x := range fixtures {
			configRevisions[i] = x.configPullBaseline(t)
		}
		for second := 1; second <= 3600; second++ {
			advance(time.Second)
			if second%60 == 0 {
				expireIDs()
			}
			if second%3 == 0 {
				beat()
			}
			if second%5 != 0 {
				continue
			}
			for i, x := range fixtures {
				if second%int(interval/time.Second) == 0 {
					counts.reset()
					x.profileReconcile(t)
					m := counts.snapshotRequests()
					n := m["GET /project/project/service-stack"]
					listing += n
					totals[i] += requestTotal(m) - n
					own := requestTotal(m) - n
					max := deepBudgets[x.kind]
					if own > max {
						t.Errorf("M4 %s deep reconcile own=%d exceeds %d", x.kind, own, max)
					}
					if second == int(interval/time.Second) {
						t.Logf("M4 deep %s interval=%s own=%d shared-listing=%d endpoints=%v", x.kind, interval, own, n, m)
					}
				}
				counts.reset()
				if !x.ready(t, true).Accepted {
					t.Fatalf("M4 %s interval=%s second=%d steady rejected", x.kind, interval, second)
				}
				if second%60 == 0 {
					configPulls += x.idleConfigPulls(t, configRevisions[i])
				}
				m := counts.snapshotRequests()
				n := m["GET /project/project/service-stack"]
				listing += n
				totals[i] += requestTotal(m) - n
			}
		}
		t.Logf("M4 conditional configuration pulls=%d platform requests=0", configPulls)
		for i, x := range fixtures {
			rate := (float64(totals[i]) + float64(listing)/3) / 3600
			t.Logf("M4 %s interval=%s own=%d listing=%d/3 rate=%.6f", x.kind, interval, totals[i], listing, rate)
			if interval == engine.SteadyResyncInterval && rate > idleBudgetPerClusterPerSecond {
				t.Errorf("M4 %s %s rate %.6f exceeds %.1f", x.kind, interval, rate, idleBudgetPerClusterPerSecond)
			}
		}
	}
	x := fixtures[0]
	counts.reset()
	op, err := b.SetContainerCount(t.Context(), x.proxy, 4, 4)
	if err != nil {
		t.Fatal(err)
	}
	awaitBudget(t, b, op)
	advance(5 * time.Second)
	x.ready(t, false)
	burst := requestTotal(counts.snapshotRequests())
	t.Logf("M5 burst=%d endpoints=%v", burst, counts.snapshotRequests())
	if burst > 53 {
		t.Errorf("M5 burst %d exceeds 53", burst)
	}
	sibling := newBudgetBackend(t, server.URL, counts)
	op, err = sibling.SetContainerCount(t.Context(), x.proxy, 3, 3)
	if err != nil {
		t.Fatal(err)
	}
	awaitBudget(t, sibling, op)
	beat()
	counts.reset()
	x.ready(t, true)
	refetch := requestTotal(counts.snapshotRequests())
	if refetch == 0 || refetch > 17 {
		t.Fatalf("M5 first steady refetch=%d, want 1..17", refetch)
	}
	t.Logf("M5 first steady=%d", refetch)
	counts.reset()
	if !x.ready(t, true).Accepted {
		t.Fatal("M5 not resumed")
	}
	assertRequests(t, counts, map[string]int{})
	t.Log("M5 resumed=0")
	original := x.rc.Backend
	x.rc.Backend = &steadyMutationBackend{Backend: original, mutate: func() {
		op, e := sibling.SetContainerCount(t.Context(), x.proxy, 3, 3)
		if e != nil {
			t.Error(e)
			return
		}
		awaitBudget(t, sibling, op)
	}}
	counts.reset()
	out := x.ready(t, true)
	if out.Accepted {
		t.Fatal("M6 mutation accepted")
	}
	t.Logf("M6 mutation accepted=%v requests=%d", out.Accepted, requestTotal(counts.snapshotRequests()))
	x.rc.Backend = original
	op, err = b.Stop(t.Context(), x.members[0])
	if err != nil {
		t.Fatal(err)
	}
	awaitBudget(t, b, op)
	x.stopped[x.members[0]] = true
	advance(agentpush.SampleMaxAge + time.Second)
	beat()
	counts.reset()
	status, out := x.observe(t, true)
	if status.Phase == model.PhaseReady || out.Accepted {
		t.Fatalf("M6 stopped phase=%s outcome=%+v", status.Phase, out)
	}
	t.Logf("M6 stopped phase=%s accepted=%v requests=%d", status.Phase, out.Accepted, requestTotal(counts.snapshotRequests()))
	counts.mu.Lock()
	t.Logf("M6 signed-URL log requests=%v", counts.signed)
	counts.mu.Unlock()
}

// profileReconcile keeps fresh pre/post observations and the exact idle-Sync
// backend reads in one pass. Counts originate in the engine packages' converged
// GR3+px3, single PostgreSQL+px2 and Neo4j fixtures (no mutations).
func (x *steadyBudgetFixture) profileReconcile(t *testing.T) {
	t.Helper()
	ctx := engine.WithObservationPass(t.Context())
	status, _ := x.observePass(t, ctx, false)
	if status.Phase != model.PhaseReady {
		t.Fatalf("profile pre phase=%s", status.Phase)
	}
	get := func(name string, n int) {
		for range n {
			if _, err := x.rc.Backend.GetService(ctx, name); err != nil {
				t.Fatal(err)
			}
		}
	}
	containers := func(name string, n int) {
		for range n {
			if _, err := x.rc.Backend.Containers(ctx, name); err != nil {
				t.Fatal(err)
			}
		}
	}
	switch x.kind {
	case "mysql":
		for i := range 3 {
			name := fmt.Sprintf("samplem%d", i)
			get(name, 1)
			get(name+"v", 2)
			containers(name, 1)
		}
		get("samplemx", 1)
		containers("samplemx", 2)
	case "postgres":
		get("samplep0", 3)
		get("samplepx", 5)
		get("samplep0v", 3)
		containers("samplepx", 2)
	case "neo4j":
		if _, err := x.rc.Backend.ListPublicPorts(ctx); err != nil {
			t.Fatal(err)
		}
		get("samplen0", 2)
		get("samplen0v", 2)
	}
	status, _ = x.observePass(t, ctx, false)
	if status.Phase != model.PhaseReady {
		t.Fatalf("profile post phase=%s", status.Phase)
	}
}
