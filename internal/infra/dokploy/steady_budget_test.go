// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package dokploy

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
	"sync/atomic"
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

type steadyBudgetClock func() time.Time

func (c steadyBudgetClock) Now() time.Time { return c() }

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
	legacy    atomic.Bool
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
	rc, _, _ := providerBudgetContext(t, p)
	rc.Backend = b
	x := &steadyBudgetFixture{kind: kind, p: p, rc: rc}
	// Each fixture has separate durable state, but all runtimes use one Dokploy
	// backend/environment and consequently one process-global target snapshot.
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
			members = append(members, map[string]any{"name": name, "role": role, "index": i, "stage": "joined", "serviceId": id, "replicas": count, "configVersion": 1, "version": rc.Cluster.Spec.Version})
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
		session := r.Header.Get("X-Budget-Session")
		if session == "" {
			member := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
			for _, instance := range x.instances[member] {
				if instance.Status == infra.ContainerRunning {
					session = instance.Hostname
					break
				}
			}
		}
		if !x.legacy.Load() {
			w.Header().Set(agentapi.HeartbeatSessionHeader, session)
		}
		switch r.URL.Path {
		case "/v1/status":
			_ = json.NewEncoder(w).Encode(agentapi.Status{ConfigVersion: 1})
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
	ctx := engine.WithObservationPass(t.Context())
	if steady {
		ctx = engine.WithSteadyEligible(ctx)
	}
	return x.observeContext(t, ctx, steady)
}
func (x *steadyBudgetFixture) observeContext(t *testing.T, ctx context.Context, steady bool) (model.ClusterStatus, *engine.SteadyOutcome) {
	t.Helper()
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
			if container.Status != infra.ContainerRunning {
				continue
			}
			key := agentpush.MemberKey{Namespace: x.rc.Cluster.Namespace, Cluster: x.rc.Cluster.Name, Member: member}
			req := agentapi.HeartbeatRequest{Session: container.Hostname, Instance: agentapi.InstanceIdentity{Hostname: container.Hostname}}
			reply := pushed.Ingest(key, req)
			req.Token = reply.Token
			for _, action := range reply.Requests {
				path := "/v1/" + action.Engine + "/" + action.Action
				if action.Engine == "" {
					path = "/v1/status"
				}
				r := httptest.NewRequest(http.MethodPost, path, bytes.NewReader(action.Request))
				r.Header.Set("Authorization", "Bearer "+member)
				r.Header.Set("X-Budget-Session", req.Session)
				w := httptest.NewRecorder()
				x.handler(w, r)
				req.Samples = append(req.Samples, agentapi.HeartbeatSample{Engine: action.Engine, Action: action.Action, Request: action.Request, Code: w.Code, Response: w.Body.Bytes()})
			}
			pushed.Ingest(key, req)
		}
	}
}

func TestDokploySteadyRequestBudget(t *testing.T) {
	f := newFakeAPI(t)
	b := fakeBackend(t, f, nil)
	counts := countProviderGETs(f)
	var clockMu sync.Mutex
	now := time.Now()
	clock := func() time.Time { clockMu.Lock(); defer clockMu.Unlock(); return now }
	advance := func(d time.Duration) { clockMu.Lock(); now = now.Add(d); clockMu.Unlock() }
	restore := infra.SetSnapshotClockForTest(clock)
	defer restore()
	pushed := agentpush.NewStore(clock)
	var fixtures []*steadyBudgetFixture
	for _, kind := range []string{"mysql", "postgres", "neo4j"} {
		fixtures = append(fixtures, newSteadyBudgetFixture(t, kind, b))
		fixtures[len(fixtures)-1].rc.Clock = steadyBudgetClock(clock)
	}
	f.mu.Lock()
	for _, app := range f.apps {
		for _, task := range app.tasks {
			if task["state"] == "running" {
				task["health"] = "healthy"
			}
		}
	}
	f.mu.Unlock()
	for _, x := range fixtures {
		x.instances = make(map[string][]infra.Container)
		x.stopped = make(map[string]bool)
		for _, member := range x.members {
			var err error
			x.instances[member], err = b.Containers(t.Context(), member)
			if err != nil {
				t.Fatal(err)
			}
		}
	}
	budgets := []map[string]int{
		{"GET environment.one": 1, "GET application.one": 7, "GET cluster.getNodes": 1, "GET swarm.getNodeApps": 1, "GET docker.getConfig": 4, "GET deployment.all": 4, "GET docker.getServiceContainersByAppName": 10},
		{"GET environment.one": 1, "GET application.one": 3, "GET cluster.getNodes": 1, "GET swarm.getNodeApps": 1, "GET docker.getConfig": 2, "GET deployment.all": 2, "GET docker.getServiceContainersByAppName": 6},
		{"GET environment.one": 1, "GET application.one": 2, "GET cluster.getNodes": 1, "GET swarm.getNodeApps": 1, "GET docker.getConfig": 1, "GET deployment.all": 1, "GET docker.getServiceContainersByAppName": 1},
	}
	for i, x := range fixtures {
		x.rc.Agents.(*providerBudgetAgents).samples = pushed
		counts.reset()
		x.ready(t, false)
		assertRequests(t, counts, budgets[i])
		t.Logf("M1 %s %v", x.kind, counts.snapshotRequests())
	}
	beat := func() {
		for _, x := range fixtures {
			x.heartbeat(t, pushed)
		}
	}
	beat()
	// Prime the target listing once; heartbeat preparation is fixture work and
	// excluded from the measured platform traffic.
	for _, x := range fixtures {
		x.ready(t, true)
	}
	for _, x := range fixtures {
		counts.reset()
		out := x.ready(t, true)
		if !out.Accepted {
			t.Fatalf("M2 %s not accepted: %+v", x.kind, out)
		}
		assertRequests(t, counts, map[string]int{})
		t.Logf("M2 heartbeat %s=%d", x.kind, requestTotal(counts.snapshotRequests()))
	}
	legacy := agentpush.NewStore(clock)
	for i, x := range fixtures {
		// Old agents neither push nor advertise a heartbeat session header.
		x.legacy.Store(true)
		x.rc.Agents.(*providerBudgetAgents).samples = legacy
		counts.reset()
		x.ready(t, true)
		want := map[string]int{}
		if i < 2 {
			want["GET docker.getServiceContainersByAppName"] = []int{6, 4}[i]
			// Legacy pinned proxy dials independently revalidate discovery and
			// ownership; steady-populated memos cannot authorize a connection.
			want["GET environment.one"] = 1
			want["GET application.one"] = 1
			want["GET swarm.getNodeApps"] = 1
			want["GET docker.getConfig"] = 1
		}
		assertRequests(t, counts, want)
		t.Logf("M2 legacy %s=%v", x.kind, counts.snapshotRequests())
		x.rc.Agents.(*providerBudgetAgents).samples = pushed
		x.legacy.Store(false)
	}
	advance(infra.SnapshotListingInterval)
	beat()
	counts.reset()
	for _, x := range fixtures {
		if !x.ready(t, true).Accepted {
			t.Fatal("M3 not accepted")
		}
	}
	assertRequests(t, counts, map[string]int{"GET environment.one": 1})
	t.Logf("M3 shared target=%v", counts.snapshotRequests())
	// Replay a full hour on a 1s clock: heartbeats every 3s and independent
	// availability every ReadinessInterval. Preparation reads are not traffic.
	totals := make([]int, len(fixtures))
	deepTotals := make([]int, len(fixtures))
	deepPass := make([]int, len(fixtures))
	listing := 0
	configRevisions := make([]map[string]string, len(fixtures))
	configPulls := 0
	for i, x := range fixtures {
		configRevisions[i] = x.configPullBaseline(t)
	}
	for second := 1; second <= 3600; second++ {
		advance(time.Second)
		if second%int(agentapi.HeartbeatInterval/time.Second) == 0 {
			beat()
		}
		if second%int(engine.ReadinessInterval/time.Second) != 0 {
			continue
		}
		for i, x := range fixtures {
			counts.reset()
			if second%int(engine.SteadyResyncInterval/time.Second) == 0 {
				counts.reset()
				x.idleReconcileModel(t)
				t.Logf("M4 deep %s requests=%d endpoints=%v", x.kind, requestTotal(counts.snapshotRequests()), counts.snapshotRequests())
				totals[i] += requestTotal(counts.snapshotRequests())
				deepPass[i] = requestTotal(counts.snapshotRequests())
				deepTotals[i] += deepPass[i]
				counts.reset()
			}
			if !x.ready(t, true).Accepted {
				t.Fatalf("M4 %s rejected at second %d", x.kind, second)
			}
			if second%60 == 0 {
				configPulls += x.idleConfigPulls(t, configRevisions[i])
			}
			m := counts.snapshotRequests()
			listing += m["GET environment.one"]
			totals[i] += requestTotal(m) - m["GET environment.one"]
		}
	}
	for i, x := range fixtures {
		attributed := float64(totals[i]) + float64(listing)/float64(len(fixtures))
		rate := attributed / 3600
		t.Logf("M4 %s own=%d shared-listing=%d/3 attributed=%.2f req/s=%.6f interval=%s", x.kind, totals[i], listing, attributed, rate, engine.SteadyResyncInterval)
		for _, interval := range []time.Duration{10 * time.Minute, 15 * time.Minute} {
			// Diagnostic projection holds measured availability/background cost
			// constant and substitutes only the deep-reconcile cadence.
			projected := (attributed - float64(deepTotals[i]) + float64(deepPass[i])*float64(time.Hour/interval)) / 3600
			t.Logf("M4 diagnostic %s interval=%s req/s=%.6f", x.kind, interval, projected)
		}
		if rate > idleBudgetPerClusterPerSecond {
			t.Errorf("M4 %s %.6f exceeds %.1f", x.kind, rate, idleBudgetPerClusterPerSecond)
		}
	}
	t.Logf("M4 shared availability listing requests=%d (deep-pass listings remain charged to their cluster)", listing)
	t.Logf("M4 conditional configuration pulls=%d platform requests=0", configPulls)
	// Real non-steady backend operation and fresh observations, then resumed
	// steady renewals. Restore the original count before continuing safety work.
	x := fixtures[0]
	counts.reset()
	op, err := b.SetContainerCount(t.Context(), x.proxy, 4, 4)
	if err != nil {
		t.Fatal(err)
	}
	awaitFake(t, b, op)
	x.ready(t, false)
	burst := counts.snapshotRequests()
	t.Logf("M5 burst=%d %v", requestTotal(burst), burst)
	assertRequests(t, counts, map[string]int{
		"GET application.one": 20, "GET cluster.getNodes": 1,
		"GET deployment.all": 4, "GET docker.getConfig": 16,
		"GET docker.getServiceContainersByAppName": 22,
		"GET environment.one":                      2, "GET swarm.getNodeApps": 13,
		"POST application.reload": 1, "POST application.update": 2,
	})
	// The fresh operation pass warms the cache. A separate backend instance's
	// real mutation must still invalidate that warmed target.
	sibling := fakeBackend(t, f, nil)
	op, err = sibling.SetContainerCount(t.Context(), x.proxy, 3, 3)
	if err != nil {
		t.Fatal(err)
	}
	awaitFake(t, sibling, op)
	x.instances[x.proxy], err = b.Containers(t.Context(), x.proxy)
	if err != nil {
		t.Fatal(err)
	}
	advance(agentpush.SampleMaxAge + time.Second)
	beat()
	counts.reset()
	x.ready(t, true)
	refetch := requestTotal(counts.snapshotRequests())
	if refetch == 0 {
		t.Fatal("M5 mutation did not refetch")
	}
	t.Logf("M5 first steady refetch=%d", refetch)
	counts.reset()
	if !x.ready(t, true).Accepted {
		t.Fatal("M5 steady did not resume")
	}
	assertRequests(t, counts, map[string]int{})
	t.Log("M5 resumed=0")
	// Interpose the mutation at the first pushed read, after the attempt has
	// touched its platform snapshot. This uses another real target backend.
	original := x.rc.Agents
	x.rc.Agents = &steadyMutationDialer{base: original, backend: b, mutate: func() {
		op, e := sibling.SetContainerCount(t.Context(), x.proxy, 3, 3)
		if e != nil {
			t.Error(e)
			return
		}
		awaitFake(t, sibling, op)
	}}
	counts.reset()
	out := x.ready(t, true)
	if out.Accepted {
		t.Fatal("M6 mutation-crossing attempt accepted")
	}
	t.Logf("M6 mutation accepted=%v fallback requests=%d", out.Accepted, requestTotal(counts.snapshotRequests()))
	if requestTotal(counts.snapshotRequests()) == 0 {
		t.Fatal("M6 rejected attempt did not fall back to fresh reads")
	}
	x.rc.Agents = original
	op, err = b.Stop(t.Context(), "samplem0")
	if err != nil {
		t.Fatal(err)
	}
	awaitFake(t, b, op)
	x.stopped["samplem0"] = true
	advance(agentpush.SampleMaxAge + time.Second)
	beat()
	counts.reset()
	status, out := x.observe(t, true)
	if status.Phase == model.PhaseReady || out.Accepted {
		t.Fatalf("M6 stopped member phase=%s outcome=%+v", status.Phase, out)
	}
	t.Logf("M6 stopped phase=%s accepted=%v requests=%d", status.Phase, out.Accepted, requestTotal(counts.snapshotRequests()))
}

type steadyMutationDialer struct {
	base    engine.AgentDialer
	once    sync.Once
	mutate  func()
	backend *Backend
	err     error
}

func (d *steadyMutationDialer) Dial(ctx context.Context, ref model.ClusterRef, member string) (*agentapi.Client, error) {
	d.once.Do(func() {
		// Pin this attempt's epoch before the sibling mutates; do not depend
		// on scheduling between platform and agent observation goroutines.
		_, d.err = d.backend.GetService(ctx, "samplem0")
		if d.err == nil {
			d.mutate()
		}
	})
	if d.err != nil {
		return nil, d.err
	}
	return d.base.Dial(ctx, ref, member)
}

// Replay the per-host idle-Sync profile measured against real engine Sync by
// the backend-call harness. The formed status fixture cannot itself run Sync:
// its synthetic volume ownership and administration credentials are not valid
// lifecycle admission proofs. Do not bypass those gates to make a benchmark.
// Both fresh observations and the model share the Manager's one pass context.
func (x *steadyBudgetFixture) idleReconcileModel(t *testing.T) {
	t.Helper()
	ctx := engine.WithObservationPass(t.Context())
	observe := func() {
		status, _ := x.observeContext(t, ctx, false)
		if status.Phase != model.PhaseReady {
			t.Fatalf("deep %s phase=%s", x.kind, status.Phase)
		}
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
	observe()
	switch x.kind {
	case "mysql":
		for _, name := range x.members[:3] {
			containers(name, 1)
			get(name+"v", 1)
		}
		containers(x.proxy, 1)
		for _, name := range x.members {
			get(name, 1)
		}
		containers(x.proxy, 1)
		for _, name := range x.members[:3] {
			get(name+"v", 1)
		}
	case "postgres":
		for _, name := range []string{"samplep0v", "samplep0", "samplep0v", "samplepx", "samplep0", "samplepx", "samplep0", "samplep0v", "samplepx"} {
			get(name, 1)
		}
		containers(x.proxy, 1)
		get(x.proxy, 2)
		containers(x.proxy, 1)
	case "neo4j":
		if _, err := x.rc.Backend.ListPublicPorts(ctx); err != nil {
			t.Fatal(err)
		}
		for range 2 {
			get("samplen0", 1)
			get("samplen0v", 1)
		}
	}
	observe()
}
