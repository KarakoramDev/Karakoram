// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package nomad

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"net"
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

// The real pre/Sync/post probe below cannot complete an idle Sync: MySQL
// lacks persisted agent credentials; PostgreSQL's imported runtime lacks
// verified cluster ownership; Neo4j lacks tls/ca.key and real leaf certificates.
// Replay the measured engine idle-Sync backend-call profile instead of counting
// those early returns, or substituting an arbitrary extra full status pass.

type steadyBudgetFixture struct {
	kind      string
	p         engine.Provider
	rc        *engine.Context
	members   []string
	proxy     string
	handler   http.HandlerFunc
	companion *companion.Controller
	instances map[string][]infra.Container
	agents    *providerBudgetAgents
}

type providerGETCounts struct {
	f     *fakeNomad
	start int
}

func countProviderGETs(f *fakeNomad) *providerGETCounts {
	c := &providerGETCounts{f: f}
	c.reset()
	return c
}
func (c *providerGETCounts) reset() {
	c.f.mu.Lock()
	c.start = len(c.f.calls)
	c.f.mu.Unlock()
}
func (c *providerGETCounts) snapshotRequests() map[string]int {
	// Count the fake's recorded suffix in place under its mutex. Copying the
	// entire growing history for each zero-request renewal is quadratic in the
	// replay length and needlessly dominates the race-enabled harness.
	c.f.mu.Lock()
	defer c.f.mu.Unlock()
	out := map[string]int{}
	for _, r := range c.f.calls[c.start:] {
		path := r.Path
		switch {
		case strings.HasPrefix(path, "/v1/allocation/"):
			path = "/v1/allocation/{id}"
		case strings.HasPrefix(path, "/v1/var/"):
			path = "/v1/var/{path}"
		case strings.HasPrefix(path, "/v1/job/"):
			_, suffix, hasSuffix := strings.Cut(strings.TrimPrefix(path, "/v1/job/"), "/")
			path = "/v1/job/{id}"
			if hasSuffix {
				path += "/" + suffix
			}
		}
		out[r.Method+" "+path]++
	}
	return out
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

// M1 is a per-endpoint maximum: concurrent immutable-address hydration may
// converge before another reader starts, so fewer requests are also valid.
func assertFreshRequestBudget(t *testing.T, c *providerGETCounts, maximum map[string]int) {
	t.Helper()
	for endpoint, count := range c.snapshotRequests() {
		if count > maximum[endpoint] {
			t.Errorf("M1 %s=%d exceeds %d", endpoint, count, maximum[endpoint])
		}
	}
}

func newSteadyBudgetFixture(t *testing.T, kind string, b *Backend, f *fakeNomad) *steadyBudgetFixture {
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
	rc := providerBudgetContext(t, p)
	rc.Backend = b
	x := &steadyBudgetFixture{kind: kind, p: p, rc: rc}
	// Each fixture has separate durable state, but all runtimes use one Nomad
	// backend/environment and consequently one process-global target snapshot.
	switch kind {
	case "mysql":
		rc.Cluster.Spec.Topology.Type = "cluster"
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
		providerBudgetState(t, rc, "mysql", map[string]any{"uid": "sample-identity", "configVersion": 1, "topology": "cluster", "members": members, "formation": map[string]any{"state": "formed", "formationEpoch": 1}})
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
	// The fake logical route reaches one deterministic allocation. Heartbeat
	// sample generation sets Host to its own CNI address; neither path bypasses
	// the real pinned dial authorization. Read wire rows without prewarming the
	// provider's immutable CNI cache before the cold M1 measurement.
	sessionFor := func(member, host string) string {
		f.mu.Lock()
		defer f.mu.Unlock()
		first := ""
		for id, a := range f.allocs {
			if fakeString(a.row, "JobID") != member || fakeString(a.row, "ClientStatus") != "running" {
				continue
			}
			addresses, _ := a.row["NetworkStatus"].(fakeRow)
			if host == fakeString(addresses, "Address") || host != "" && host == fakeString(addresses, "AddressIPv6") {
				return id
			}
			if first == "" || id < first {
				first = id
			}
		}
		return first
	}
	x.handler = func(w http.ResponseWriter, r *http.Request) {
		member := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
		host := r.Host
		if h, _, err := net.SplitHostPort(host); err == nil {
			host = h
		}
		session := sessionFor(member, host)
		w.Header().Set(agentapi.HeartbeatSessionHeader, session)
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
	x.agents = rc.Agents.(*providerBudgetAgents)
	return x
}

func (x *steadyBudgetFixture) observe(t *testing.T, steady bool) (model.ClusterStatus, *engine.SteadyOutcome) {
	t.Helper()
	ctx := engine.WithObservationPass(t.Context())
	if steady {
		ctx = engine.WithSteadyEligible(ctx)
	}
	ctx, out := engine.WithSteadyOutcome(ctx)
	status, err := engine.ObserveStatus(ctx, x.p, x.rc)
	if err != nil {
		t.Fatal(err)
	}
	if x.companion != nil {
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
		if x.instances == nil {
			x.instances = map[string][]infra.Container{}
		}
		containers, ok := x.instances[member]
		if !ok {
			var err error
			containers, err = x.rc.Backend.Containers(t.Context(), member)
			if err != nil {
				t.Fatal(err)
			}
			x.instances[member] = containers
		}
		for _, container := range containers {
			if container.Status != infra.ContainerRunning {
				continue
			}
			key := agentpush.MemberKey{Namespace: x.rc.Cluster.Namespace, Cluster: x.rc.Cluster.Name, Member: member}
			req := agentapi.HeartbeatRequest{Session: container.ID, Instance: agentapi.InstanceIdentity{TaskID: container.ID, Hostname: container.Hostname, Addresses: []string{container.IPv4, container.IPv6}}}
			reply := pushed.Ingest(key, req)
			req.Token = reply.Token
			for _, action := range reply.Requests {
				path := "/v1/" + action.Engine + "/" + action.Action
				if action.Engine == "" {
					path = "/v1/status"
				}
				r := httptest.NewRequest(http.MethodPost, path, bytes.NewReader(action.Request))
				r.Host = container.IPv4
				r.Header.Set("Authorization", "Bearer "+member)
				w := httptest.NewRecorder()
				x.handler(w, r)
				req.Samples = append(req.Samples, agentapi.HeartbeatSample{Engine: action.Engine, Action: action.Action, Request: action.Request, Code: w.Code, Response: w.Body.Bytes()})
			}
			pushed.Ingest(key, req)
		}
	}
}

func TestNomadSteadyRequestBudget(t *testing.T) {
	f := newFakeNomad(t)
	b := fakeBackend(t, f, nil)
	counts := countProviderGETs(f)
	var clockMu sync.Mutex
	now := time.Now()
	clock := func() time.Time { clockMu.Lock(); defer clockMu.Unlock(); return now }
	advance := func(d time.Duration) { clockMu.Lock(); now = now.Add(d); clockMu.Unlock() }
	restore := infra.SetSnapshotClockForTest(clock)
	defer restore()
	b.c.http = withConfigListingLatency(b.c.http, advance)
	pushed := agentpush.NewStore(clock)
	var fixtures []*steadyBudgetFixture
	for _, kind := range []string{"mysql", "postgres", "neo4j"} {
		fixtures = append(fixtures, newSteadyBudgetFixture(t, kind, b, f))
	}
	f.mu.Lock()
	f.projectAllocations = true
	f.mu.Unlock()
	// Establish the target listing baseline before the fresh M1 reads populate
	// entries that warm steady observations are expected to reuse.
	b.PlatformSnapshot().Lookup(steadyNomadContext(t), "budget-baseline")
	// MySQL also freshly observes failure-domain placement via the node list.
	budgets := map[string]map[string]int{
		"mysql":    {"GET /v1/nodes": 1, "GET /v1/var/{path}": 8, "GET /v1/job/{id}": 8, "GET /v1/job/{id}/allocations": 14, "GET /v1/allocation/{id}": 9},
		"postgres": {"GET /v1/var/{path}": 2, "GET /v1/job/{id}": 2, "GET /v1/job/{id}/allocations": 6, "GET /v1/allocation/{id}": 3},
		"neo4j":    {"GET /v1/var/{path}": 1, "GET /v1/job/{id}": 1, "GET /v1/job/{id}/allocations": 1, "GET /v1/allocation/{id}": 1},
	}
	for _, x := range fixtures {
		x.rc.Agents.(*providerBudgetAgents).samples = pushed
		counts.reset()
		x.ready(t, false)
		assertFreshRequestBudget(t, counts, budgets[x.kind])
		t.Logf("M1 %s total=%d %v", x.kind, requestTotal(counts.snapshotRequests()), counts.snapshotRequests())
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
			probe, ev := infra.WithEvidence(infra.WithSteadyObservation(infra.ObservationContext(engine.WithObservationPass(t.Context()))))
			status, err := x.p.Status(probe, x.rc)
			oldest, sampled := ev.Oldest()
			t.Logf("M2 diagnostic status=%+v err=%v oldest=%s sampled=%v snapshotValid=%v", status, err, oldest, sampled, ev.SnapshotValid())
			t.Fatalf("M2 %s not accepted: %+v", x.kind, out)
		}
		assertRequests(t, counts, map[string]int{})
		t.Logf("M2 heartbeat %s=0", x.kind)
	}
	// Measured M2: each proxy service reloads one durable record outside the
	// steady memo; allocations still enumerate on every pinned connection.
	legacy := map[string]map[string]int{
		"mysql":    {"GET /v1/var/{path}": 1, "GET /v1/job/{id}/allocations": 6},
		"postgres": {"GET /v1/var/{path}": 1, "GET /v1/job/{id}/allocations": 4},
		"neo4j":    {},
	}
	for _, x := range fixtures {
		x.rc.Agents.(*providerBudgetAgents).samples = nil
		counts.reset()
		x.ready(t, true)
		assertRequests(t, counts, legacy[x.kind])
		t.Logf("M2 legacy %s=%v", x.kind, counts.snapshotRequests())
		x.rc.Agents.(*providerBudgetAgents).samples = pushed
	}
	advance(infra.SnapshotListingInterval)
	beat()
	counts.reset()
	for _, x := range fixtures {
		if !x.ready(t, true).Accepted {
			t.Fatal("M3 not accepted")
		}
	}
	assertRequests(t, counts, map[string]int{"GET /v1/jobs": 1, "GET /v1/allocations": 1})
	t.Logf("M3 shared target=%v", counts.snapshotRequests())
	// Replay production and the two shorter diagnostic schedules to compare
	// reconcile cadence, not snapshot expiry (SnapshotMaxAge is forty minutes).
	// Only the steady target listing is shared; fresh reconcile reads stay
	// charged to their cluster kind.
	for _, interval := range []time.Duration{engine.SteadyResyncInterval, 10 * time.Minute, 15 * time.Minute} {
		for _, x := range fixtures {
			x.ready(t, false)
		}
		beat()
		totals := make([]int, len(fixtures))
		deepCosts, deepRuns := make([]int, len(fixtures)), make([]int, len(fixtures))
		listing := 0
		configRevisions := make([]map[string]string, len(fixtures))
		configPulls := 0
		for i, x := range fixtures {
			x.ready(t, true)
			configRevisions[i] = x.configPullBaseline(t)
		}
		gridStart := clock().Truncate(engine.ReadinessInterval)
		for second := 1; second <= 3600; second++ {
			if remaining := gridStart.Add(time.Duration(second) * time.Second).Sub(clock()); remaining > 0 {
				advance(remaining)
			}
			if second%int(agentapi.HeartbeatInterval/time.Second) == 0 {
				beat()
			}
			if second%int(engine.ReadinessInterval/time.Second) != 0 {
				continue
			}
			for i, x := range fixtures {
				counts.reset()
				if second%int(interval/time.Second) == 0 {
					x.reconcileWithMeasuredIdleSyncModel(t)
					deepCosts[i] += requestTotal(counts.snapshotRequests())
					deepRuns[i]++
				}
				totals[i] += requestTotal(counts.snapshotRequests())
				counts.reset()
				if !x.ready(t, true).Accepted {
					t.Fatal("M4 steady attempt rejected")
				}
				if second%60 == 0 {
					configPulls += x.idleConfigPulls(t, configRevisions[i])
				}
				m := counts.snapshotRequests()
				shared := m["GET /v1/jobs"] + m["GET /v1/allocations"]
				listing += shared
				totals[i] += requestTotal(m) - shared
			}
		}
		t.Logf("M4 conditional configuration pulls=%d platform requests=0", configPulls)
		for i, x := range fixtures {
			attributed := float64(totals[i]) + float64(listing)/float64(len(fixtures))
			rate := attributed / 3600
			t.Logf("M4 %s interval=%s own=%d shared=%d/3 attributed=%.3f req/s=%.6f", x.kind, interval, totals[i], listing, attributed, rate)
			t.Logf("M4 %s deep pre+measured-Sync+post requests=%d/%d=%.3f", x.kind, deepCosts[i], deepRuns[i], float64(deepCosts[i])/float64(deepRuns[i]))
			if interval == engine.SteadyResyncInterval && rate > idleBudgetPerClusterPerSecond {
				t.Errorf("M4 %s %s %.6f exceeds %.1f", x.kind, interval, rate, idleBudgetPerClusterPerSecond)
			}
		}
	}
	// Real non-steady backend operation and fresh observations, then resumed
	// steady renewals. Restore the original count before continuing safety work.
	x := fixtures[0]
	counts.reset()
	phaseCounts := countProviderGETs(f)
	op, err := b.SetContainerCount(t.Context(), x.proxy, 4, 4)
	if err != nil {
		t.Fatal(err)
	}
	directOperationRequests := requestTotal(phaseCounts.snapshotRequests())
	observeAndCompleteRequests := 0
	done := false
	for range 10 {
		advance(engine.ReadinessInterval)
		phaseCounts.reset()
		state, e := b.OpStatus(t.Context(), op)
		if e != nil {
			t.Fatal(e)
		}
		if state.Failed {
			t.Fatalf("M5 operation failed: %+v", state)
		}
		directOperationRequests += requestTotal(phaseCounts.snapshotRequests())
		phaseCounts.reset()
		x.ready(t, false)
		observeAndCompleteRequests += requestTotal(phaseCounts.snapshotRequests())
		if state.Done {
			done = true
			break
		}
	}
	if !done {
		t.Fatal("M5 operation did not finish within ten 5s observations")
	}
	burst := counts.snapshotRequests()
	// The combined cap includes scale submission/completion plus the existing
	// two-attempt fresh observation path. A fresh four-proxy attempt costs at
	// most nodes=1, var=8, job=8, allocations=16, detail=8 (new proxy
	// allocations may hydrate concurrently twice). ObserveStatus reads can advance
	// the scale operation through resumePending, so the diagnostic phase totals
	// below are not independently attributable to operation versus observation.
	const scaleOperationRequestMaximum = 78
	const freshFourProxyAttemptMaximum = 1 + 8 + 8 + 16 + 8
	const scaleBurstRequestMaximum = scaleOperationRequestMaximum + 2*freshFourProxyAttemptMaximum
	t.Logf("M5 burst=%d directOperation=%d observeAndComplete=%d bound=%d %v", requestTotal(burst), directOperationRequests, observeAndCompleteRequests, scaleBurstRequestMaximum, burst)
	if requestTotal(burst) > scaleBurstRequestMaximum {
		t.Errorf("M5 burst=%d exceeds %d", requestTotal(burst), scaleBurstRequestMaximum)
	}
	// The fresh operation pass warms the cache. A separate backend instance's
	// real mutation must still invalidate that warmed target.
	sibling := fakeBackend(t, f, nil)
	op, err = sibling.SetContainerCount(t.Context(), x.proxy, 3, 3)
	if err != nil {
		t.Fatal(err)
	}
	awaitFake(t, sibling, op)
	for _, fixture := range fixtures {
		fixture.instances = nil
	}
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
	x.rc.Agents = &steadyMutationDialer{base: original, mutate: func(ctx context.Context) {
		// Record the old target epoch in this attempt's actual Evidence before
		// mutating; do not depend on which concurrent platform read wins.
		if _, e := b.GetService(ctx, x.members[0]); e != nil {
			t.Error(e)
			return
		}
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
	x.rc.Agents = original
	op, err = b.Stop(t.Context(), "samplem0")
	if err != nil {
		t.Fatal(err)
	}
	awaitFake(t, b, op)
	x.instances = nil
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
	base   engine.AgentDialer
	once   sync.Once
	mutate func(context.Context)
}

func (d *steadyMutationDialer) Dial(ctx context.Context, ref model.ClusterRef, member string) (*agentapi.Client, error) {
	d.once.Do(func() { d.mutate(ctx) })
	return d.base.Dial(ctx, ref, member)
}

// This probe executes the real reconcile ordering in one pass. Its missing
// lifecycle prerequisites are documented above; early-return reads must not
// masquerade as a complete idle Sync profile.
func TestNomadIdleSyncFixtureProfile(t *testing.T) {
	for _, kind := range []string{"mysql", "postgres", "neo4j"} {
		t.Run(kind, func(t *testing.T) {
			f := newFakeNomad(t)
			b := fakeBackend(t, f, nil)
			x := newSteadyBudgetFixture(t, kind, b, f)
			f.mu.Lock()
			f.projectAllocations = true
			f.mu.Unlock()
			ctx := engine.WithObservationPass(t.Context())
			before, err := engine.ObserveStatus(ctx, x.p, x.rc)
			if err != nil {
				t.Fatal(err)
			}
			counts := countProviderGETs(f)
			result, syncErr := x.p.Sync(ctx, x.rc)
			t.Logf("real idle Sync: result=%+v err=%v requests=%v", result, syncErr, counts.snapshotRequests())
			after, err := engine.ObserveStatus(ctx, x.p, x.rc)
			t.Logf("real reconcile: before=%s after=%s postErr=%v", before.Phase, after.Phase, err)
		})
	}
}

// Reconcile's provider memo belongs to the whole pre/Sync/post sequence. Each
// ObserveStatus still creates an independent fresh platform observation cache.
// Keep the fixture's logical pools warm across passes, as the production dialer
// does. Test cleanup closes them. The test clock separates this request-count
// replay's wall-clock sweep budget from production's five-second latency limit.
func (x *steadyBudgetFixture) reconcileWithMeasuredIdleSyncModel(t *testing.T) {
	t.Helper()
	ctx := engine.WithObservationPass(t.Context())
	fresh := func() {
		status, err := engine.ObserveStatus(ctx, x.p, x.rc)
		if err != nil {
			t.Fatal(err)
		}
		if status.Phase != model.PhaseReady {
			t.Fatalf("M4 %s fresh phase=%s", x.kind, status.Phase)
		}
		if x.companion != nil {
			if err = x.companion.Observe(ctx, x.rc, &status); err != nil {
				t.Fatal(err)
			}
		}
	}
	fresh()
	x.measuredIdleSyncBackendCalls(t, ctx)
	fresh()
}

// The engine idle-Sync profile is measured with real engines in their converged
// fixtures. Replay its exact per-member backend calls against this real Nomad
// backend, so volume, data and proxy costs (and CNI hydration) are measured, not
// estimated from a single average method cost. Local Layout/Capabilities calls
// have no HTTP cost. Calls stay fresh: this is the Sync context, not observation.
func (x *steadyBudgetFixture) measuredIdleSyncBackendCalls(t *testing.T, ctx context.Context) {
	t.Helper()
	get := func(member string, count int) {
		for range count {
			if _, err := x.rc.Backend.GetService(ctx, member); err != nil {
				t.Fatal(err)
			}
		}
	}
	containers := func(member string, count int) {
		for range count {
			if _, err := x.rc.Backend.Containers(ctx, member); err != nil {
				t.Fatal(err)
			}
		}
	}
	switch x.kind {
	case "mysql":
		for _, name := range x.members[:3] {
			get(name, 1)
			get(name+"v", 2)
			containers(name, 1)
		}
		get(x.proxy, 1)
		containers(x.proxy, 2)
	case "postgres":
		get("samplep0", 3)
		get("samplep0v", 3)
		get(x.proxy, 5)
		containers(x.proxy, 2)
	case "neo4j":
		if _, err := x.rc.Backend.ListPublicPorts(ctx); err != nil {
			t.Fatal(err)
		}
		get("samplen0", 2)
		get("samplen0v", 2)
	}
}
