// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

// Package configpulltest drives the production HTTP/manager configuration path
// against existing provider fake APIs. It is imported only by provider tests.
package configpulltest

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/agentpush"
	"github.com/KarakoramDev/Karakoram/internal/auth"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/events"
	"github.com/KarakoramDev/Karakoram/internal/infra"
	"github.com/KarakoramDev/Karakoram/internal/rbac"
	"github.com/KarakoramDev/Karakoram/internal/reconciler"
	"github.com/KarakoramDev/Karakoram/internal/secrets"
	"github.com/KarakoramDev/Karakoram/internal/server"
	"github.com/KarakoramDev/Karakoram/internal/store"
)

// The fixture is already deployed. Isolate the config transport from lifecycle
// ownership proofs the existing budget fixtures cannot satisfy. Config rendering
// delegates unchanged to the real engine; manager eligibility is not stubbed.
type convergedProvider struct {
	engine.Provider
	renders *atomic.Int64
}

func (p convergedProvider) Sync(context.Context, *engine.Context) (engine.Result, error) {
	return engine.Result{Steady: true}, nil
}
func (p convergedProvider) Status(_ context.Context, rc *engine.Context) (domain.ClusterStatus, error) {
	return domain.ClusterStatus{Phase: domain.PhaseReady, Conditions: []domain.Condition{{Type: "Ready", Status: "True"}}, Members: []domain.MemberStatus{{Name: rc.Cluster.Name + "p0", Health: true, Agent: domain.AgentStatus{Healthy: true, LastSeen: rc.Clock.Now()}}}}, nil
}
func (p convergedProvider) MemberConfig(ctx context.Context, rc *engine.Context, member string) (agentapi.MemberConfig, error) {
	p.renders.Add(1)
	return p.Provider.(engine.AgentConfigSource).MemberConfig(ctx, rc, member)
}

func (p convergedProvider) RequiresFreshMemberConfig(ctx context.Context, rc *engine.Context, member string) (bool, error) {
	if source, ok := p.Provider.(engine.FreshMemberConfigSource); ok {
		return source.RequiresFreshMemberConfig(ctx, rc, member)
	}
	return false, nil
}

func Run(t *testing.T, p engine.Provider, rc *engine.Context, advance func(time.Duration), total func() int, withdraw func()) {
	t.Helper()
	st := rc.Store.(*store.Store)
	rc.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	rc.Cluster.Spec.Backup.Storage = "backup"
	rc.Cluster.Spec.Provider = p.Name()
	if p.Name() == "mysql" {
		// These runtime fixtures are fully joined, unlike an initializing GR
		// member whose live clone-donor choice deliberately forbids 304.
		var root map[string]any
		if err := json.Unmarshal(rc.Cluster.Internal, &root); err != nil {
			t.Fatal(err)
		}
		for _, value := range root["mysql"].(map[string]any)["members"].([]any) {
			member := value.(map[string]any)
			if member["role"] == "data" {
				member["initialized"] = true
			}
		}
		raw, err := json.Marshal(root)
		if err != nil {
			t.Fatal(err)
		}
		rc.Cluster.Internal = raw
	}
	for _, key := range []string{
		"users/" + engine.DataAccessUser(engine.DataAccessRead),
		"users/" + engine.DataAccessUser(engine.DataAccessReadWrite),
		"tls/galera/server.crt", "tls/galera/server.key", "tls/galera/ca.crt",
	} {
		if err := st.SetClusterSecret(t.Context(), rc.Cluster.Ref(), key, []byte("fixture")); err != nil {
			t.Fatal(err)
		}
	}
	if err := st.UpdateClusterSpec(t.Context(), rc.Cluster.Ref(), rc.Cluster.Spec); err != nil {
		t.Fatal(err)
	}
	current, err := st.GetCluster(t.Context(), rc.Cluster.Ref())
	if err != nil {
		t.Fatal(err)
	}
	current.Internal = rc.Cluster.Internal
	current.Status = domain.ClusterStatus{Phase: domain.PhaseReady, ObservedGeneration: current.Generation, ObservedAt: rc.Clock.Now(), ObservationEpoch: rc.ObservationEpoch}
	if err = st.UpdateClusterState(t.Context(), current); err != nil {
		t.Fatal(err)
	}
	rc.Cluster = current
	storage := &domain.BackupStorage{Name: "backup", Type: "s3", EndpointURL: "https://storage.example.test", Bucket: "bucket", Region: "region", VerifyTLS: true, Credentials: domain.StorageCredentials{AccessKeyID: "access", SecretAccessKey: "original"}}
	if err = st.CreateBackupStorage(t.Context(), storage); err != nil {
		t.Fatal(err)
	}
	renderCalls := &atomic.Int64{}
	registry, err := engine.NewRegistry(convergedProvider{Provider: p, renders: renderCalls})
	if err != nil {
		t.Fatal(err)
	}
	namespaces := engine.StaticNamespaces{DefaultBackend: rc.Backend}
	manager, err := reconciler.New(reconciler.Options{Store: st, Registry: registry, Namespaces: namespaces, Context: *rc})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- manager.Run(ctx) }()
	defer func() {
		cancel()
		if err := <-done; err != nil {
			t.Error(err)
		}
	}()
	waitSteady := func() {
		t.Helper()
		deadline := time.NewTimer(5 * time.Second)
		defer deadline.Stop()
		tick := time.NewTicker(time.Millisecond)
		defer tick.Stop()
		for {
			c, e := st.GetCluster(t.Context(), current.Ref())
			if e == nil && manager.SteadyConfigBaseline(t.Context(), c) {
				break
			}
			select {
			case <-deadline.C:
				t.Fatal("production manager did not establish config baseline")
			case <-tick.C:
			}
		}
	}
	waitSteady()
	snapshot := rc.Backend.(infra.PlatformSnapshotter).PlatformSnapshot()
	renewalGridStart := rc.Clock.Now()
	listing := func() {
		snapshot.Lookup(infra.WithSteadyObservation(infra.ObservationContext(t.Context())), "config-integration-baseline")
	}
	listing()
	sealer, err := secrets.New(strings.Repeat("c", 32))
	if err != nil {
		t.Fatal(err)
	}
	authentication, err := auth.New(auth.Options{Store: st, Sealer: sealer, PublicURL: "http://localhost"})
	if err != nil {
		t.Fatal(err)
	}
	policy, err := rbac.New(st)
	if err != nil {
		t.Fatal(err)
	}
	cp, err := server.New(server.Options{Store: st, Auth: authentication, RBAC: policy, Registry: registry, Context: *rc, Namespaces: namespaces, Trigger: manager, AgentPush: agentpush.NewStore(nil), Hub: events.NewHub(128), Logger: rc.Logger})
	if err != nil {
		t.Fatal(err)
	}
	defer cp.Close()
	handler := cp.InternalHandler()
	proxy, data, replicas := "samplepx", "samplep0", 2
	if p.Name() == "mysql" {
		proxy, data, replicas = "samplemx", "samplem0", 3
	}
	for _, member := range []string{proxy, data} {
		if err = st.SetAgentToken(t.Context(), &domain.AgentToken{Cluster: current.Name, Member: member, Token: member + "-token"}); err != nil {
			t.Fatal(err)
		}
	}
	pull := func(member, revision string) (int, agentapi.MemberConfig) {
		t.Helper()
		request := httptest.NewRequest("GET", "/internal/agent/v1/config?revision="+url.QueryEscape(revision), nil)
		request.Header.Set("Authorization", "Bearer "+member+"-token")
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, request)
		var cfg agentapi.MemberConfig
		if response.Code == 200 {
			if err := json.Unmarshal(response.Body.Bytes(), &cfg); err != nil {
				t.Fatal(err)
			}
		} else if response.Code != 304 {
			t.Fatalf("full config failed: %d %s", response.Code, response.Body.String())
		}
		return response.Code, cfg
	}
	code, initial := pull(proxy, "")
	if code != 200 || initial.PullRevision == "" {
		t.Fatalf("full real config has no production baseline: %d %+v", code, initial)
	}
	revisions := make([]string, replicas)
	for i := range revisions {
		_, cfg := pull(proxy, "")
		revisions[i] = cfg.PullRevision
		if cfg.PullRevision == "" {
			t.Fatal("missing applied revision")
		}
	}
	publish := func() {
		t.Helper()
		c, e := st.GetCluster(t.Context(), current.Ref())
		if e != nil {
			t.Fatal(e)
		}
		c.Status.ObservedAt = rc.Clock.Now()
		c.Status.ObservedGeneration = c.Generation
		c.Status.ObservationEpoch = rc.ObservationEpoch
		c.Status.Phase = domain.PhaseReady
		if e = st.UpdateClusterState(t.Context(), c); e != nil {
			t.Fatal(e)
		}
	}
	pulls := 0
	networkStart := total()
	rendersBefore := renderCalls.Load()
	configNetwork := func() uint64 {
		var requests uint64
		for _, provider := range infra.PlatformCounters() {
			for _, counter := range provider.Requests {
				if counter.Caller == infra.CallerAgentConfig {
					requests += counter.Reads + counter.Mutations
				}
			}
		}
		return requests
	}
	for second := 1; second <= 3600; second++ {
		if remaining := renewalGridStart.Add(time.Duration(second) * time.Second).Sub(rc.Clock.Now()); remaining > 0 {
			advance(remaining)
		}
		if second%5 == 0 {
			publish()
			listing()
		}
		for i := range revisions {
			// Actual pull phases are independent of the 5s observer and 60s listing.
			if second%60 != 17+i*13 {
				continue
			}
			before := configNetwork()
			code, _ := pull(proxy, revisions[i])
			if code != 304 || configNetwork() != before {
				t.Fatalf("M4 production minute pull caused config traffic: code=%d requests=%d", code, configNetwork()-before)
			}
			pulls++
		}
	}
	if pulls != 60*replicas {
		t.Fatal("missing actual minute pulls", pulls)
	}
	if renderCalls.Load() != rendersBefore {
		t.Fatal("unchanged hour performed full configuration rendering", renderCalls.Load()-rendersBefore)
	}
	t.Logf("M4 production HTTP pulls=%d configuration network requests=0 shared/background requests=%d", pulls, total()-networkStart)
	// These are real MySQL/PG data renderers, not a renderer stub. Storage-only
	// updates must change both the opaque revision and the rendered credentials.
	_, previous := pull(data, "")
	for _, change := range []func(){func() { storage.Credentials.SecretAccessKey = "rotated" }, func() { storage.EndpointURL = "https://replacement.example.test"; storage.Bucket = "changed" }} {
		change()
		if err = st.UpdateBackupStorage(t.Context(), storage); err != nil {
			t.Fatal(err)
		}
		status, next := pull(data, previous.PullRevision)
		if status != 200 || next.PullRevision == previous.PullRevision || string(next.EngineSpec) == string(previous.EngineSpec) {
			t.Fatal("storage-only change was hidden by conditional pull")
		}
		previous = next
	}
	// Reproduce the adverse phase: a scheduled pull is unchanged; immediately
	// afterwards the observation baseline is invalidated, before the next minute.
	publish()
	listing()
	_, applied := pull(proxy, "")
	if withdraw != nil {
		withdraw()
	}
	status, _ := pull(proxy, applied.PullRevision)
	if status != 304 {
		t.Fatal("phase-offset setup did not produce 304")
	}
	// Account for both the 3s wait and up to 4s bounded sample collection.
	advance(7 * time.Second)
	if _, err := authentication.CreateAccount(t.Context(), domain.Account{Name: "config-admin", Roles: []string{"role:admin"}}, "config-test-password"); err != nil {
		t.Fatal(err)
	}
	admin, _, err := authentication.CreateToken(t.Context(), "config-admin", "force-config", nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	forceRequest := httptest.NewRequest("POST", "/api/v1/clusters/"+url.PathEscape(current.Name)+"/reconcile", nil)
	forceRequest.Header.Set("Authorization", "Bearer "+admin)
	forceResponse := httptest.NewRecorder()
	cp.Handler().ServeHTTP(forceResponse, forceRequest)
	if forceResponse.Code != 202 {
		t.Fatalf("forced reconcile request failed: %d %s", forceResponse.Code, forceResponse.Body.String())
	}
	// Wait until the forced pass has already re-entered steady. The notice
	// must survive that transient invalidation even without a target epoch bump.
	waitSteady()
	request := httptest.NewRequest("POST", agentapi.HeartbeatPath, strings.NewReader(fmt.Sprintf(`{"session":"phase-offset","instance":{"hostname":"proxy"},"configRevision":%q}`, applied.ChangeRevision)))
	request.Header.Set("Authorization", "Bearer "+proxy+"-token")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	var notice agentapi.HeartbeatResponse
	if response.Code != 200 || json.Unmarshal(response.Body.Bytes(), &notice) != nil || !notice.ReloadConfig {
		t.Fatalf("baseline invalidation did not notify applied config: %d %s", response.Code, response.Body.String())
	}
	before := configNetwork()
	status, corrected := pull(proxy, applied.PullRevision)
	if status != 200 || p.Name() == "postgres" && configNetwork() <= before {
		t.Fatal("notified pull skipped fresh rendering", status, configNetwork()-before)
	}
	if withdraw != nil {
		var payload struct{ Peers []struct{ Name string } }
		if err := json.Unmarshal(corrected.EngineSpec, &payload); err != nil {
			t.Fatal(err)
		}
		for _, peer := range payload.Peers {
			if peer.Name == data {
				t.Fatal("notified fresh config retained externally withdrawn peer")
			}
		}
	}
	t.Log("forced reconcile invalidation survived steady re-entry; notice and full fresh rendering completed at the next <=7s heartbeat opportunity")
}
