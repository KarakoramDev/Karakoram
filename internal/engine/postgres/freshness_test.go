// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package postgres

import (
	"context"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/postgres/pgspec"
	"github.com/KarakoramDev/Karakoram/internal/infra"
)

func TestMemberVersionUsesOnlyLiveObservation(t *testing.T) {
	p, rc, b := setup(t, "single")
	s := converge(t, p, rc)
	name := s.data()[0].Name
	for _, version := range []string{"17.10.1", ""} {
		st, err := live(t.Context(), rc, name)
		if err != nil {
			t.Fatal(err)
		}
		st.Version = version
		b.statusOverrides = map[string]pgspec.StatusResponse{name: st}
		status, err := p.Status(t.Context(), rc)
		if err != nil {
			t.Fatal(err)
		}
		for _, member := range status.Members {
			if member.Name == name && member.Version != version {
				t.Fatalf("member version=%q, want observed %q (assigned %q)", member.Version, version, rc.Cluster.Spec.Version)
			}
		}
	}
}

func TestVersionDriftIsVisibleAndCannotPrepareDowngrade(t *testing.T) {
	p, rc, b := setup(t, "single")
	rc.Cluster.Spec.Version = "17.10.1"
	s := converge(t, p, rc)
	m := s.data()[0]
	observed, err := live(t.Context(), rc, m.Name)
	if err != nil {
		t.Fatal(err)
	}
	observed.Version = "17.11.1"
	b.statusOverrides = map[string]pgspec.StatusResponse{m.Name: observed}
	status, err := p.Status(t.Context(), rc)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, condition := range status.Conditions {
		if condition.Type == "VersionDrift" && condition.Status == "True" {
			found = strings.Contains(condition.Message, "observed 17.11.1, desired 17.10.1") &&
				strings.Contains(condition.Message, "backup") && strings.Contains(condition.Message, "automatic downgrades are blocked")
		}
	}
	if !found || status.Phase != domain.PhaseDegraded || rc.Cluster.Spec.Version != "17.10.1" {
		t.Fatalf("drift must be visible without rewriting intent: %+v", status)
	}
	rc.Cluster.Status = status
	if _, err := p.runtimeSpec(rc, m); err == nil || !strings.Contains(err.Error(), "automatic downgrade is blocked") {
		t.Fatalf("runtime preparation accepted an observed downgrade: %v", err)
	}
	if _, err := p.agentUpgradeRuntimeSpec(rc, m); err == nil || !strings.Contains(err.Error(), "automatic downgrade is blocked") {
		t.Fatalf("agent-only preparation accepted an observed downgrade: %v", err)
	}
	rc.Cluster.Spec.Version = observed.Version
	if _, err := p.runtimeSpec(rc, m); err != nil {
		t.Fatalf("explicit alignment with the running release is blocked: %v", err)
	}
	status, err = p.Status(t.Context(), rc)
	if err != nil {
		t.Fatal(err)
	}
	for _, condition := range status.Conditions {
		if condition.Type == "VersionDrift" {
			t.Fatal("resolved drift remained reported")
		}
	}
}

func TestMajorUpgradeRequiresExplicitDriftResolution(t *testing.T) {
	p, rc, b := setup(t, "single")
	rc.Cluster.Spec.Version = "17.10.1"
	s := converge(t, p, rc)
	m := s.data()[0]
	observed, err := live(t.Context(), rc, m.Name)
	if err != nil {
		t.Fatal(err)
	}
	observed.Version = "17.11.1"
	b.statusOverrides = map[string]pgspec.StatusResponse{m.Name: observed}
	rc.Cluster.Spec.Backup.Storage = "archive"
	if err := rc.Store.CreateBackup(t.Context(), &domain.Backup{
		Namespace: rc.Cluster.Namespace, Cluster: rc.Cluster.Name, Name: "observed-release",
		Provider: "postgres", Storage: "archive", Type: "full", Phase: domain.PhaseSucceeded,
		SourceVersion: observed.Version, Postgres: &domain.PostgresBackup{Label: "20261027-100000F"},
	}); err != nil {
		t.Fatal(err)
	}
	rc.Cluster.Spec.Version = "18.6.1"
	handled, err := p.reconcileUpgrade(t.Context(), rc, s)
	if !handled || err == nil || !strings.Contains(err.Error(), "before a major upgrade") || !strings.Contains(err.Error(), "explicitly align") || s.Upgrade != nil {
		t.Fatalf("drift was obscured as a missing-backup error or upgrade began: handled=%t err=%v upgrade=%+v", handled, err, s.Upgrade)
	}
	// Simulate the completed, explicit same-major alignment, preserving the
	// actual-release backup rather than relabeling it as the old assignment.
	m.Version = observed.Version
	handled, err = p.reconcileUpgrade(t.Context(), rc, s)
	if !handled || err != nil || s.Upgrade == nil || s.Upgrade.From != observed.Version {
		t.Fatalf("aligned release could not use its actual-release backup: handled=%t err=%v upgrade=%+v", handled, err, s.Upgrade)
	}
}

func TestMajorUpgradeAcceptsAlignedUpstreamReleaseBackup(t *testing.T) {
	p, rc, b := setup(t, "single")
	rc.Cluster.Spec.Version = "16.15.1"
	s := converge(t, p, rc)
	m := s.data()[0]
	st, err := live(t.Context(), rc, m.Name)
	if err != nil {
		t.Fatal(err)
	}
	st.Version = "16.15"
	b.statusOverrides = map[string]pgspec.StatusResponse{m.Name: st}
	b.genericOverrides = map[string]agentapi.Status{m.Name: {ConfigVersion: m.ConfigVersion, EngineVersion: "16.15"}}
	rc.Cluster.Spec.Backup.Storage = "archive"
	if err := rc.Store.CreateBackup(t.Context(), &domain.Backup{
		Namespace: rc.Cluster.Namespace, Cluster: rc.Cluster.Name, Name: "aligned-upstream",
		Provider: "postgres", Storage: "archive", Type: "full", Phase: domain.PhaseSucceeded,
		SourceVersion: "16.15", Postgres: &domain.PostgresBackup{Label: "20261027-120000F"},
	}); err != nil {
		t.Fatal(err)
	}
	rc.Cluster.Spec.Version = "17.11.1"
	handled, err := p.reconcileUpgrade(t.Context(), rc, s)
	if err != nil || !handled || s.Upgrade == nil {
		t.Fatalf("aligned upstream release or truthful backup rejected: handled=%t err=%v", handled, err)
	}
}

func TestLegacyAgentBinaryObservationBlocksDowngrade(t *testing.T) {
	p, rc, b := setup(t, "single")
	rc.Cluster.Spec.Version = "17.10.1"
	s := converge(t, p, rc)
	m := s.data()[0]
	b.genericOverrides = map[string]agentapi.Status{m.Name: {
		ConfigVersion: m.ConfigVersion, EngineVersion: "17.11.1",
		Agent: agentapi.AgentBuild{Build: "old-build"},
	}}
	// The old native endpoint still echoes 17.10.1. Its generic status
	// reports the installed binary and must protect the first agent refresh.
	if _, err := readyDesired(t.Context(), rc, m); err == nil || !strings.Contains(err.Error(), "automatic downgrade is blocked") {
		t.Fatalf("legacy native assignment hid an installed newer server: %v", err)
	}
	b.actions = nil
	op := rolloutOperation(false)
	result, err := p.upgradeAgents(t.Context(), rc, op, false)
	if err != nil {
		t.Fatal(err)
	}
	op.Checkpoint = result.Checkpoint
	if _, err := p.upgradeAgents(t.Context(), rc, op, false); err == nil || !strings.Contains(err.Error(), "automatic downgrade is blocked") {
		t.Fatalf("first legacy agent refresh did not fail closed: %v", err)
	}
	if len(b.actions) != 0 {
		t.Fatalf("legacy drift guard performed infrastructure actions: %v", b.actions)
	}
	rc.Cluster.Spec.Version = "18.6.1"
	if _, err := p.reconcileUpgrade(t.Context(), rc, s); err == nil || !strings.Contains(err.Error(), "binary version drift") {
		t.Fatalf("legacy major-upgrade admission ignored binary drift: %v", err)
	}
	rc.Cluster.Spec.Version = "17.11.1"
	if _, err := readyDesired(t.Context(), rc, m); err != nil {
		t.Fatalf("explicit alignment remains blocked: %v", err)
	}
}

func TestCorrectiveMinorUpgradeWithVersionDrift(t *testing.T) {
	for _, topology := range []string{"single", "ha"} {
		for _, legacy := range []bool{true, false} {
			for _, observed := range []string{"16.15", "16.14.2"} {
				t.Run(topology+"/"+observed+fmt.Sprint("/legacy=", legacy), func(t *testing.T) {
					p, rc, b := setup(t, topology)
					rc.Cluster.Spec.Version = "16.14.1"
					s := converge(t, p, rc)
					b.genericOverrides = map[string]agentapi.Status{}
					b.statusOverrides = map[string]pgspec.StatusResponse{}
					for _, m := range s.data() {
						st, err := live(t.Context(), rc, m.Name)
						if err != nil {
							t.Fatal(err)
						}
						if !legacy {
							st.Version = observed
							b.statusOverrides[m.Name] = st
						}
						b.genericOverrides[m.Name] = agentapi.Status{ConfigVersion: m.ConfigVersion, EngineVersion: observed}
					}
					status, err := p.Status(t.Context(), rc)
					if err != nil {
						t.Fatal(err)
					}
					rc.Cluster.Status = status
					// Before selecting a corrective release, an agent-only
					// deployment must not reinstall the old database pin.
					if _, err := p.agentUpgradeRuntimeSpec(rc, s.data()[0]); err == nil && !legacy {
						t.Fatal("agent rollout accepted the stale pin")
					}
					rc.Cluster.Spec.Version = "16.15.1"
					rc.Cluster.Generation++
					b.actions = nil
					for range 100 {
						for name, st := range b.statusOverrides {
							if topology == "ha" {
								leader := b.leader
								if leader == "" {
									leader = "demop0"
								}
								st.Leader, st.Role = leader, "replica"
								if name == leader {
									st.Role = "primary"
								}
								b.statusOverrides[name] = st
							}
						}
						s, _ = load(rc.Cluster)
						for _, m := range s.data() {
							st := b.genericOverrides[m.Name]
							st.ConfigVersion = m.ConfigVersion
							b.genericOverrides[m.Name] = st
						}
						if _, err := p.Sync(t.Context(), rc); err != nil {
							t.Fatal(err)
						}
						// A completed package deployment supplies the target
						// native release; legacy status echoes its assignment.
						for _, action := range b.actions {
							if strings.HasPrefix(action, "deploy:") {
								delete(b.statusOverrides, strings.TrimPrefix(action, "deploy:"))
							}
						}
						status, err := p.Status(t.Context(), rc)
						if err != nil {
							t.Fatal(err)
						}
						rc.Cluster.Status = status
						if status.Phase == domain.PhaseReady && status.ObservedGeneration == rc.Cluster.Generation {
							break
						}
					}
					if rc.Cluster.Status.Phase != domain.PhaseReady || rc.Cluster.Status.ObservedGeneration != rc.Cluster.Generation {
						t.Fatalf("corrective upgrade did not converge: %+v", rc.Cluster.Status)
					}
					s, _ = load(rc.Cluster)
					for _, m := range s.data() {
						if m.Version != "16.15.1" || !slices.Contains(b.actions, "deploy:"+m.Name) {
							t.Fatalf("member did not roll to corrective pin: %+v actions=%v", m, b.actions)
						}
						if _, err := p.agentUpgradeRuntimeSpec(rc, m); err != nil {
							t.Fatalf("agent rollout after corrective upgrade blocked: %v", err)
						}
					}
					rc.Backend = &rolloutBackend{fakeBackend: b, rc: rc}
					op := rolloutOperation(true)
					done := false
					for range 80 {
						result, err := p.upgradeAgents(t.Context(), rc, op, false)
						if err != nil {
							t.Fatal(err)
						}
						op.Checkpoint = result.Checkpoint
						if result.Done {
							done = true
							break
						}
					}
					if !done {
						t.Fatal("agent rollout after corrective upgrade did not finish")
					}
				})
			}
		}
	}
}

func TestUpgradeAllowedWithPartialObservedVersion(t *testing.T) {
	for _, tc := range []struct {
		from, to string
		allowed  bool
	}{
		{"16.15", "16.15.1", true},
		{"16.14.2", "16.15.1", true},
		{"16.15", "16.14.1", false},
		{"16.15.2", "16.15.1", false},
		{"16.15.1", "16.15.1", false},
		{"17.11", "16.15.1", false},
	} {
		if got := upgradeAllowed(tc.from, tc.to); got != tc.allowed {
			t.Errorf("upgradeAllowed(%q, %q)=%t, want %t", tc.from, tc.to, got, tc.allowed)
		}
	}
}

func TestPartialObservedReleaseResolvesDriftWithoutInventingRevision(t *testing.T) {
	p, rc, b := setup(t, "single")
	rc.Cluster.Spec.Version = "16.14.1"
	s := converge(t, p, rc)
	m := s.data()[0]
	st, err := live(t.Context(), rc, m.Name)
	if err != nil {
		t.Fatal(err)
	}
	st.Version = "16.15"
	b.statusOverrides = map[string]pgspec.StatusResponse{m.Name: st}
	status, err := p.Status(t.Context(), rc)
	if err != nil {
		t.Fatal(err)
	}
	rc.Cluster.Status = status
	rc.Cluster.Spec.Version = "16.15.1"
	converge(t, p, rc)
	for _, condition := range rc.Cluster.Status.Conditions {
		if condition.Type == "VersionDrift" {
			t.Fatalf("upstream-only observation remained drifted: %+v", condition)
		}
	}
	for _, member := range rc.Cluster.Status.Members {
		if member.Name == m.Name && member.Version != "16.15" {
			t.Fatalf("fabricated product revision: %q", member.Version)
		}
	}
	if observedReleaseMatches("16.15.2", "16.15.1") {
		t.Fatal("reported product revision mismatch was hidden")
	}
}

func TestExpectedVersionRolloutsDoNotReportDrift(t *testing.T) {
	for _, tc := range []struct {
		name, source, target string
		checkpoint, major    bool
	}{
		{"minor source before deployment", "17.10.1", "17.11.1", false, false},
		{"minor source after deployment checkpoint", "17.10.1", "17.11.1", true, false},
		{"major source during admitted upgrade", "16.15.1", "17.11.1", true, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p, rc, b := setup(t, "ha")
			rc.Cluster.Spec.Version = tc.source
			converge(t, p, rc)
			rc.Cluster.Spec.Version = tc.target
			rc.Cluster.Generation++
			s, err := load(rc.Cluster)
			if err != nil {
				t.Fatal(err)
			}
			b.statusOverrides = map[string]pgspec.StatusResponse{}
			for _, m := range s.data() {
				st, err := live(t.Context(), rc, m.Name)
				if err != nil {
					t.Fatal(err)
				}
				st.Version = tc.source
				b.statusOverrides[m.Name] = st
				if tc.checkpoint {
					m.Version = tc.target
					if !tc.major {
						m.PreviousVersion = tc.source
					}
				}
			}
			if tc.major {
				s.Upgrade = &upgradeState{From: tc.source, To: tc.target, Stage: "prepare"}
			}
			if err := save(rc.Cluster, s); err != nil {
				t.Fatal(err)
			}
			status, err := p.Status(t.Context(), rc)
			if err != nil {
				t.Fatal(err)
			}
			if status.Phase != domain.PhaseUpdating {
				t.Fatalf("expected rollout phase=%s, want Updating", status.Phase)
			}
			for _, condition := range status.Conditions {
				if condition.Type == "VersionDrift" {
					t.Fatalf("source release during rollout was classified as drift: %+v", condition)
				}
			}
			// A different release is not excused by an upgrade checkpoint.
			first := s.data()[0].Name
			st := b.statusOverrides[first]
			st.Version = "18.6.1"
			b.statusOverrides[first] = st
			status, err = p.Status(t.Context(), rc)
			if err != nil || status.Phase != domain.PhaseDegraded {
				t.Fatalf("unrelated rollout mismatch was hidden: phase=%s err=%v", status.Phase, err)
			}
		})
	}
}

func TestKnownVersionDriftSurvivesMissingObservations(t *testing.T) {
	p, rc, b := setup(t, "single")
	rc.Cluster.Spec.Version = "17.10.1"
	s := converge(t, p, rc)
	name := s.data()[0].Name
	base, err := live(t.Context(), rc, name)
	if err != nil {
		t.Fatal(err)
	}
	setVersion := func(version string) domain.ClusterStatus {
		t.Helper()
		st := base
		st.Version = version
		b.statusOverrides = map[string]pgspec.StatusResponse{name: st}
		status, err := p.Status(t.Context(), rc)
		if err != nil {
			t.Fatal(err)
		}
		rc.Cluster.Status = status
		return status
	}
	assertDrift := func(status domain.ClusterStatus, want string) {
		t.Helper()
		if status.Phase != domain.PhaseDegraded {
			t.Fatalf("unresolved drift phase=%s, want Degraded", status.Phase)
		}
		for _, condition := range status.Conditions {
			if condition.Type == "VersionDrift" && condition.Status == want {
				return
			}
		}
		t.Fatalf("missing VersionDrift=%s: %+v", want, status.Conditions)
	}
	// No version observation must not invent a mismatch on a never-drifted member.
	unknown := setVersion("")
	for _, condition := range unknown.Conditions {
		if condition.Type == "VersionDrift" {
			t.Fatal("unknown initial observation manufactured drift")
		}
	}
	assertDrift(setVersion("17.11.1"), "True")
	// Reconstruct the provider and discard public status to prove evidence is
	// retained in serialized provider state, not only a previous condition.
	p = New(p.options)
	rc.Cluster.Status = domain.ClusterStatus{}
	for range 2 {
		status := setVersion("")
		assertDrift(status, "Unknown")
		for _, member := range status.Members {
			if member.Name == name && member.Version != "" {
				t.Fatal("last-known drift was substituted for a fresh member version")
			}
		}
	}
	stored, err := load(rc.Cluster)
	if err != nil || stored.find(name).VersionDrift != "17.11.1" {
		t.Fatalf("confirmed mismatch was not persisted: %v", err)
	}
	if _, err := p.runtimeSpec(rc, stored.find(name)); err == nil {
		t.Fatal("missing fresh version allowed a previously observed downgrade")
	}
	matching := setVersion("17.10.1")
	for _, condition := range matching.Conditions {
		if condition.Type == "VersionDrift" {
			t.Fatal("fresh matching SQL observation did not clear drift")
		}
	}
	if matching.Phase != domain.PhaseReady {
		t.Fatalf("matching observation did not recover readiness: %s", matching.Phase)
	}
	stored, err = load(rc.Cluster)
	if err != nil || stored.find(name).VersionDrift != "" {
		t.Fatalf("resolved drift remained persisted: %v", err)
	}
}

func TestFreshnessHungFormerPrimaryDoesNotHideNewWriter(t *testing.T) {
	p, rc, _ := setup(t, "ha")
	s := converge(t, p, rc)
	statuses := make(map[string]pgspec.StatusResponse, len(s.Members))
	for _, m := range s.Members {
		st, err := live(t.Context(), rc, m.Name)
		if err != nil {
			t.Fatal(err)
		}
		st.Role = "replica"
		st.Streaming = true
		st.Leader = "demop1"
		if m.Name == "demop1" {
			st.Role = "primary"
		}
		if m.Role == "proxy" {
			st.Role = "proxy"
		}
		statuses[m.Name] = st
	}
	var firstPeerProbe atomic.Int64
	start := time.Now()
	releaseHungRequests := make(chan struct{})
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		name := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
		if name == "demop0" {
			// A POST whose body is deliberately unread need not observe its
			// peer's close until another read. Explicit teardown releases it.
			select {
			case <-r.Context().Done():
			case <-releaseHungRequests:
			}
			return
		}
		firstPeerProbe.CompareAndSwap(0, time.Since(start).Nanoseconds())
		switch r.URL.Path {
		case "/v1/status":
			json.NewEncoder(w).Encode(agentapi.Status{ConfigVersion: s.ConfigVersion})
		case "/v1/postgres/status":
			json.NewEncoder(w).Encode(statuses[name])
		default:
			t.Errorf("unexpected mutating call %s", r.URL.Path)
			http.NotFound(w, r)
		}
	}))
	defer func() {
		close(releaseHungRequests)
		server.Close()
	}()
	rc.Agents = fakeAgents{url: server.URL, ca: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw})}
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	defer cancel()
	// Exercise the real order: Sync must finish before the reconciler asks for
	// Status. A client's 180-second engine timeout must never be the bound.
	if _, err := p.Sync(ctx, rc); err != nil {
		t.Fatal(err)
	}
	status, err := p.Status(ctx, rc)
	if err != nil {
		t.Fatal(err)
	}
	if elapsed := time.Since(start); elapsed > 5*engine.ObservationTimeout {
		t.Fatalf("dead member delayed publication for %s", elapsed)
	}
	if elapsed := time.Duration(firstPeerProbe.Load()); elapsed == 0 || elapsed >= 2*engine.ObservationTimeout {
		t.Fatalf("healthy peer waited behind the dead member: %s", elapsed)
	}
	found := false
	for _, m := range status.Members {
		found = found || m.Name == "demop1" && m.Role == "primary" && m.Health
	}
	if !found || status.Phase == domain.PhaseReady {
		t.Fatalf("new writer was not published with unhealthy former primary: %+v", status)
	}
}

func TestProxyBackendCapacityBudget(t *testing.T) {
	p, rc, _ := setup(t, "ha")
	s := converge(t, p, rc)
	proxy := s.find("demopx")
	for _, tc := range []struct {
		name                   string
		old, next              string
		current, desired, want int
	}{
		{"default", "", "", 1, 1, 43},
		{"two-proxies", "", "", 2, 2, 20},
		{"scale-up", "", "", 1, 2, 27},
		{"scale-down", "", "", 2, 1, 27},
		{"increase", "100", "200", 1, 1, 43},
		{"decrease", "200", "100", 1, 1, 43},
		{"larger", "200", "200", 1, 1, 93},
	} {
		t.Run(tc.name, func(t *testing.T) {
			raw := func(value string) json.RawMessage {
				if value == "" {
					return nil
				}
				return parameterOptions(map[string]string{"max_connections": value})
			}
			rc.Cluster.Spec.EngineOptions = raw(tc.next)
			for _, m := range s.data() {
				m.Options = raw(tc.old)
			}
			proxy.Replicas = tc.current
			component := rc.Cluster.Spec.Components.Extra["proxy"]
			component.Replicas = tc.desired
			rc.Cluster.Spec.Components.Extra["proxy"] = component
			if err := save(rc.Cluster, s); err != nil {
				t.Fatal(err)
			}
			config, err := p.MemberConfig(t.Context(), rc, proxy.Name)
			if err != nil {
				t.Fatal(err)
			}
			var spec pgspec.Spec
			if err := json.Unmarshal(config.EngineSpec, &spec); err != nil {
				t.Fatal(err)
			}
			if spec.Proxy.MaxConnections != tc.want {
				t.Fatalf("per-instance backend pool=%d want=%d", spec.Proxy.MaxConnections, tc.want)
			}
		})
	}
}

func TestHealthProxyWithoutOnlineWriterIsDegraded(t *testing.T) {
	p, rc, b := setup(t, "ha")
	converge(t, p, rc)
	b.statusOverrides = map[string]pgspec.StatusResponse{
		"demopx": {Role: "proxy", Running: false, ProxyBackends: []pgspec.ProxyBackend{{Host: "demop0", Hostgroup: 10, Status: "SHUNNED", Diagnostics: &pgspec.ProxyDiagnostics{Ping: &pgspec.ProxyMonitorError{Class: "timeout", TimeUnixMicro: 1792300090000000}, ConnectionErrors: 17, PoolObserved: true}}}},
	}
	status, err := p.Status(t.Context(), rc)
	if err != nil {
		t.Fatal(err)
	}
	if status.Phase != domain.PhaseDegraded {
		t.Fatalf("SHUNNED writer hidden by healthy data members: phase=%s", status.Phase)
	}
	if condition := healthCondition(status, "ProxyWriter"); condition.Status != "False" || condition.Reason != "WriterRouteUnavailable" {
		t.Fatalf("writer-route condition missing: %+v", condition)
	}
	message := healthCondition(status, "ProxyWriter").Message
	if !strings.Contains(message, "SHUNNED") || !strings.Contains(message, "ConnERR=17") || !strings.Contains(message, "ping=timeout@") {
		t.Fatalf("native writer failure attribution missing: %s", message)
	}
	for _, m := range status.Members {
		if m.Set == "proxy" && m.Health {
			t.Fatal("proxy reported healthy without an ONLINE writer")
		}
	}
}

func TestProxyCapacityDecreaseWaitsForEveryProxyAck(t *testing.T) {
	p, rc, b, _, first, late := replicatedProxyFixture(t)
	rc.Cluster.Spec.EngineOptions = parameterOptions(map[string]string{"max_connections": "50"})
	b.actions = nil
	for range 2 {
		if _, err := p.Sync(t.Context(), rc); err != nil {
			t.Fatal(err)
		}
	}
	s, err := load(rc.Cluster)
	if err != nil || s.Rolling == nil || s.Rolling.Version == 0 {
		t.Fatal("capacity decrease did not publish its phase", err)
	}
	first.applied(s.Rolling.Version)
	for range 5 {
		if _, err := p.Sync(t.Context(), rc); err != nil {
			t.Fatal(err)
		}
	}
	for _, action := range b.actions {
		if strings.HasPrefix(action, "/v1/postgres/restart:") {
			t.Fatal("PostgreSQL shrank before proxy pool acknowledgement", b.actions)
		}
	}
	late.mu.Lock()
	reloads := late.reloads
	late.mu.Unlock()
	if reloads == 0 {
		t.Fatal("smaller budget was not requested from the lagging proxy")
	}
	late.applied(s.Rolling.Version)
	for range 5 {
		if _, err := p.Sync(t.Context(), rc); err != nil {
			t.Fatal(err)
		}
	}
	if !strings.Contains(strings.Join(b.actions, "\n"), "/v1/postgres/restart:demop0") {
		t.Fatal("acknowledged smaller pool did not release the writer restart", b.actions)
	}
}

func TestProxyAgentBuildInventory(t *testing.T) {
	current := engine.CurrentAgentBuild()
	old := agentapi.AgentBuild{Version: "previous", Commit: "old-commit", Build: "old-build"}
	latest := agentapi.AgentBuild{Version: current.Version, Commit: current.Commit, Build: current.Build}
	for _, tc := range []struct {
		name string
		a, b agentapi.AgentBuild
		want agentapi.AgentBuild
	}{
		{"current", latest, latest, latest},
		{"old-first", old, latest, old},
		{"old-last", latest, old, old},
		{"legacy-first", agentapi.AgentBuild{}, latest, agentapi.AgentBuild{}},
		{"legacy-last", latest, agentapi.AgentBuild{}, agentapi.AgentBuild{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p, rc, _, _, a, b := replicatedProxyFixture(t)
			a.mu.Lock()
			a.agent = tc.a
			a.mu.Unlock()
			b.mu.Lock()
			b.agent = tc.b
			b.mu.Unlock()
			status, err := p.Status(t.Context(), rc)
			if err != nil {
				t.Fatal(err)
			}
			for _, member := range status.Members {
				if member.Name != "demopx" {
					continue
				}
				if !member.Agent.Healthy || member.Agent.Version != tc.want.Version ||
					member.Agent.Commit != tc.want.Commit || member.Agent.Build != tc.want.Build {
					t.Fatalf("proxy build identity lost or masked: %+v, want %+v", member.Agent, tc.want)
				}
				engine.ObserveAgentVersions(&status, current, time.Now())
				if tc.want.Build == "" && !slices.Contains(status.Agents.Unknown, member.Name) {
					t.Fatal("legacy proxy was not classified unknown")
				}
				if tc.want.Build != "" && tc.want.Build != current.Build && !slices.Contains(status.Agents.Outdated, member.Name) {
					t.Fatal("older proxy instance was not classified outdated")
				}
				return
			}
			t.Fatal("proxy member missing")
		})
	}
}

type proxyPlacementStatusBackend struct {
	*proxyInventoryBackend
}

func (b proxyPlacementStatusBackend) PlacementFailures(context.Context, []string) ([]infra.PlacementFailure, error) {
	return []infra.PlacementFailure{{Member: "demopx", Message: `placement blocked: dimension "cpu" exhausted; reserved port collision`}}, nil
}

func TestProxyUnplacedInstanceDoesNotMislabelServingWriterRoutes(t *testing.T) {
	p, rc, b, s, _, late := replicatedProxyFixture(t)
	component := rc.Cluster.Spec.Components.Extra["proxy"]
	component.Replicas = 3
	rc.Cluster.Spec.Components.Extra["proxy"] = component
	s.find("demopx").Replicas = 3
	if err := save(rc.Cluster, s); err != nil {
		t.Fatal(err)
	}
	b.services["demopx"].Status = infra.StatusCreating
	rc.Backend = proxyPlacementStatusBackend{b}
	status, err := p.Status(t.Context(), rc)
	if err != nil {
		t.Fatal(err)
	}
	ready := healthCondition(status, "Ready")
	if ready.Status != "False" || ready.Reason != "ProxyInstanceUnplaced" || status.Phase != domain.PhaseDegraded {
		t.Fatalf("unplaced proxy was not attributed to placement: %+v", status)
	}
	if !strings.Contains(ready.Message, "2 of 3") || !strings.Contains(ready.Message, "cpu") || !strings.Contains(ready.Message, "reserved port collision") {
		t.Fatalf("placement reason missing: %+v", ready)
	}
	if condition := healthCondition(status, "ProxyWriter"); condition.Status == "False" {
		t.Fatalf("healthy current-primary writer routes mislabeled: %+v", condition)
	}
	// A real serving-proxy failure still takes priority over placement and
	// continues to enforce the every-instance current-primary writer rule.
	late.mu.Lock()
	late.backends = []pgspec.ProxyBackend{{Host: "demop0", Hostgroup: 10, Status: "SHUNNED"}}
	late.mu.Unlock()
	status, err = p.Status(t.Context(), rc)
	if err != nil {
		t.Fatal(err)
	}
	if ready := healthCondition(status, "Ready"); ready.Status != "False" || ready.Reason != "ProxyUnavailable" {
		t.Fatalf("serving writer failure hidden by placement: %+v", ready)
	}
	if writer := healthCondition(status, "ProxyWriter"); writer.Status != "False" || writer.Reason != "WriterRouteUnavailable" {
		t.Fatalf("serving writer rule was weakened: %+v", writer)
	}
}

type failedProxyScaleBackend struct {
	*fakeBackend
	canRecover     bool
	submitted      int
	ordinaryScales int
}

func (b *failedProxyScaleBackend) RecoverFailedScale(_ context.Context, name string, min, max int) (infra.Op, bool, error) {
	if !b.canRecover {
		return infra.Op{}, false, nil
	}
	b.submitted++
	b.services[name].Status = infra.StatusCreating
	return infra.Op{ID: "recovered-count", Kind: "scale", Hostname: name}, true, nil
}

func (b *failedProxyScaleBackend) SetContainerCount(ctx context.Context, name string, min, max int) (infra.Op, error) {
	b.ordinaryScales++
	return b.fakeBackend.SetContainerCount(ctx, name, min, max)
}

func (b *failedProxyScaleBackend) OpStatus(ctx context.Context, op infra.Op) (infra.OpState, error) {
	switch op.ID {
	case "failed-count":
		return infra.OpState{Failed: true, Status: "failed", Message: "native deployment failed"}, nil
	case "recovered-count":
		b.services[op.Hostname].Status = infra.StatusRunning
		return infra.OpState{Done: true, Status: "done"}, nil
	default:
		return b.fakeBackend.OpStatus(ctx, op)
	}
}

func TestProxyTerminalScaleFailureRecoversOnlyWithPlacementFreeProof(t *testing.T) {
	for _, canRecover := range []bool{false, true} {
		name := "unplaceable"
		if canRecover {
			name = "already-healthy"
		}
		t.Run(name, func(t *testing.T) {
			p, rc, base := setup(t, "ha")
			component := rc.Cluster.Spec.Components.Extra["proxy"]
			component.Replicas = 2
			rc.Cluster.Spec.Components.Extra["proxy"] = component
			s := converge(t, p, rc)
			rc.Cluster.Generation++
			b := &failedProxyScaleBackend{fakeBackend: base, canRecover: canRecover}
			rc.Backend = b
			m := s.find("demopx")
			m.Replicas = 1
			s.Pending = &pending{Op: infra.Op{ID: "failed-count", Kind: "scale"}, Action: "scale", Member: m.Name, Replicas: 2}
			b.services[m.Name].Status = infra.StatusFailed
			b.services[m.Name].MinContainers, b.services[m.Name].MaxContainers = 2, 2
			if err := save(rc.Cluster, s); err != nil {
				t.Fatal(err)
			}
			status, err := p.Status(t.Context(), rc)
			if err != nil {
				t.Fatal(err)
			}
			if ready := healthCondition(status, "Ready"); ready.Status != "False" || ready.Reason != "ProxyRolloutFailed" {
				t.Fatalf("terminal failure described as pending: %+v", ready)
			}
			if writer := healthCondition(status, "ProxyWriter"); writer.Status == "False" {
				t.Fatalf("terminal infrastructure failure mislabeled healthy serving routes: %+v", writer)
			}
			if _, err := p.Sync(t.Context(), rc); err == nil {
				t.Fatal("terminal old handle did not surface failure")
			}
			if !canRecover {
				for range 4 {
					if _, err := p.Sync(t.Context(), rc); err == nil || !strings.Contains(err.Error(), "infrastructure runtime failed") {
						t.Fatalf("failed runtime became a successful endless wait: %v", err)
					}
				}
				if b.submitted != 0 || b.ordinaryScales != 0 {
					t.Fatalf("unplaceable failure resubmitted operations: recovery=%d ordinary=%d", b.submitted, b.ordinaryScales)
				}
				return
			}
			for range 10 {
				if _, err := p.Sync(t.Context(), rc); err != nil {
					t.Fatal(err)
				}
				status, err = p.Status(t.Context(), rc)
				if err != nil {
					t.Fatal(err)
				}
				rc.Cluster.Status = status
				if status.Phase == domain.PhaseReady && status.ObservedGeneration == rc.Cluster.Generation {
					break
				}
			}
			if b.submitted != 1 || b.ordinaryScales != 0 || status.Phase != domain.PhaseReady || status.ObservedGeneration != rc.Cluster.Generation {
				t.Fatalf("new bounded scale operation did not converge: submitted=%d status=%+v", b.submitted, status)
			}
		})
	}
}

func TestProxyTerminalFailureSurvivesUnavailableObservations(t *testing.T) {
	for _, observation := range []string{"not-running", "invalid-observation"} {
		for _, corrective := range []bool{false, true} {
			name := observation + "/unchanged"
			if corrective {
				name = observation + "/corrective-resources"
			}
			t.Run(name, func(t *testing.T) {
				p, rc, b := setup(t, "ha")
				converge(t, p, rc)
				b.services["demopx"].Status = infra.StatusFailed
				if observation == "not-running" {
					b.statusOverrides = map[string]pgspec.StatusResponse{"demopx": {Role: "proxy", Running: false}}
				} else {
					b.statusWireOverrides = map[string]json.RawMessage{"demopx": json.RawMessage(`{"running":"invalid"}`)}
				}
				desired := rc.Cluster.Spec.Components.Extra["proxy"]
				if corrective {
					desired.Resources.MinRAM++
					desired.Resources.MaxRAM++
					rc.Cluster.Spec.Components.Extra["proxy"] = desired
				}
				for range 8 {
					_, err := p.Sync(t.Context(), rc)
					if !corrective {
						if err == nil || !strings.Contains(err.Error(), "infrastructure runtime failed") {
							t.Fatalf("unavailable observation swallowed terminal failure: %v", err)
						}
						continue
					}
					if err != nil {
						t.Fatalf("terminal failure blocked corrective resources: %v", err)
					}
					if b.services["demopx"].Resources == desired.Resources {
						return
					}
				}
				if corrective {
					t.Fatal("corrective resize was not submitted")
				}
			})
		}
	}
}

func TestFailedProxyScaleAppliesResourceCorrectionBeforeRetryingCount(t *testing.T) {
	p, rc, base := setup(t, "ha")
	desired := rc.Cluster.Spec.Components.Extra["proxy"]
	desired.Replicas = 2
	rc.Cluster.Spec.Components.Extra["proxy"] = desired
	s := converge(t, p, rc)
	m := s.find("demopx")
	desired.Replicas = 3
	rc.Cluster.Spec.Components.Extra["proxy"] = desired
	base.services[m.Name].MinContainers, base.services[m.Name].MaxContainers = 3, 3
	base.services[m.Name].Status = infra.StatusFailed
	s.Pending = &pending{Op: infra.Op{ID: "failed-count", Kind: "scale"}, Action: "scale", Member: m.Name, Replicas: 3}
	if err := save(rc.Cluster, s); err != nil {
		t.Fatal(err)
	}
	backend := &failedProxyScaleBackend{fakeBackend: base}
	rc.Backend = backend
	if _, err := p.Sync(t.Context(), rc); err == nil {
		t.Fatal("failed scale was not observed")
	}
	desired.Resources.MinRAM /= 2
	desired.Resources.MaxRAM /= 2
	rc.Cluster.Spec.Components.Extra["proxy"] = desired
	for range 20 {
		if _, err := p.Sync(t.Context(), rc); err != nil {
			t.Fatalf("resource correction was blocked: %v", err)
		}
		if reflect.DeepEqual(base.services[m.Name].Resources, desired.Resources) {
			break
		}
	}
	s, err := load(rc.Cluster)
	if err != nil {
		t.Fatal(err)
	}
	if backend.ordinaryScales != 0 || backend.submitted != 0 || s.Pending == nil || s.Pending.Action != "resize" ||
		!reflect.DeepEqual(s.Pending.Resources, desired.Resources) {
		t.Fatalf("count retry preceded resource correction: scales=%d recovery=%d pending=%+v", backend.ordinaryScales, backend.submitted, s.Pending)
	}
}
