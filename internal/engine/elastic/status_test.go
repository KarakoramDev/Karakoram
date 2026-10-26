// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package elastic

import (
	"context"
	"encoding/json"
	"encoding/pem"
	"errors"
	"io"
	"net"
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
	"github.com/KarakoramDev/Karakoram/internal/engine/elastic/agentspec"
)

// Cover both connection establishment and a request sent over established TLS.
func TestPausedNativeEndpointKeepsOtherMembersConnected(t *testing.T) {
	for _, stage := range []string{"tls", "response"} {
		t.Run(stage, func(t *testing.T) { testPausedNativeEndpoint(t, stage) })
	}
}

func testPausedNativeEndpoint(t *testing.T, stage string) {
	p, rc, f, es := coreSetup(t, 3)
	coreConverge(t, p, rc, f)
	es.master = "demon2"
	original := rc.Dialer
	var calls atomic.Int32
	if stage == "tls" {
		rc.Dialer = nativeHangDialer{next: original, calls: &calls}
	} else {
		es.hangingMembers = map[string]bool{"demon0": true}
	}
	status, err := engine.ObserveStatus(t.Context(), p, rc)
	if err != nil || status.Phase != domain.PhaseDegraded {
		t.Fatalf("paused endpoint status: %+v %v", status, err)
	}
	if stage == "tls" && calls.Load() == 0 {
		t.Fatal("hung native endpoint was not exercised")
	}
	for _, member := range status.Members {
		if !member.Agent.Healthy || member.Agent.LastSeen.IsZero() {
			t.Fatalf("native stall revoked agent response: %+v", member)
		}
		if member.Name == "demon0" {
			if member.Health || member.State != "unknown" || !strings.Contains(member.Agent.Message, "paused/stopped") {
				t.Fatalf("missing affected member diagnostic: %+v", member)
			}
		} else if !member.Health || member.State == "absent" || member.Agent.Message != "" {
			t.Fatalf("healthy peer lost native evidence: %+v", member)
		}
	}
	found := false
	for _, condition := range status.Conditions {
		if condition.Type == "ClusterHealth" && condition.Status != "True" {
			t.Fatalf("successful cluster evidence withdrawn: %+v", condition)
		}
		if condition.Type == "NativeObservation/demon0" {
			found = condition.Status == "Unknown"
		}
	}
	if !found {
		t.Fatal("missing per-member native uncertainty condition")
	}
}

func TestPausedMasterMembershipCannotBlockObservationCadence(t *testing.T) {
	p, rc, f, es := coreSetup(t, 3)
	coreConverge(t, p, rc, f)
	es.master = "demon2"
	es.hangingMembers = map[string]bool{"demon2": true}
	ctx, cancel := context.WithTimeout(t.Context(), 4*time.Second)
	defer cancel()
	started := time.Now()
	_, err := p.Sync(ctx, rc)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("paused membership must preserve deadline error: %v", err)
	}
	if elapsed := time.Since(started); elapsed >= 3*time.Second {
		t.Fatalf("membership fan-out blocked observation persistence for %s", elapsed)
	}
	for _, diagnostic := range []string{"elasticsearch convergence-membership read", "2s budget", "inspect member responsiveness/connectivity"} {
		if !strings.Contains(err.Error(), diagnostic) {
			t.Fatalf("membership timeout missing diagnostic %q: %v", diagnostic, err)
		}
	}
}

func TestMasterWideNativeOutageKeepsSuccessfulAgentsConnected(t *testing.T) {
	for _, mode := range []string{"failure", "hang"} {
		t.Run(mode, func(t *testing.T) {
			p, rc, f, es := coreSetup(t, 3)
			coreConverge(t, p, rc, f)
			es.master = "demon2"
			if mode == "hang" {
				es.hangingMembers = map[string]bool{"demon0": true, "demon1": true, "demon2": true}
			} else {
				es.observationFailures = map[string]bool{"/_cluster/health": true}
			}
			status, err := engine.ObserveStatus(t.Context(), p, rc)
			if err != nil || status.Phase != domain.PhaseDegraded || status.ObservedAt.IsZero() {
				t.Fatalf("native-only outage failed sweep: %+v %v", status, err)
			}
			for _, member := range status.Members {
				if !member.Agent.Healthy || member.Agent.LastSeen.Before(status.ObservedAt) || member.Health {
					t.Fatalf("native-only outage revoked successful agent response: %+v", member)
				}
				if !strings.Contains(member.Agent.Message, "paused/stopped") {
					t.Fatalf("missing actionable native diagnostic: %+v", member)
				}
				if !slices.ContainsFunc(status.Conditions, func(c domain.Condition) bool {
					return c.Type == "NativeObservation/"+member.Name && c.Status == "Unknown"
				}) {
					t.Fatalf("missing native failure for %s: %+v", member.Name, status.Conditions)
				}
			}
			if !slices.ContainsFunc(status.Conditions, func(c domain.Condition) bool {
				return c.Type == "ClusterHealth" && c.Status == "Unknown"
			}) {
				t.Fatalf("native outage did not withhold cluster health: %+v", status.Conditions)
			}
		})
	}
}

func TestHungComponentNativeProbeKeepsESPeersConnected(t *testing.T) {
	p, rc, f, _ := coreSetup(t, 3)
	component := rc.Cluster.Spec.Components.Extra[kindKibana]
	component.Replicas = 1
	rc.Cluster.Spec.Components.Extra[kindKibana] = component
	coreConverge(t, p, rc, f)
	var nativeCalls atomic.Int32
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/status":
			_ = json.NewEncoder(w).Encode(agentapi.Status{ConfigVersion: 1 << 30, Processes: map[string]agentapi.ProcessStatus{"elasticsearch": {State: "running"}, "kibana": {State: "running"}}})
		case "/v1/elasticsearch/readiness":
			_, _ = io.Copy(io.Discard, r.Body)
			_ = r.Body.Close()
			nativeCalls.Add(1)
			<-r.Context().Done()
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	rc.Agents = coreAgentDialer{url: server.URL, ca: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw})}
	status, err := engine.ObserveStatus(t.Context(), p, rc)
	if err != nil || status.Phase != domain.PhaseDegraded || nativeCalls.Load() == 0 {
		t.Fatalf("hung component status: %+v %v calls=%d", status, err, nativeCalls.Load())
	}
	for _, member := range status.Members {
		if !member.Agent.Healthy {
			t.Fatalf("component native stall revoked agent response: %+v", member)
		}
		if member.Name != "demokb" && (!member.Health || member.State == "absent") {
			t.Fatalf("healthy ES peer lost evidence: %+v", member)
		}
		if member.Name == "demokb" && member.Health {
			t.Fatalf("unobserved component reported healthy: %+v", member)
		}
	}
}

type nativeHangDialer struct {
	next interface {
		DialContext(context.Context, string, string) (net.Conn, error)
	}
	calls *atomic.Int32
}

func (d nativeHangDialer) DialContext(ctx context.Context, network, address string) (net.Conn, error) {
	if address != "demon0:9200" {
		return d.next.DialContext(ctx, network, address)
	}
	d.calls.Add(1)
	client, server := net.Pipe()
	go func() {
		defer server.Close()
		_, _ = io.Copy(io.Discard, server)
	}()
	return client, nil
}

func TestNativeObservationFailureWithdrawsReadiness(t *testing.T) {
	for _, health := range []string{"green", "yellow"} {
		for _, endpoint := range []string{"/_nodes", "/_cat/master", "/_cluster/health"} {
			t.Run(health+endpoint, func(t *testing.T) {
				p, rc, f, es := coreSetup(t, 1)
				coreConverge(t, p, rc, f)
				es.health = health
				if health == "yellow" {
					es.replicas = 1 // One node cannot place the replica beside its primary.
				}
				before, err := p.Status(t.Context(), rc)
				if err != nil || before.Phase != domain.PhaseReady {
					t.Fatalf("baseline: %+v %v", before, err)
				}
				es.observationFailures = map[string]bool{endpoint: true}
				status, err := p.Status(t.Context(), rc)
				if err != nil || status.Phase == domain.PhaseReady || !strings.Contains(status.Message, "restore observations") {
					t.Fatalf("silent native failure: %+v %v", status, err)
				}
				for _, member := range status.Members {
					if member.Health || !member.Agent.Healthy || !strings.Contains(member.Agent.Message, "observation unavailable") {
						t.Fatalf("native failure hidden behind healthy agent: %+v", member)
					}
				}
				if !slices.ContainsFunc(status.Conditions, func(c domain.Condition) bool {
					return c.Type == "ClusterHealth" && c.Status == "Unknown" && c.Reason == "ObservationUnavailable" && c.Message != ""
				}) {
					t.Fatalf("native uncertainty not projected: %+v", status.Conditions)
				}
				es.observationFailures = nil
				recovered, err := p.Status(t.Context(), rc)
				if err != nil || recovered.Phase != domain.PhaseReady || strings.Contains(recovered.Message, "observation unavailable") {
					t.Fatalf("native recovery: %+v %v", recovered, err)
				}
			})
		}
	}
}

func TestBlockedScaleDownRetainsElectedMasterAndWarnsOnce(t *testing.T) {
	p, rc, f, es := coreSetup(t, 3)
	coreConverge(t, p, rc, f)
	es.master, es.replicas = "demon2", 2
	rc.Cluster.Spec.Components.Engine.Replicas = 1
	events := rc.Events.(*coreEvents)
	events.events = nil
	for range 6 {
		if _, err := p.Sync(context.Background(), rc); err != nil {
			t.Fatal(err)
		}
	}
	status, err := p.Status(context.Background(), rc)
	if err != nil {
		t.Fatal(err)
	}
	if len(status.Members) != 3 {
		t.Fatalf("draining native members hidden: %+v", status.Members)
	}
	master := status.Members[slices.IndexFunc(status.Members, func(m domain.MemberStatus) bool { return m.Name == es.master })]
	if master.State != "draining" || master.Health || f.services[master.Name] == nil {
		t.Fatalf("draining elected master reported healthy or removed: %+v", master)
	}
	for _, text := range []string{es.master, "insufficient data nodes", "index replicas", "Restore or raise the desired data-node count", "Never force removal"} {
		if !strings.Contains(status.Message, text) {
			t.Fatalf("missing actionable drain message %q: %s", text, status.Message)
		}
	}
	if len(events.events) != 1 || events.events[0].Type != "Warning" || events.events[0].Reason != "ShardDrainBlocked" || events.events[0].Message != status.Message {
		t.Fatalf("expected one blocked transition event: %+v", events.events)
	}
	s, _ := load(rc.Cluster)
	m := s.find(es.master)
	// A different unsafe native state is a new warning transition, not a sync heartbeat.
	m.Stage, es.health = stageDraining, "red"
	for range 2 {
		if err := p.removeStep(context.Background(), rc, s, m); err != nil {
			t.Fatal(err)
		}
	}
	if len(events.events) != 2 || !strings.Contains(events.events[1].Message, "unassigned primary shards") {
		t.Fatalf("missing changed drain reason: %+v", events.events)
	}
}

func TestSuspendedStatusDoesNotReusePreviouslyHealthyRuntime(t *testing.T) {
	p, rc, f, _ := coreSetup(t, 1)
	kb := rc.Cluster.Spec.Components.Extra[kindKibana]
	kb.Replicas = 1
	rc.Cluster.Spec.Components.Extra[kindKibana] = kb
	s := coreConverge(t, p, rc, f)
	before, err := p.Status(context.Background(), rc)
	if err != nil || !before.Members[0].Health || !before.Members[0].Agent.Healthy {
		t.Fatalf("fixture did not start healthy: %+v %v", before, err)
	}
	s.Suspended = true
	if err := save(rc.Cluster, s); err != nil {
		t.Fatal(err)
	}
	// These dependencies cannot supply new live observations after suspension.
	rc.Agents, rc.Dialer = nil, nil
	status, err := p.Status(context.Background(), rc)
	if err != nil || status.Phase != domain.PhaseSuspended || len(status.Members) != 2 {
		t.Fatalf("suspended inventory: %+v %v", status, err)
	}
	for _, member := range status.Members {
		if member.State != "suspended" || member.Health || member.Agent.Healthy || !member.Agent.LastSeen.IsZero() || !strings.Contains(member.Agent.Message, "not running") {
			t.Fatalf("cached live status survived suspension: %+v", member)
		}
	}
	for _, condition := range status.Conditions {
		if (condition.Type == "ClusterHealth" || condition.Type == "ComponentsReady") && condition.Status != "False" {
			t.Fatalf("suspended health condition: %+v", condition)
		}
	}
}

func TestYellowAndRedStatusExplainShardAvailability(t *testing.T) {
	p, rc, f, es := coreSetup(t, 1)
	coreConverge(t, p, rc, f)
	for _, health := range []string{"yellow", "red"} {
		es.health = health
		status, err := p.Status(context.Background(), rc)
		if err != nil {
			t.Fatal(err)
		}
		if health == "yellow" && (!strings.Contains(status.Message, "primary shards are allocated") || !strings.Contains(status.Message, "replica shards are unassigned") || !strings.Contains(status.Message, "redundancy is reduced")) {
			t.Fatal(status.Message)
		}
		if health == "red" && !strings.Contains(status.Message, "primary shards are unavailable") {
			t.Fatal(status.Message)
		}
	}
}

func TestRollingStatusAndEventsFollowNativeComponentSteps(t *testing.T) {
	p, rc, f, es := coreSetup(t, 3)
	rc.Cluster.Spec.Version = "9.4.8"
	for _, kind := range []string{kindKibana, kindLogstash, kindAPM} {
		component := rc.Cluster.Spec.Components.Extra[kind]
		component.Replicas = 1
		rc.Cluster.Spec.Components.Extra[kind] = component
	}
	coreConverge(t, p, rc, f)
	es.master = "demon1"
	rc.Cluster.Spec.Version = "9.5.5"
	events := rc.Events.(*coreEvents)
	events.events = nil
	order := []string{}
	steps := map[string]bool{}
	complete := false
	for range 200 {
		if _, err := p.Sync(context.Background(), rc); err != nil {
			t.Fatal(err)
		}
		s, err := load(rc.Cluster)
		if err != nil {
			t.Fatal(err)
		}
		if s.Rolling != nil {
			status, err := p.Status(context.Background(), rc)
			if err != nil || status.Phase != domain.PhaseUpgrading || status.Message == "" {
				t.Fatalf("rolling status: %+v %v", status, err)
			}
			step, message := rollingProgress(s)
			if status.Message != message {
				t.Fatalf("status lost native progress: %s != %s", status.Message, message)
			}
			if !slices.ContainsFunc(status.Conditions, func(c domain.Condition) bool {
				return c.Type == "RollingProgress" && c.Reason == step && c.Message == message
			}) {
				t.Fatalf("native operation step missing: %+v", status.Conditions)
			}
			if s.Rolling.Current != "" && !slices.Contains(order, s.Rolling.Current) {
				order = append(order, s.Rolling.Current)
			}
			steps[step] = true
		}
		if s.Phase == "ready" && s.Rolling == nil {
			complete = true
			break
		}
	}
	if !complete || !reflect.DeepEqual(order, []string{"demon0", "demon2", "demon1", "demokb", "demols", "demoapm"}) {
		t.Fatalf("unexpected rolling component order: %v (complete=%v)", order, complete)
	}
	for _, step := range []string{"node/demon0/flush", "node/demon0/rejoin", "node/demon0/recover", "kibana/demokb/rejoin", "logstash/demols/rejoin", "apm/demoapm/rejoin"} {
		if !steps[step] {
			t.Fatalf("missing member/component stage %s: %+v", step, steps)
		}
	}
	for i, event := range events.events {
		if event.Reason != "RollingProgress" || event.Type != "Normal" || event.Message == "" || (i > 0 && event.Message == events.events[i-1].Message) {
			t.Fatalf("rolling transitions missing or repeated: %+v", events.events)
		}
	}
	if len(events.events) < 10 {
		t.Fatalf("rolling transitions not recorded: %+v", events.events)
	}
}

func TestApplicationReadinessGatesComponentStatusAndRollingCompletion(t *testing.T) {
	p, rc, f, _ := coreSetup(t, 1)
	logstash := rc.Cluster.Spec.Components.Extra[kindLogstash]
	logstash.Replicas = 1
	rc.Cluster.Spec.Components.Extra[kindLogstash] = logstash
	s := coreConverge(t, p, rc, f)
	observation := agentspec.Readiness{Component: "logstash", ConfigVersion: 1 << 30, Message: "Logstash pipeline ingest is not running"}
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/status":
			_ = json.NewEncoder(w).Encode(agentapi.Status{ConfigVersion: 1 << 30, Processes: map[string]agentapi.ProcessStatus{"elasticsearch": {State: "running"}, "logstash": {State: "running"}}})
		case "/v1/elasticsearch/readiness":
			_ = json.NewEncoder(w).Encode(observation)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	rc.Agents = coreAgentDialer{server.URL, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw})}
	m := s.find("demols")
	for _, scenario := range []string{"pipeline-failed", "configuration-pending", "ready"} {
		switch scenario {
		case "configuration-pending":
			observation.Ready, observation.ConfigVersion = true, 0
		case "ready":
			observation.ConfigVersion, observation.Message = 1<<30, ""
		}
		ready := scenario == "ready"
		if p.componentReady(context.Background(), rc, m) != ready {
			t.Fatalf("%s: process state overrode application readiness", scenario)
		}
		status, err := p.Status(context.Background(), rc)
		if err != nil {
			t.Fatal(err)
		}
		member := status.Members[slices.IndexFunc(status.Members, func(member domain.MemberStatus) bool { return member.Name == m.Name })]
		if member.Health != ready || (status.Phase == domain.PhaseReady) != ready {
			t.Fatalf("%s: component status %+v phase %s", scenario, member, status.Phase)
		}
		if !ready && !strings.Contains(status.Message, m.Name) {
			t.Fatalf("%s: component failure not surfaced: %+v", scenario, status)
		}
		s.Rolling = &rolling{Reason: "upgrade", Version: rc.Cluster.Spec.Version, Current: m.Name, Order: []string{m.Name}, Step: "rejoin"}
		if _, _, err := p.rollingStep(context.Background(), rc, s); err != nil {
			t.Fatal(err)
		}
		if (s.Rolling.Current == "") != ready {
			t.Fatalf("%s: rolling completion ignored readiness: %+v", scenario, s.Rolling)
		}
	}
}

func TestStatusReportsAgentResourcesNotPlatformRecords(t *testing.T) {
	p, rc, f, _ := coreSetup(t, 1)
	coreConverge(t, p, rc, f)
	var reported atomic.Pointer[agentapi.ContainerResources]
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/status":
			_ = json.NewEncoder(w).Encode(agentapi.Status{ConfigVersion: 1 << 30, Processes: map[string]agentapi.ProcessStatus{"elasticsearch": {State: "running"}}, Resources: reported.Load()})
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	rc.Agents = coreAgentDialer{server.URL, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw})}
	name := NodeHostname("demo", 0)
	f.services[name].Resources = domain.Resources{CPUMode: "SHARED", MinCPU: 1, MaxCPU: 2, MinRAM: 2, MaxRAM: 4, MinDisk: 5, MaxDisk: 20}
	member := func() domain.MemberStatus {
		t.Helper()
		status, err := p.Status(context.Background(), rc)
		if err != nil {
			t.Fatal(err)
		}
		i := slices.IndexFunc(status.Members, func(m domain.MemberStatus) bool { return m.Name == name })
		if i < 0 {
			t.Fatalf("member %s missing: %+v", name, status.Members)
		}
		return status.Members[i]
	}
	if m := member(); m.Resources == nil || *m.Resources != (domain.ObservedResources{CPUCores: 1, MemoryGB: 2, DiskGB: 5}) {
		t.Fatalf("agent without a resource reading must fall back to effective autoscaling: %+v", m.Resources)
	}
	reported.Store(&agentapi.ContainerResources{CPUCores: 2, MemoryBytes: 4 << 30, RootDiskBytes: 5 << 30})
	if m := member(); m.Resources == nil || *m.Resources != (domain.ObservedResources{CPUCores: 2, MemoryGB: 4, DiskGB: 5}) {
		t.Fatalf("agent reading must not be overwritten by platform records: %+v", m.Resources)
	}
}

func TestProcessDiagnosticsPreserveAgentConnectivityAndNativeRole(t *testing.T) {
	for _, kind := range []string{kindNode, kindKibana, kindLogstash, kindAPM} {
		for _, scenario := range []string{"healthy", "paused", "error", "configuration-error"} {
			t.Run(kind+"/"+scenario, func(t *testing.T) {
				p, rc, f, _ := coreSetup(t, 1)
				if kind != kindNode {
					desired := rc.Cluster.Spec.Components.Extra[kind]
					desired.Replicas = 1
					rc.Cluster.Spec.Components.Extra[kind] = desired
				}
				s := coreConverge(t, p, rc, f)
				target := s.members(kindNode)[0]
				if kind != kindNode {
					target = s.componentMembers()[0]
				}
				server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					switch r.URL.Path {
					case "/v1/status":
						processes := map[string]agentapi.ProcessStatus{"elasticsearch": {State: "running"}}
						process := agentapi.ProcessStatus{State: "running"}
						if scenario == "paused" {
							process.State = "paused"
						}
						if scenario == "error" {
							process.Error = "child failure"
						}
						processes[product(target)] = process
						status := agentapi.Status{ConfigVersion: 1 << 30, Processes: processes}
						if scenario == "configuration-error" {
							status.Error = "configuration rejected"
						}
						_ = json.NewEncoder(w).Encode(status)
					case "/v1/elasticsearch/readiness":
						_ = json.NewEncoder(w).Encode(agentspec.Readiness{Ready: true, ConfigVersion: 1 << 30})
					default:
						http.NotFound(w, r)
					}
				}))
				defer server.Close()
				rc.Agents = coreAgentDialer{server.URL, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw})}
				if ready := p.componentReady(t.Context(), rc, target); ready != (scenario == "healthy") {
					t.Fatalf("child/config diagnostic did not gate reconciliation readiness: %v", ready)
				}
				status, err := p.Status(t.Context(), rc)
				if err != nil {
					t.Fatal(err)
				}
				ms := status.Members[slices.IndexFunc(status.Members, func(m domain.MemberStatus) bool { return m.Name == target.Name })]
				if !ms.Agent.Healthy || ms.Agent.LastSeen.IsZero() || ms.Role == "" {
					t.Fatalf("reachable agent/native role lost: %+v", ms)
				}
				if scenario == "healthy" {
					if !ms.Health || status.Phase != domain.PhaseReady {
						t.Fatalf("healthy processes changed native health: %+v", status)
					}
					for _, condition := range status.Conditions {
						if strings.HasPrefix(condition.Type, "Process/") {
							t.Fatalf("healthy process emitted condition: %+v", condition)
						}
					}
					return
				}
				if ms.Health || status.Phase == domain.PhaseReady {
					t.Fatalf("failed child/config reported Ready: %+v", status)
				}
				if scenario == "configuration-error" {
					return
				}
				if ms.State != scenario {
					t.Fatalf("child diagnostic lost: %+v", ms)
				}
				reason := "ProcessError"
				if scenario == "paused" {
					reason = "ProcessPaused"
					if !strings.Contains(ms.Agent.Message, "SIGCONT") {
						t.Fatalf("missing investigation guidance: %+v", ms)
					}
				}
				if !slices.ContainsFunc(status.Conditions, func(c domain.Condition) bool {
					return c.Type == "Process/"+target.Name+"/"+product(target) && c.Reason == reason
				}) {
					t.Fatalf("missing process condition: %+v", status.Conditions)
				}
			})
		}
	}
}
