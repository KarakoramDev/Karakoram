// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package elastic

import (
	"context"
	"encoding/json"
	"encoding/pem"
	"net/http"
	"net/http/httptest"
	"reflect"
	"slices"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine/elastic/agentspec"
)

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
