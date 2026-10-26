// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package postgres

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/postgres/pgspec"
)

func TestResourceRolloutAfterFailoverChoosesSynchronousReplica(t *testing.T) {
	p, rc, b := setup(t, "ha")
	rc.Cluster.Spec.EngineOptions = []byte(`{"synchronousMode":"strict"}`)
	rc.Cluster.Spec.Components.Engine.Resources.MinRAM = 1
	rc.Cluster.Spec.Components.Engine.Resources.MaxRAM = 1
	s := converge(t, p, rc)
	// The original writer has restarted at the new size. The unrolled p2
	// now holds the leader lock; only p1 is a synchronous standby.
	b.leader = "demop2"
	b.statusOverrides = map[string]pgspec.StatusResponse{
		"demop0": {Role: "replica", Running: true, Streaming: true, SyncState: "async", EtcdHealthy: true},
		"demop1": {Role: "replica", Running: true, Streaming: true, SyncState: "sync", EtcdHealthy: true},
	}
	m := s.find("demop2")
	m.Resources.MinRAM, m.Resources.MaxRAM = 2, 2
	b.services[m.Name].Resources = m.Resources
	if err := save(rc.Cluster, s); err != nil {
		t.Fatal(err)
	}
	requests := 0
	b.beforeSwitchover = func(leader, candidate string) string {
		requests++
		if leader != "demop2" || candidate != "demop1" {
			return "switchover_candidate_changed"
		}
		return ""
	}
	for range 30 {
		if _, err := p.Sync(t.Context(), rc); err != nil {
			t.Fatal(err)
		}
		if requests > 0 {
			break
		}
	}
	if b.leader != "demop1" || requests != 1 {
		t.Fatalf("rollout reused an ineligible candidate: leader=%s requests=%d", b.leader, requests)
	}
	b.statusOverrides = nil
	converge(t, p, rc)
}

func TestSwitchoverReobservesDemotionAfterConflict(t *testing.T) {
	p, rc, b := setup(t, "ha")
	s := converge(t, p, rc)
	b.beforeSwitchover = func(_, candidate string) string {
		b.leader = candidate // A graceful shutdown won the race.
		return "switchover_leader_changed"
	}
	if err := switchAway(t.Context(), rc, s, "demop0"); err != nil {
		t.Fatalf("completed failover burned a retry: %v", err)
	}
	b.beforeSwitchover = func(_, _ string) string { t.Fatal("demoted member requested another switchover"); return "" }
	if err := switchAway(t.Context(), rc, s, "demop0"); err != nil {
		t.Fatal(err)
	}
}

func TestSwitchoverRejectsUnsatisfiableConflict(t *testing.T) {
	p, rc, b := setup(t, "ha")
	s := converge(t, p, rc)
	b.beforeSwitchover = func(_, _ string) string { return "switchover_paused" }
	err := switchAway(t.Context(), rc, s, "demop0")
	if !errors.Is(err, engine.ErrInvalid) || !strings.Contains(err.Error(), "resume Patroni") {
		t.Fatalf("unsatisfiable conflict lacks terminal actionable diagnostic: %v", err)
	}
	if err := save(rc.Cluster, s); err != nil {
		t.Fatal(err)
	}
	status, statusErr := p.Status(t.Context(), rc)
	if statusErr != nil {
		t.Fatal(statusErr)
	}
	found := false
	for _, condition := range status.Conditions {
		if condition.Type == "Switchover" && condition.Reason == "SwitchoverBlocked" && strings.Contains(condition.Message, "resume Patroni") {
			found = true
		}
		if condition.Type == "Ready" && condition.Status == "True" {
			t.Fatal("blocked handoff reported Ready")
		}
	}
	if !found {
		t.Fatal("blocked handoff lost its actionable condition")
	}
}

func TestSwitchoverWaitsForHealthySynchronousReplica(t *testing.T) {
	for _, candidate := range []pgspec.StatusResponse{
		{Role: "replica", Running: true, Streaming: true, SyncState: "async"},
		{Role: "replica", Running: true, Streaming: false, SyncState: "sync"},
	} {
		p, rc, b := setup(t, "ha")
		s := converge(t, p, rc)
		rc.Cluster.Spec.EngineOptions = []byte(`{"synchronousMode":"strict"}`)
		b.statusOverrides = map[string]pgspec.StatusResponse{"demop1": candidate, "demop2": candidate}
		b.beforeSwitchover = func(_, _ string) string { t.Fatal("unsafe switchover requested"); return "" }
		if err := switchAway(t.Context(), rc, s, "demop0"); err != nil {
			t.Fatalf("temporary standby recovery burned a retry: %v", err)
		}
	}
}

func TestSwitchoverSupportsFlavourOptions(t *testing.T) {
	p, rc, b := setup(t, "ha")
	s := converge(t, p, rc)
	rc.Cluster.Spec.EngineOptions = []byte(`{"synchronousMode":"strict","siteUrl":"https://example.invalid"}`)
	b.statusOverrides = map[string]pgspec.StatusResponse{
		"demop1": {Role: "replica", Running: true, Streaming: true, SyncState: "async"},
		"demop2": {Role: "replica", Running: true, Streaming: true, SyncState: "sync"},
	}
	if err := switchAway(t.Context(), rc, s, "demop0"); err != nil || b.leader != "demop2" {
		t.Fatalf("flavour options bypassed safe shared handoff: leader=%s error=%v", b.leader, err)
	}
}

func TestSwitchoverReselectsCandidateAfterNativeConflict(t *testing.T) {
	p, rc, b := setup(t, "ha")
	s := converge(t, p, rc)
	rc.Cluster.Spec.EngineOptions = []byte(`{"synchronousMode":"strict"}`)
	b.statusOverrides = map[string]pgspec.StatusResponse{
		"demop1": {Role: "replica", Running: true, Streaming: true, SyncState: "sync"},
		"demop2": {Role: "replica", Running: true, Streaming: true, SyncState: "async"},
	}
	requests := 0
	b.beforeSwitchover = func(_, candidate string) string {
		requests++
		if requests == 1 {
			b.statusOverrides["demop1"] = pgspec.StatusResponse{Role: "replica", Running: true, Streaming: false, SyncState: "async"}
			b.statusOverrides["demop2"] = pgspec.StatusResponse{Role: "replica", Running: true, Streaming: true, SyncState: "sync"}
			return "switchover_candidate_changed"
		}
		if candidate != "demop2" {
			t.Fatalf("reused stale candidate %s", candidate)
		}
		return ""
	}
	for range 2 {
		if err := switchAway(t.Context(), rc, s, "demop0"); err != nil {
			t.Fatalf("topology race burned a retry: %v", err)
		}
	}
	if b.leader != "demop2" || requests != 2 {
		t.Fatalf("fresh topology did not converge: leader=%s requests=%d", b.leader, requests)
	}
}

func TestResourceRolloutWithLegacyAgentReachesPrimaryHandoff(t *testing.T) {
	p, rc, b := setup(t, "ha")
	rc.Cluster.Spec.EngineOptions = json.RawMessage(`{"synchronousMode":"off"}`)
	s := converge(t, p, rc)
	b.statusWireOverrides = make(map[string]json.RawMessage)
	setLegacyRole := func(name, role string) {
		// Literal previous-release wire shape: no synthetic Leader, capability
		// flags or etcdHealthy fields are added by the fake agent.
		b.statusWireOverrides[name] = json.RawMessage(fmt.Sprintf(`{"role":%q,"running":true,"streaming":true,"version":%q,"systemId":"123456789","timeline":1,"walPosition":"1/100","etcdMemberId":%q}`, role, rc.Cluster.Spec.Version, name+"-etcd"))
	}
	for _, m := range s.data() {
		role := "replica"
		if m.Name == "demop0" {
			role = "primary"
		}
		setLegacyRole(m.Name, role)
	}
	handoffs := 0
	b.beforeSwitchover = func(source, candidate string) string {
		handoffs++
		setLegacyRole(source, "replica")
		setLegacyRole(candidate, "primary")
		return ""
	}
	rc.Cluster.Spec.Components.Engine.Resources.MinRAM = 1
	rc.Cluster.Spec.Components.Engine.Resources.MaxRAM = 1
	rc.Cluster.Generation++
	for range 100 {
		if _, err := p.Sync(t.Context(), rc); err != nil {
			t.Fatal(err)
		}
		if rc.Cluster.Status.ObservedGeneration == rc.Cluster.Generation {
			break
		}
	}
	if handoffs != 1 || rc.Cluster.Status.ObservedGeneration != rc.Cluster.Generation {
		t.Fatalf("legacy rollout stalled at final primary handoff: handoffs=%d generation=%d observed=%d", handoffs, rc.Cluster.Generation, rc.Cluster.Status.ObservedGeneration)
	}
	current, err := load(rc.Cluster)
	if err != nil {
		t.Fatal(err)
	}
	for _, m := range current.data() {
		if m.Resources.MinRAM != 1 || m.Resources.MaxRAM != 1 || b.services[m.Name].Resources.MinRAM != 1 || b.services[m.Name].Resources.MaxRAM != 1 {
			t.Fatalf("legacy rollout left %s at old resources", m.Name)
		}
	}
	status, err := p.Status(t.Context(), rc)
	if err != nil {
		t.Fatal(err)
	}
	if status.Phase != "Ready" || healthCondition(status, "Ready").Status != "True" {
		t.Fatalf("legacy rollout did not become ready: phase=%s ready=%s", status.Phase, healthCondition(status, "Ready").Status)
	}
}

func TestSwitchoverRequiresAdvertisedLocalEtcdHealth(t *testing.T) {
	p, rc, b := setup(t, "ha")
	s := converge(t, p, rc)
	status, err := live(t.Context(), rc, "demop0")
	if err != nil {
		t.Fatal(err)
	}
	status.EtcdHealthy = false
	status.HealthCapabilities = &pgspec.HealthCapabilities{LocalEtcd: true}
	b.statusOverrides = map[string]pgspec.StatusResponse{"demop0": status}
	b.beforeSwitchover = func(_, _ string) string {
		t.Fatal("handoff bypassed advertised local DCS failure")
		return ""
	}
	if err := switchAway(t.Context(), rc, s, "demop0"); err != nil {
		t.Fatal(err)
	}
}

func TestSwitchoverLegacyPlaintextConflictRemainsRetryable(t *testing.T) {
	p, rc, b := setup(t, "ha")
	s := converge(t, p, rc)
	b.plaintextSwitchover = true
	requests := 0
	b.beforeSwitchover = func(_, _ string) string {
		requests++
		if requests == 1 {
			return "internal PostgreSQL service unavailable"
		}
		return ""
	}
	err := switchAway(t.Context(), rc, s, "demop0")
	if err == nil || errors.Is(err, engine.ErrInvalid) || s.SwitchoverBlocked != "" {
		t.Fatalf("legacy transient conflict became permanent: error=%v blocked=%q", err, s.SwitchoverBlocked)
	}
	if err := switchAway(t.Context(), rc, s, "demop0"); err != nil {
		t.Fatalf("legacy conflict did not recover on fresh attempt: %v", err)
	}
	if requests != 2 || b.leader == "" || b.leader == "demop0" {
		t.Fatalf("legacy handoff did not complete: requests=%d leader=%s", requests, b.leader)
	}
}

func TestSwitchoverStructuredRejectionIsTerminal(t *testing.T) {
	p, rc, b := setup(t, "ha")
	s := converge(t, p, rc)
	b.beforeSwitchover = func(_, _ string) string { return "switchover_rejected" }
	if err := switchAway(t.Context(), rc, s, "demop0"); !errors.Is(err, engine.ErrInvalid) || s.SwitchoverBlocked == "" {
		t.Fatalf("explicit rejection lost terminal classification: error=%v blocked=%q", err, s.SwitchoverBlocked)
	}
}

func TestSwitchoverRequiresProvenLeaderForModernAgent(t *testing.T) {
	p, rc, b := setup(t, "ha")
	s := converge(t, p, rc)
	b.statusWireOverrides = map[string]json.RawMessage{
		"demop0": json.RawMessage(`{"role":"primary","running":true,"etcdHealthy":true,"healthCapabilities":{"localEtcd":true}}`),
	}
	b.beforeSwitchover = func(_, _ string) string {
		t.Fatal("modern agent's missing leader was treated as legacy")
		return ""
	}
	if err := switchAway(t.Context(), rc, s, "demop0"); err != nil {
		t.Fatal(err)
	}
}
