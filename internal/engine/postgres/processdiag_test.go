// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package postgres

import (
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/domain"
)

func assertProcessProjection(t *testing.T, status domain.ClusterStatus, name, role, state, conditionType string) {
	t.Helper()
	for _, member := range status.Members {
		if member.Name != name {
			continue
		}
		if !member.Agent.Healthy || member.Role != role {
			t.Fatalf("lost connectivity or native role: %+v", member)
		}
		if state == "running" {
			if !member.Health || status.Phase != domain.PhaseReady {
				t.Fatalf("healthy child changed status: %+v", member)
			}
		} else if member.Health || member.State != state || status.Phase == domain.PhaseReady {
			t.Fatalf("process failure hidden: %+v phase=%s", member, status.Phase)
		}
		found := false
		for _, condition := range status.Conditions {
			if condition.Type == conditionType {
				found = true
				reason := "ProcessError"
				if state == "paused" {
					reason = "ProcessPaused"
				}
				if condition.Status != "False" || condition.Reason != reason {
					t.Fatalf("wrong process condition: %+v", condition)
				}
			}
			if state != "running" && condition.Type == "Ready" && condition.Status == "True" {
				t.Fatal("failed child reported Ready")
			}
		}
		if found != (state != "running") {
			t.Fatalf("unexpected diagnostic presence: %+v", status.Conditions)
		}
		return
	}
	t.Fatal("member not found", name)
}

func TestDataProcessDiagnostics(t *testing.T) {
	for _, state := range []string{"paused", "error", "running"} {
		t.Run(state, func(t *testing.T) {
			p, rc, backend := setup(t, "ha")
			s := converge(t, p, rc)
			process := agentapi.ProcessStatus{State: state}
			if state == "error" {
				process.Error = "supervised child failed"
			}
			backend.genericOverrides = map[string]agentapi.Status{"demop0": {ConfigVersion: s.ConfigVersion, Processes: map[string]agentapi.ProcessStatus{"postgres": process}}}
			status, err := p.Status(t.Context(), rc)
			if err != nil {
				t.Fatal(err)
			}
			assertProcessProjection(t, status, "demop0", "primary", state, "Process/demop0/postgres")
			applied, err := ready(t.Context(), rc, s.find("demop0"))
			if err != nil || applied != (state == "running") {
				t.Fatalf("data reconcile readiness ignored process state: %v %v", applied, err)
			}
		})
	}
}

func TestProxyProcessDiagnosticsEveryInstance(t *testing.T) {
	for _, state := range []string{"paused", "error", "running"} {
		t.Run(state, func(t *testing.T) {
			p, rc, _, s, _, late := replicatedProxyFixture(t)
			process := agentapi.ProcessStatus{State: state}
			if state == "error" {
				process.Error = "supervised child failed"
			}
			late.mu.Lock()
			late.processes = map[string]agentapi.ProcessStatus{"proxysql": process}
			late.mu.Unlock()
			status, err := p.Status(t.Context(), rc)
			if err != nil {
				t.Fatal(err)
			}
			assertProcessProjection(t, status, "demopx", "proxy", state, "Process/demopx/proxysql/allocation-b")
			applied, err := proxyApplied(t.Context(), rc, s.find("demopx"), s.ConfigVersion, true)
			if err != nil || applied != (state == "running") {
				t.Fatalf("proxy reconcile readiness ignored process state: %v %v", applied, err)
			}
		})
	}
}

func TestProxyNativeFailureKeepsAgentConnected(t *testing.T) {
	p, rc, _, _, _, late := replicatedProxyFixture(t)
	late.mu.Lock()
	late.nativeError = true
	late.mu.Unlock()
	status, err := p.Status(t.Context(), rc)
	if err != nil {
		t.Fatal(err)
	}
	for _, member := range status.Members {
		if member.Name == "demopx" {
			if !member.Agent.Healthy || member.Health || status.Phase == domain.PhaseReady {
				t.Fatalf("native failure confused with connectivity: %+v", member)
			}
			return
		}
	}
	t.Fatal("proxy missing")
}
