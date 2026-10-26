// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package rabbitmq

import (
	"context"
	"encoding/json"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
)

func TestCorrectedConfigRepairsCurrentRollingMember(t *testing.T) {
	for _, checkpoint := range []string{"reload", "start", "wait"} {
		t.Run(checkpoint, func(t *testing.T) {
			p, rc, f, a := setup(t)
			ready(t, p, rc)
			s := state(t, rc)
			m := s.find("demom0")
			rc.Cluster.Spec.EngineConfig = "invalid_setting = true"
			beginChange(rc, s, m, "deploy")
			m.EngineConfig = m.TargetConfig
			m.ChangeStep = checkpoint
			capturedVersion := m.TargetVersion
			if err := s.save(rc.Cluster); err != nil {
				t.Fatal(err)
			}
			a.broker(m.Name).running = false
			// Correct the desired config while the activated bad config is still
			// unable to start. Do not create a second rolling disruption.
			rc.Cluster.Spec.EngineConfig = "heartbeat = 30"
			a.log.clear()
			before := len(f.deployments)
			syncUntil(t, p, rc, func(s *internalState) bool {
				m := s.find("demom0")
				return m.ChangeAction == "" && m.EngineConfig == rc.Cluster.Spec.EngineConfig
			})
			m = state(t, rc).find("demom0")
			if m.TargetVersion != capturedVersion || len(f.deployments) != before+1 {
				t.Fatalf("repair changed binary target or did not redeploy exactly once: %+v", m)
			}
			calls := a.log.snapshot()
			assertBefore(t, calls, "agent:demom0:stop", "infra:demom0:deploy")
			for _, call := range calls {
				if call == "agent:demom0:drain" || strings.Contains(call, "demom1:deploy") || strings.Contains(call, "demom2:deploy") || strings.Contains(call, "demom1:stop") || strings.Contains(call, "demom2:stop") {
					t.Fatalf("repair escaped the already-disrupted member: %v", calls)
				}
			}
		})
	}
}

func TestManagedAccessReconcilesNativeDriftPeriodically(t *testing.T) {
	p, rc, _, a := setup(t)
	ready(t, p, rc)
	a.log.clear()
	if _, err := p.Sync(t.Context(), rc); err != nil {
		t.Fatal(err)
	}
	if countCall(a.log.snapshot(), "agent:demom0:bootstrap") != 0 {
		t.Fatal("native password repair was repeated before its interval")
	}
	rc.Clock.(*testClock).at = rc.Clock.Now().Add(time.Minute)
	if _, err := p.Sync(t.Context(), rc); err != nil {
		t.Fatal(err)
	}
	if countCall(a.log.snapshot(), "agent:demom0:bootstrap") != 1 {
		t.Fatal("unchanged desired secrets suppressed native access repair")
	}
	for range 3 {
		if _, err := p.Sync(t.Context(), rc); err != nil {
			t.Fatal(err)
		}
	}
	if countCall(a.log.snapshot(), "agent:demom0:bootstrap") != 1 {
		t.Fatal("managed access reconciliation hot-looped")
	}
}

func TestManagedAccessFailureIsActionableAndRecovers(t *testing.T) {
	p, rc, _, a := setup(t)
	ready(t, p, rc)
	rc.Clock.(*testClock).at = rc.Clock.Now().Add(time.Minute)
	a.failures["demom0:bootstrap"] = 503
	if _, err := p.Sync(t.Context(), rc); err == nil {
		t.Fatal("managed access reconciliation failure hidden")
	}
	status, err := p.Status(t.Context(), rc)
	if err != nil || status.Phase != domain.PhaseDegraded || !strings.Contains(status.Message, "karakoram/admin") || !strings.Contains(status.Message, "vhost /") {
		t.Fatalf("missing managed access diagnosis: %+v %v", status, err)
	}
	assertLifecycleCondition(t, status, "ManagedAccessReady", "False")
	delete(a.failures, "demom0:bootstrap")
	if _, err := p.Sync(t.Context(), rc); err != nil {
		t.Fatal(err)
	}
	status, err = p.Status(t.Context(), rc)
	if err != nil || status.Phase != domain.PhaseReady || state(t, rc).SystemUsersError != "" {
		t.Fatalf("repaired native access remained degraded: %+v %v", status, err)
	}
}

func TestAlarmStatusNamesAffectedNodesAndSafeActions(t *testing.T) {
	p, rc, _, a := setup(t)
	ready(t, p, rc)
	// Every observer receives the same cluster-wide alarms. Report affected
	// nodes once, rather than attributing a remote alarm to each observer.
	raw := json.RawMessage(`[{"type":"resource_limit","resource":"disk","node":"rabbit@demom1"},{"type":"resource_limit","resource":"memory","node":"rabbit@demom2"}]`)
	for _, name := range []string{"demom0", "demom1", "demom2"} {
		a.broker(name).alarms = raw
	}
	status, err := p.Status(t.Context(), rc)
	if err != nil || status.Phase != domain.PhaseDegraded {
		t.Fatalf("alarm status: %+v %v", status, err)
	}
	for _, condition := range status.Conditions {
		if condition.Type != "Alarms" {
			continue
		}
		for _, detail := range []string{"rabbit@demom1: disk", "rabbit@demom2: memory", "add disk capacity", "reduce publisher pressure", "do not delete queue data"} {
			if !strings.Contains(condition.Message, detail) {
				t.Fatalf("missing %q from alarm diagnosis: %s", detail, condition.Message)
			}
		}
		if strings.Count(condition.Message, "rabbit@demom1: disk") != 1 {
			t.Fatalf("cluster-wide alarm duplicated per observer: %s", condition.Message)
		}
	}
	for _, member := range status.Members {
		if member.State != "ALARM" || member.Agent.Message == "" {
			t.Fatalf("member did not explain its alarm-blocked state: %+v", member)
		}
	}
	for _, name := range []string{"demom0", "demom1", "demom2"} {
		a.broker(name).alarms = nil
	}
	status, err = p.Status(t.Context(), rc)
	if err != nil || status.Phase != domain.PhaseReady || status.Message != "" {
		t.Fatalf("cleared alarms remained degraded: %+v %v", status, err)
	}
}

func TestQuorumLossWaitsForRetainedMembersInEitherReturnOrder(t *testing.T) {
	for _, order := range [][]string{{"demom1", "demom2"}, {"demom2", "demom1"}} {
		t.Run(strings.Join(order, "-"), func(t *testing.T) {
			p, rc, f, a := setup(t)
			ready(t, p, rc)
			a.log.clear()
			before := f.mutations
			for _, name := range order {
				a.broker(name).down = true
				a.broker(name).running = false
			}
			for remaining := 2; remaining >= 0; remaining-- {
				_, _ = p.Sync(context.Background(), rc)
				status, err := p.Status(t.Context(), rc)
				if err != nil {
					t.Fatal(err)
				}
				want := domain.PhaseDegraded
				if remaining == 0 {
					want = domain.PhaseReady
				}
				if status.Phase != want {
					t.Fatalf("remaining unavailable=%d: phase=%s want=%s", remaining, status.Phase, want)
				}
				partition := "False"
				if remaining > 0 {
					partition = "Unknown"
				}
				assertLifecycleCondition(t, status, "Partition", partition)
				if remaining > 0 {
					name := order[2-remaining]
					a.broker(name).down = false
					a.broker(name).running = true
				}
			}
			if f.mutations != before {
				t.Fatalf("transient quorum loss triggered platform mutation: %v", a.log.snapshot())
			}
			for _, call := range a.log.snapshot() {
				for _, action := range []string{":forget", ":drain", ":shrink", ":stop", ":bootstrap"} {
					if strings.HasSuffix(call, action) {
						t.Fatalf("retained return used destructive/forced recovery: %v", a.log.snapshot())
					}
				}
			}
		})
	}
}

func TestObservedPartitionBlocksReadyUntilNativeRecovery(t *testing.T) {
	p, rc, f, a := setup(t)
	ready(t, p, rc)
	a.log.clear()
	before := f.mutations
	for _, name := range []string{"demom0", "demom1", "demom2"} {
		a.broker(name).partitions = json.RawMessage(`{"rabbit@demom0":["rabbit@demom1","rabbit@demom2"]}`)
	}
	if _, err := p.Sync(t.Context(), rc); err != nil {
		t.Fatal(err)
	}
	status, err := p.Status(t.Context(), rc)
	if err != nil || status.Phase != domain.PhaseDegraded || !strings.Contains(status.Message, "restore peer connectivity") {
		t.Fatalf("partition recovery advice absent: %+v %v", status, err)
	}
	assertLifecycleCondition(t, status, "Partition", "True")
	for _, name := range []string{"demom0", "demom1", "demom2"} {
		a.broker(name).partitions = nil
	}
	if _, err := p.Sync(t.Context(), rc); err != nil {
		t.Fatal(err)
	}
	status, err = p.Status(t.Context(), rc)
	if err != nil || status.Phase != domain.PhaseReady || f.mutations != before {
		t.Fatalf("native partition recovery did not converge without intervention: %+v %v", status, err)
	}
	if slices.Contains(a.log.snapshot(), "agent:demom0:stop") {
		t.Fatal("partition recovery needlessly restarted a broker")
	}
}

func TestVolumeLossStatusOverridesHealthyRuntime(t *testing.T) {
	p, rc, _, _ := setup(t)
	ready(t, p, rc)
	s := state(t, rc)
	s.find("demom0").RuntimeRecovery.VolumeLost = true
	if err := s.save(rc.Cluster); err != nil {
		t.Fatal(err)
	}
	status, err := p.Status(t.Context(), rc)
	if err != nil || status.Phase != domain.PhaseDegraded || !strings.Contains(status.Message, "empty-volume initialization is refused") {
		t.Fatalf("durable volume-loss diagnosis hidden by healthy process observation: %+v %v", status, err)
	}
	assertLifecycleCondition(t, status, "VolumeLost", "True")
	for _, member := range status.Members {
		if member.Name == "demom0" && member.Health {
			t.Fatalf("lost durable data reported healthy: %+v", member)
		}
	}
}
