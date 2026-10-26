// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package rabbitmq

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/domain"
)

func TestPartitionDiagnostics(t *testing.T) {
	for _, scenario := range []string{"healthy", "native", "disagree", "majority-timeout", "all-timeout"} {
		t.Run(scenario, func(t *testing.T) {
			p, rc, _, a := setup(t)
			ready(t, p, rc)
			want := "False"
			switch scenario {
			case "native":
				a.broker("demom0").partitions = json.RawMessage(`{"rabbit@demom0":["rabbit@demom1"]}`)
				want = "True"
			case "disagree":
				a.broker("demom0").runningNodes = []string{"rabbit@demom0"}
				a.broker("demom1").runningNodes = []string{"rabbit@demom1", "rabbit@demom2"}
				a.broker("demom2").runningNodes = []string{"rabbit@demom1", "rabbit@demom2"}
				want = "True"
			case "majority-timeout", "all-timeout":
				a.broker("demom0").runningNodes = []string{"rabbit@demom0"}
				for _, name := range []string{"demom1", "demom2"} {
					a.broker(name).observationError = "management API unresponsive: context deadline exceeded"
				}
				if scenario == "all-timeout" {
					a.broker("demom0").observationError = "management API unresponsive: context deadline exceeded"
				}
				want = "Unknown"
			}
			status, err := p.Status(t.Context(), rc)
			if err != nil {
				t.Fatal(err)
			}
			assertLifecycleCondition(t, status, "Partition", want)
			if scenario == "healthy" {
				if status.Phase != domain.PhaseReady || status.Message != "" {
					t.Fatalf("healthy status: %+v", status)
				}
				return
			}
			if status.Phase != domain.PhaseDegraded || status.Message == "" {
				t.Fatalf("missing degradation diagnosis: %+v", status)
			}
			assertLifecycleCondition(t, status, "Healthy", "False")
			for _, condition := range status.Conditions {
				if condition.Type == "Partition" && (condition.Message == "" || !strings.Contains(condition.Message, "peer connectivity")) {
					t.Fatalf("missing partition guidance: %+v", condition)
				}
			}
			if strings.Contains(scenario, "timeout") {
				for _, member := range status.Members {
					if member.Agent.Message == "" {
						t.Fatalf("silent unavailable membership: %+v", member)
					}
				}
				if !strings.Contains(status.Message, "unresponsive") {
					t.Fatalf("timeout hidden: %+v", status)
				}
			}
		})
	}
}

func TestPausedPeerPreservesHealthyLocalMembers(t *testing.T) {
	p, rc, _, a := setup(t)
	ready(t, p, rc)
	a.broker("demom0").observationError = "local management API unresponsive: paused beam"
	for _, name := range []string{"demom1", "demom2"} {
		a.broker(name).membershipError = "nodes: peer RPC timed out; inspect peer connectivity"
		a.broker(name).runningNodes = []string{}
	}
	status, err := p.Status(t.Context(), rc)
	if err != nil {
		t.Fatal(err)
	}
	assertLifecycleCondition(t, status, "Healthy", "False")
	assertLifecycleCondition(t, status, "Partition", "Unknown")
	if status.Phase != domain.PhaseDegraded {
		t.Fatalf("unobserved membership established Ready: %+v", status)
	}
	for _, name := range []string{"demom0", "demom1", "demom2"} {
		if !strings.Contains(status.Message, name) || !strings.Contains(status.Message, "peer connectivity") {
			t.Fatalf("public cluster diagnosis omitted member/action: %+v", status)
		}
	}
	for _, member := range status.Members {
		if !member.Agent.Healthy || member.Agent.LastSeen.IsZero() {
			t.Fatalf("lost fresh connected agent: %+v", member)
		}
		if member.Name == "demom0" {
			if member.Health || member.State != "UNKNOWN" || member.Agent.Message == "" {
				t.Fatalf("paused broker not scoped unavailable: %+v", member)
			}
		} else if !member.Health || member.State != "RUNNING" || !strings.Contains(member.Agent.Message, "peer connectivity") {
			t.Fatalf("peer membership timeout hid healthy local broker: %+v", member)
		}
	}
	// Local evidence alone must not permit formation/rolling/removal gates.
	s, err := loadState(rc.Cluster)
	if err != nil {
		t.Fatal(err)
	}
	for _, m := range s.runningMembers() {
		if m.Name == "demom0" {
			continue
		}
		if _, healthy, err := p.memberHealth(t.Context(), rc, m); err != nil || healthy {
			t.Fatalf("membership-unavailable member passed change gate: healthy=%v err=%v", healthy, err)
		}
	}
	for _, name := range []string{"demom0", "demom1", "demom2"} {
		a.broker(name).observationError = ""
		a.broker(name).membershipError = ""
		a.broker(name).runningNodes = nil
	}
	status, err = p.Status(t.Context(), rc)
	if err != nil || status.Phase != domain.PhaseReady {
		t.Fatalf("fresh native recovery did not clear degradation: %+v %v", status, err)
	}
}
