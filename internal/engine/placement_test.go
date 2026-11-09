// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package engine

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/infra"
)

type placementBackend struct {
	infra.Backend
	failures []infra.PlacementFailure
	err      error
}

func (b *placementBackend) PlacementFailures(context.Context, []string) ([]infra.PlacementFailure, error) {
	return b.failures, b.err
}

func TestPlacementObservationSurfacesAndClearsCurrentFailure(t *testing.T) {
	at := time.Now().UTC()
	b := &placementBackend{failures: []infra.PlacementFailure{{Member: "m0", Node: "node-a", Message: "member m0 node node-a: dimension memory exhausted; constraint filtered"}}}
	events := &deploymentGuardEvents{}
	rc := &Context{Backend: b, Events: events, Clock: observationClock{at}, Cluster: &domain.Cluster{Name: "demo"}}
	p := observationProvider{observe: func(context.Context, *Context) (domain.ClusterStatus, error) {
		s := readyObservation(at)
		s.Conditions = append(s.Conditions, domain.Condition{Type: "Ready", Status: "True"})
		return s, nil
	}}
	status, err := ObserveStatus(t.Context(), p, rc)
	if err != nil || status.Phase == domain.PhaseReady || status.Members[0].Health || !strings.Contains(status.Message, "memory") {
		t.Fatalf("scheduler failure hidden or false Ready: %+v %v", status, err)
	}
	found := false
	for _, c := range status.Conditions {
		if c.Type == "Placement/m0" && c.Reason == "BackendPlacementFailed" {
			found = true
		}
		if c.Type == "Ready" && c.Status == "True" {
			t.Fatal("false Ready condition")
		}
	}
	if !found || len(events.events) != 1 || events.events[0].Reason != "BackendPlacementFailed" {
		t.Fatalf("missing condition/event: %+v %+v", status, events.events)
	}
	rc.Cluster.Status = status
	if _, err := ObserveStatus(t.Context(), p, rc); err != nil || len(events.events) != 1 {
		t.Fatal("unchanged placement failure repeated event", err)
	}
	b.failures = nil
	status, err = ObserveStatus(t.Context(), p, rc)
	if err != nil || status.Phase != domain.PhaseReady {
		t.Fatalf("placement recovery hidden: %+v %v", status, err)
	}
	for _, c := range status.Conditions {
		if c.Reason == "BackendPlacementFailed" {
			t.Fatal("stale placement failure retained")
		}
	}
}

func TestPlacementObservationReadFailureNeverRenewsReady(t *testing.T) {
	at := time.Now().UTC()
	rc := &Context{Backend: &placementBackend{err: errors.New("scheduler unavailable")}, Clock: observationClock{at}, Cluster: &domain.Cluster{Name: "demo"}}
	calls := 0
	p := observationProvider{observe: func(context.Context, *Context) (domain.ClusterStatus, error) {
		calls++
		return readyObservation(at), nil
	}}
	status, err := ObserveStatus(t.Context(), p, rc)
	if err == nil || calls != 2 || status.Phase == domain.PhaseReady || !status.ObservedAt.IsZero() || status.Members[0].Agent.Healthy {
		t.Fatalf("failed scheduler read renewed readiness: %+v %v", status, err)
	}
}

type failureDomainBackend struct {
	infra.Backend
	eligible []string
	groups   map[string]string
}

func (*failureDomainBackend) Capabilities() infra.Capabilities {
	return infra.Capabilities{Placement: true}
}
func (b *failureDomainBackend) PlacementSnapshot(context.Context) (infra.PlacementSnapshot, error) {
	return infra.PlacementSnapshot{Eligible: b.eligible}, nil
}
func (b *failureDomainBackend) Containers(_ context.Context, member string) ([]infra.Container, error) {
	return []infra.Container{{PlacementGroup: b.groups[member]}}, nil
}

func TestVotingFailureDomainsConditionAndEvent(t *testing.T) {
	b := &failureDomainBackend{eligible: []string{"free", "worker-a", "worker-b"}}
	events := &deploymentGuardEvents{}
	rc := &Context{Backend: b, Events: events, Cluster: &domain.Cluster{Name: "demo"}}
	makeStatus := func() domain.ClusterStatus {
		return domain.ClusterStatus{Phase: domain.PhaseReady, Conditions: []domain.Condition{{Type: "Ready", Status: "True"}}, Members: []domain.MemberStatus{
			{Name: "m0", PlacementGroup: "worker-a"},
			{Name: "m1", PlacementGroup: "worker-b"},
			{Name: "m2", PlacementGroup: "worker-b"},
			{Name: "proxy", PlacementGroup: "worker-a"},
			{Name: "learner", PlacementGroup: "worker-b"},
		}}
	}
	voters := []string{"m0", "m1", "m2"}
	status := makeStatus()
	if err := ObserveFailureDomains(t.Context(), rc, &status, voters); err != nil {
		t.Fatal(err)
	}
	condition := status.Conditions[len(status.Conditions)-1]
	if condition.Status != "False" || condition.Reason != "SharedNode" || status.Phase != domain.PhaseDegraded ||
		!strings.Contains(condition.Message, "m1, m2 share node worker-b") || !strings.Contains(condition.Message, "clusters replace-member demo m2 --wait") ||
		len(events.events) != 0 {
		t.Fatalf("missing actionable condition or observation emitted an uncommitted event: %+v, %+v", status, events.events)
	}
	rc.Cluster.Status = status
	status = makeStatus()
	if err := ObserveFailureDomains(t.Context(), rc, &status, voters); err != nil || len(events.events) != 0 {
		t.Fatalf("observation emitted an uncommitted event: %v %+v", err, events.events)
	}
	b.eligible = []string{"worker-a", "worker-b"}
	status = makeStatus()
	if err := ObserveFailureDomains(t.Context(), rc, &status, voters); err != nil {
		t.Fatal(err)
	}
	condition = status.Conditions[len(status.Conditions)-1]
	if condition.Status != "Unknown" || condition.Reason != "InsufficientEligibleNodes" || len(events.events) != 0 {
		t.Fatalf("insufficient nodes reported as repairable drift: %+v", condition)
	}
	status = makeStatus()
	status.Members[2].PlacementGroup = "free"
	if err := ObserveFailureDomains(t.Context(), rc, &status, voters); err != nil {
		t.Fatal(err)
	}
	condition = status.Conditions[len(status.Conditions)-1]
	if condition.Status != "True" || condition.Reason != "DistinctNodes" {
		t.Fatalf("repaired placement retained warning: %+v", condition)
	}
	status.Members[2].PlacementGroup = ""
	if err := ObserveFailureDomains(t.Context(), rc, &status, voters); err != nil {
		t.Fatal(err)
	}
	condition = status.Conditions[len(status.Conditions)-1]
	if condition.Status != "Unknown" || condition.Reason != "PlacementUnknown" {
		t.Fatal(condition)
	}
}

func TestReplacementPlacementAvoidRequiresFreeSurvivorDomain(t *testing.T) {
	b := &failureDomainBackend{eligible: []string{"free", "worker-a", "worker-b"}, groups: map[string]string{"m0": "worker-a", "m1": "worker-b"}}
	rc := &Context{Backend: b, Cluster: &domain.Cluster{Name: "demo"}}
	avoid, err := ReplacementPlacementAvoid(t.Context(), rc, []string{"m0", "m1"}, nil)
	if err != nil || strings.Join(avoid, ",") != "worker-a,worker-b" {
		t.Fatalf("strict avoidance = %v, %v", avoid, err)
	}
	if _, err := ReplacementPlacementAvoid(t.Context(), rc, []string{"m0", "m1"}, []string{"free"}); !errors.Is(err, infra.ErrPlacementUnsatisfiable) {
		t.Fatalf("explicit avoidance weakened: %v", err)
	}
	b.eligible = []string{"worker-a", "worker-b"}
	if _, err := ReplacementPlacementAvoid(t.Context(), rc, []string{"m0", "m1"}, nil); !errors.Is(err, infra.ErrPlacementUnsatisfiable) || !strings.Contains(err.Error(), "refusing co-location") {
		t.Fatalf("exhausted eligibility did not fail closed: %v", err)
	}
	delete(b.groups, "m1")
	if _, err := ReplacementPlacementAvoid(t.Context(), rc, []string{"m0", "m1"}, nil); !errors.Is(err, ErrUnavailable) {
		t.Fatal(err)
	}
}
