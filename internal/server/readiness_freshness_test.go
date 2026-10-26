// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package server

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
)

func TestStatusAPIWithdrawsExpiredReadinessDuringBlockedReconciliation(t *testing.T) {
	server := &Server{Options: Options{Context: engine.Context{ObservationEpoch: "current-process"}}}
	for _, sampled := range []time.Time{time.Time{}, time.Now().Add(-engine.ReadinessMaxAge - time.Second)} {
		stored := domain.ClusterStatus{
			Phase: domain.PhaseReady, ObservedAt: sampled,
			ObservationEpoch: "current-process",
			Conditions:       []domain.Condition{{Type: "Healthy", Status: "True"}, {Type: "Replication/rs0", Status: "True", Reason: "WritablePrimary"}},
		}
		got := server.statusAPI(domain.ClusterRef{Name: "demo"}, stored)
		if string(got.Phase) == "Ready" {
			t.Fatal("cached Ready survived its observation lease", got)
		}
		for _, condition := range got.Conditions {
			if string(condition.Status) == "True" {
				t.Fatal("cached positive availability survived lease", condition)
			}
		}
		if stored.Phase != domain.PhaseReady || stored.Conditions[0].Status != "True" {
			t.Fatal("API projection mutated stored observation")
		}
	}
}

func TestStatusAPIFencesReadinessAtProcessStart(t *testing.T) {
	sampledAt := time.Now().Add(-2 * time.Second)
	s := &Server{Options: Options{Context: engine.Context{ObservationEpoch: "current-process"}}}
	for _, tc := range []struct {
		name    string
		sampled time.Time
		epoch   string
		ready   bool
	}{
		{"pre-start-inside-lease", sampledAt.Add(-time.Second), "previous-process", false},
		{"same-wall-previous-process", sampledAt, "previous-process", false},
		{"at-start-current-process", sampledAt, "current-process", true},
		{"post-start-current-process", sampledAt.Add(time.Second), "current-process", true},
		{"legacy-provenance-missing", sampledAt, "", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			stored := domain.ClusterStatus{
				Phase: domain.PhaseReady, ObservedAt: tc.sampled, ObservedGeneration: 1,
				ObservationEpoch: tc.epoch,
				Conditions:       []domain.Condition{{Type: "Healthy", Status: "True"}, {Type: "Ready", Status: "True"}, {Type: "Replication/rs0", Status: "True", Reason: "WritablePrimary"}},
				Members:          []domain.MemberStatus{{Name: "db", Health: true, Agent: domain.AgentStatus{Healthy: true}}},
			}
			got := s.statusAPI(domain.ClusterRef{Name: "demo"}, stored)
			wantPhase, wantCondition := "Degraded", "Unknown"
			if tc.ready {
				wantPhase, wantCondition = "Ready", "True"
			}
			if string(got.Phase) != wantPhase {
				t.Fatalf("phase = %s, want %s", got.Phase, wantPhase)
			}
			for _, condition := range got.Conditions {
				if string(condition.Status) != wantCondition || (!tc.ready && condition.Reason != "ObservationStale") {
					t.Fatalf("unexpected availability condition: %+v", condition)
				}
			}
			if got.Members[0].Health != tc.ready || got.Members[0].Agent.Connected != tc.ready {
				t.Fatalf("unexpected member readiness: %+v", got.Members[0])
			}
			if stored.Phase != domain.PhaseReady || stored.Conditions[0].Status != "True" || !stored.Members[0].Health || !stored.Members[0].Agent.Healthy || !stored.ObservedAt.Equal(tc.sampled) {
				t.Fatal("API projection mutated persisted observation", stored)
			}
			// The internal deployment decision must retain its lease-only view,
			// even after the API has projected this same snapshot as stale.
			var decision *bool
			args := engine.StatelessReadiness(&engine.Context{Cluster: &domain.Cluster{Status: stored}}, "frontend", &decision, "native")
			if len(args) != 1 || args[0] != "native" || engine.FreshStatus(stored, time.Now()).Phase != domain.PhaseReady {
				t.Fatal("API restart fence changed internal readiness decisions")
			}
		})
	}
}

func TestReadinessRoutesFencePreStartObservations(t *testing.T) {
	s, _, token := testServer(t)
	sampledAt := time.Now().Add(-time.Second)
	for _, tc := range []struct {
		name    string
		sampled time.Time
		epoch   string
		ready   bool
	}{
		{"pre-start-inside-lease", sampledAt.Add(-time.Second), "previous-process", false},
		{"same-wall-previous-process", sampledAt, "previous-process", false},
		{"current-process", sampledAt, s.Context.ObservationEpoch, true},
		{"future", time.Now().Add(time.Minute), s.Context.ObservationEpoch, false},
		{"expired", time.Now().Add(-engine.ReadinessMaxAge - time.Second), s.Context.ObservationEpoch, false},
		{"missing", time.Time{}, s.Context.ObservationEpoch, false},
		{"legacy-provenance-missing", sampledAt, "", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c, err := s.Store.GetCluster(t.Context(), domain.ClusterRef{Name: "prod"})
			if err != nil {
				t.Fatal(err)
			}
			c.Status = domain.ClusterStatus{
				Phase: domain.PhaseReady, ObservedAt: tc.sampled, ObservedGeneration: c.Generation,
				ObservationEpoch: tc.epoch,
				Conditions:       []domain.Condition{{Type: "Healthy", Status: "True"}},
				Members:          []domain.MemberStatus{{Name: "prodm0", Set: "rs0", Health: true, Agent: domain.AgentStatus{Healthy: true}}},
			}
			if err := s.Store.UpdateClusterState(t.Context(), c); err != nil {
				t.Fatal(err)
			}
			for _, path := range []string{
				"/api/v1/clusters/prod",
				"/api/v1/clusters/prod/status",
				"/api/v1/clusters/prod/members",
				"/api/v1/clusters/prod/members/prodm0",
			} {
				r := call(s, http.MethodGet, path, "", token)
				if r.Code != http.StatusOK {
					t.Fatalf("%s: %d %s", path, r.Code, r.Body.String())
				}
				var body map[string]any
				if err := json.Unmarshal(r.Body.Bytes(), &body); err != nil {
					t.Fatal(err)
				}
				status := body
				if nested, ok := body["status"].(map[string]any); ok {
					status = nested
				}
				if phase, ok := status["phase"]; ok {
					want := "Degraded"
					if tc.ready {
						want = "Ready"
					}
					if phase != want {
						t.Fatalf("%s: phase = %v, want %s", path, phase, want)
					}
				}
				member := status
				if members, ok := status["members"].([]any); ok {
					member = members[0].(map[string]any)
				} else if items, ok := body["items"].([]any); ok {
					member = items[0].(map[string]any)
				}
				agent, ok := member["agent"].(map[string]any)
				if member["health"] != tc.ready || !ok || agent["connected"] != tc.ready {
					t.Fatalf("%s: unexpected member readiness: %+v", path, member)
				}
			}
			persisted, err := s.Store.GetCluster(t.Context(), c.Ref())
			if err != nil {
				t.Fatal(err)
			}
			if persisted.Status.Phase != domain.PhaseReady || !persisted.Status.Members[0].Health || !persisted.Status.ObservedAt.Equal(tc.sampled) {
				t.Fatal("status reads changed persisted readiness", persisted.Status)
			}
		})
	}
}

func TestStatusAPIStartupFenceAcrossClockSteps(t *testing.T) {
	started := time.Date(2026, time.October, 24, 9, 0, 0, 0, time.UTC)
	for _, step := range []struct {
		name string
		jump time.Duration
	}{
		{"large-backstep", -8 * time.Hour},
		{"small-backstep", -5 * time.Second},
		{"unchanged", 0},
		{"small-forward-step", 5 * time.Second},
		{"large-forward-step", 8 * time.Hour},
	} {
		t.Run(step.name, func(t *testing.T) {
			at := started.Add(4*time.Second + step.jump)
			s := &Server{Options: Options{Context: engine.Context{ObservationEpoch: "current-process"}}}
			for _, tc := range []struct {
				name    string
				sampled time.Time
				epoch   string
				ready   bool
			}{
				{"fresh-current-process", at.Add(-time.Second), "current-process", true},
				{"same-wall-pre-start-snapshot", at.Add(-time.Second), "previous-process", false},
				{"exact-wall-current-process", at, "current-process", true},
				{"exact-wall-previous-process", at, "previous-process", false},
				{"old-frame-pre-start", started.Add(-time.Second), "previous-process", false},
				{"old-frame-current-process", started.Add(time.Second), "current-process", step.jump == 0 || step.jump == 5*time.Second},
				{"future-observation", at.Add(time.Nanosecond), "current-process", false},
				{"expired", at.Add(-engine.ReadinessMaxAge - time.Nanosecond), "current-process", false},
				{"exact-lease-boundary", at.Add(-engine.ReadinessMaxAge), "current-process", true},
				{"zero-observation", time.Time{}, "current-process", false},
				{"missing-provenance", at, "", false},
			} {
				t.Run(tc.name, func(t *testing.T) {
					stored := domain.ClusterStatus{
						Phase: domain.PhaseReady, ObservedAt: tc.sampled, ObservedGeneration: 7, ObservationEpoch: tc.epoch,
						Conditions: []domain.Condition{
							{Type: "Healthy", Status: "True", Reason: "Observed", LastTransitionTime: started},
							{Type: "Ready", Status: "True", Reason: "Observed", LastTransitionTime: started},
							{Type: "Replication/rs0", Status: "True", Reason: "WritablePrimary", LastTransitionTime: started},
							{Type: "Certificates", Status: "True", Reason: "Valid", LastTransitionTime: started},
						},
						Members: []domain.MemberStatus{{Name: "db", Health: true, Agent: domain.AgentStatus{Healthy: true, LastSeen: at}}},
					}
					got := s.freshStatusAt(stored, at)
					if (got.Phase == domain.PhaseReady) != tc.ready || got.Members[0].Health != tc.ready || got.Members[0].Agent.Healthy != tc.ready {
						t.Fatalf("incorrect provenance/lease projection: ready=%v phase=%s member=%+v", tc.ready, got.Phase, got.Members[0])
					}
					if !got.ObservedAt.Equal(tc.sampled) || !got.Members[0].Agent.LastSeen.Equal(at) || got.ObservedGeneration != 7 || got.ObservationEpoch != tc.epoch {
						t.Fatal("projection rewrote observation provenance or generation")
					}
					for _, condition := range got.Conditions[:3] {
						if tc.ready {
							if condition.Status != "True" {
								t.Fatal("fresh observation lost availability", condition)
							}
						} else if condition.Status != "Unknown" || condition.Reason != "ObservationStale" {
							t.Fatal("invalid/previous-process observation escaped the provenance fence", condition)
						}
					}
					if got.Conditions[3].Status != "True" {
						t.Fatal("projection changed a non-availability condition")
					}
					if stored.Phase != domain.PhaseReady || !stored.Members[0].Health || !stored.Members[0].Agent.Healthy || !stored.ObservedAt.Equal(tc.sampled) || stored.ObservationEpoch != tc.epoch {
						t.Fatal("projection mutated persisted status")
					}
					for _, condition := range stored.Conditions {
						if condition.Status != "True" || !condition.LastTransitionTime.Equal(started) {
							t.Fatal("projection mutated persisted condition slices")
						}
					}
					internal := engine.FreshStatus(stored, at)
					leaseValid := !tc.sampled.IsZero() && !tc.sampled.After(at) && at.Sub(tc.sampled) <= engine.ReadinessMaxAge
					if (internal.Phase == domain.PhaseReady) != leaseValid {
						t.Fatal("API provenance fence changed the generic readiness lease")
					}
				})
			}
		})
	}
}

type epochReadinessClock struct{ at time.Time }

func (c epochReadinessClock) Now() time.Time { return c.at }

type epochReadinessProvider struct {
	engine.Provider
	observe func() domain.ClusterStatus
}

func (p epochReadinessProvider) Status(context.Context, *engine.Context) (domain.ClusterStatus, error) {
	return p.observe(), nil
}

func TestStartupEpochRejectsSmallBackstepReplayAtIdenticalWallTime(t *testing.T) {
	started := time.Date(2026, time.October, 24, 9, 0, 0, 0, time.UTC)
	preStart := started.Add(-time.Second)
	at := started.Add(4*time.Second - 5*time.Second)
	if !at.Equal(preStart) {
		t.Fatal("test must reproduce the identical-wall-time replay counterexample")
	}
	stored := domain.ClusterStatus{
		Phase: domain.PhaseReady, ObservedAt: preStart, ObservationEpoch: "previous-process",
		Conditions: []domain.Condition{{Type: "Ready", Status: "True"}},
		Members:    []domain.MemberStatus{{Name: "db", Health: true, Agent: domain.AgentStatus{Healthy: true, LastSeen: at}}},
	}
	rc := engine.Context{Cluster: &domain.Cluster{Status: stored}, Clock: epochReadinessClock{at}}
	observer := epochReadinessProvider{Provider: &fakeProvider{}, observe: func() domain.ClusterStatus {
		// Construct a new current-process observation, not a cached projection.
		return domain.ClusterStatus{
			Phase:      domain.PhaseReady,
			Conditions: []domain.Condition{{Type: "Ready", Status: "True"}},
			Members:    []domain.MemberStatus{{Name: "db", Health: true, Agent: domain.AgentStatus{Healthy: true, LastSeen: at}}},
		}
	}}
	for _, epoch := range []string{"current-process", "next-process"} {
		s := &Server{Options: Options{Context: engine.Context{ObservationEpoch: epoch}}}
		if got := s.freshStatusAt(stored, at); got.Phase == domain.PhaseReady || got.Members[0].Agent.Healthy {
			t.Fatal("previous-process snapshot replayed Ready at an identical wall timestamp")
		}
		rc.ObservationEpoch = epoch
		observed, err := engine.ObserveStatus(t.Context(), observer, &rc)
		if err != nil || observed.ObservationEpoch != epoch || !observed.ObservedAt.Equal(stored.ObservedAt) {
			t.Fatal("successful fresh sweep did not distinguish identical wall time", err)
		}
		raw, err := json.Marshal(observed)
		if err != nil {
			t.Fatal(err)
		}
		var reloaded domain.ClusterStatus
		if err := json.Unmarshal(raw, &reloaded); err != nil {
			t.Fatal(err)
		}
		if got := s.freshStatusAt(reloaded, at); got.Phase != domain.PhaseReady || !got.Members[0].Agent.Healthy {
			t.Fatal("fresh matching-process observation was suppressed after persistence")
		}
		stored = reloaded // The next process must reject this persisted epoch.
		rc.Cluster.Status = stored
	}
}

func TestStatusAPIObservationEpochIsPrivateAndExplicit(t *testing.T) {
	at := time.Now().UTC()
	stored := domain.ClusterStatus{
		Phase: domain.PhaseReady, ObservedAt: at, ObservationEpoch: "private-observer-process",
		Conditions: []domain.Condition{{Type: "Ready", Status: "True"}},
	}
	s := &Server{}
	if got := s.freshStatusAt(stored, at); got.Phase == domain.PhaseReady {
		t.Fatal("missing shared process epoch manufactured availability")
	}
	s.Context.ObservationEpoch = stored.ObservationEpoch
	persisted, err := json.Marshal(stored)
	if err != nil {
		t.Fatal(err)
	}
	var internal map[string]any
	if err := json.Unmarshal(persisted, &internal); err != nil || internal["observationEpoch"] != stored.ObservationEpoch {
		t.Fatal("private observation provenance did not survive persistence", err)
	}
	public, err := json.Marshal(s.statusAPI(domain.ClusterRef{Name: "demo"}, stored))
	if err != nil {
		t.Fatal(err)
	}
	var response map[string]any
	if err := json.Unmarshal(public, &response); err != nil {
		t.Fatal(err)
	}
	if _, exposed := response["observationEpoch"]; exposed {
		t.Fatal("private process epoch leaked into the public API")
	}
}
