// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package engine

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
)

type observationProvider struct {
	Provider
	observe func(context.Context, *Context) (domain.ClusterStatus, error)
}

func (p observationProvider) Status(ctx context.Context, rc *Context) (domain.ClusterStatus, error) {
	return p.observe(ctx, rc)
}

type observationClock struct{ at time.Time }

func (c observationClock) Now() time.Time { return c.at }

func readyObservation(at time.Time) domain.ClusterStatus {
	return domain.ClusterStatus{
		Phase: domain.PhaseReady, ObservedAt: at, ObservedGeneration: 7,
		Conditions: []domain.Condition{{Type: "Healthy", Status: "True"}, {Type: "Replication/rs0", Status: "True", Reason: "WritablePrimary"}, {Type: "Certificates", Status: "True"}},
		Members:    []domain.MemberStatus{{Name: "m0", Health: true, Agent: domain.AgentStatus{Healthy: true, LastSeen: at}}},
	}
}

func TestObservationTransientFailureRetriesWithinSameCycle(t *testing.T) {
	at := time.Now().UTC()
	for _, mode := range []string{"error", "unreachable-member", "stale-member", "no-writer"} {
		t.Run(mode, func(t *testing.T) {
			calls := 0
			p := observationProvider{observe: func(ctx context.Context, rc *Context) (domain.ClusterStatus, error) {
				calls++
				deadline, ok := ctx.Deadline()
				if !ok || time.Until(deadline) > ObservationSweepTimeout {
					t.Fatal("unbounded observation")
				}
				status := readyObservation(at)
				if calls == 1 {
					switch mode {
					case "error":
						return domain.ClusterStatus{}, errors.New("one failed read")
					case "unreachable-member":
						status.Members[0].Agent.Healthy = false
					case "stale-member":
						status.Members[0].Agent.LastSeen = at.Add(-time.Second)
					case "no-writer":
						status.Phase = domain.PhaseDegraded
						status.Conditions[1].Status, status.Conditions[1].Reason = "False", "NoWritablePrimary"
					}
				}
				return status, nil
			}}
			rc := &Context{Clock: observationClock{at}, Cluster: &domain.Cluster{Status: readyObservation(at.Add(-time.Hour))}}
			got, err := ObserveStatus(t.Context(), p, rc)
			if err != nil || calls != 2 || got.Phase != domain.PhaseReady || !got.ObservedAt.Equal(at) {
				t.Fatalf("fresh successful retry did not absorb transient failure: %+v calls=%d err=%v", got, calls, err)
			}
		})
	}
}

func TestObservationFailedOrStaleSamplesNeverRenewReadiness(t *testing.T) {
	at := time.Now().UTC()
	for _, mode := range []string{"empty-error", "empty-error-current", "partial-error", "stale-agent", "missing-agent-time", "unreachable-agent"} {
		t.Run(mode, func(t *testing.T) {
			calls := 0
			p := observationProvider{observe: func(context.Context, *Context) (domain.ClusterStatus, error) {
				calls++
				status := readyObservation(at)
				switch mode {
				case "empty-error", "empty-error-current":
					return domain.ClusterStatus{}, errors.New("unavailable")
				case "partial-error":
					return status, errors.New("incomplete sweep")
				case "stale-agent":
					status.Members[0].Agent.LastSeen = at.Add(-time.Second)
				case "missing-agent-time":
					status.Members[0].Agent.LastSeen = time.Time{}
				case "unreachable-agent":
					status.Members[0].Agent.Healthy = false
				}
				return status, nil
			}}
			rc := &Context{Clock: observationClock{at}, Cluster: &domain.Cluster{Status: readyObservation(at.Add(-time.Second))}}
			if mode == "empty-error-current" {
				rc.Cluster.Status = readyObservation(at)
			}
			got, _ := ObserveStatus(t.Context(), p, rc)
			if calls != 2 || got.Phase == domain.PhaseReady || !got.ObservedAt.IsZero() || got.Members[0].Health || got.Members[0].Agent.Healthy {
				t.Fatalf("failed samples renewed readiness: %+v calls=%d", got, calls)
			}
			for _, condition := range got.Conditions {
				if readinessCondition(condition) && condition.Status == "True" {
					t.Fatal("positive availability survived failed observation", condition)
				}
			}
		})
	}
}

func TestObservationFreshNegativeSamplesPreserveHealthyPeers(t *testing.T) {
	at := time.Now().UTC()
	for _, reason := range []string{"Unreachable", "DependencyUnavailable", "NoWritablePrimary"} {
		t.Run(reason, func(t *testing.T) {
			calls := 0
			p := observationProvider{observe: func(context.Context, *Context) (domain.ClusterStatus, error) {
				calls++
				return domain.ClusterStatus{
					Phase:   domain.PhaseDegraded,
					Message: "Member m0 requires attention",
					Conditions: []domain.Condition{
						{Type: "Healthy", Status: "False", Reason: reason, Message: "Member m0 requires attention"},
						{Type: "Replication/rs0", Status: "Unknown", Reason: reason},
						{Type: "Storage", Status: "False", Reason: reason, Message: "Dependency unavailable"},
					},
					Members: []domain.MemberStatus{
						{Name: "m0", Agent: domain.AgentStatus{Message: reason}},
						{Name: "m1", Health: true, Agent: domain.AgentStatus{Healthy: true, LastSeen: at}},
						{Name: "m2", Health: true, Agent: domain.AgentStatus{Healthy: true, LastSeen: at}},
					},
				}, nil
			}}
			rc := &Context{Clock: observationClock{at}, Cluster: &domain.Cluster{Status: readyObservation(at.Add(-time.Second))}}
			got, err := ObserveStatus(t.Context(), p, rc)
			if err != nil || calls != 2 || got.Phase != domain.PhaseDegraded || !got.ObservedAt.Equal(at) {
				t.Fatalf("fresh degraded observation was discarded: %+v calls=%d err=%v", got, calls, err)
			}
			if got.Message != "Member m0 requires attention" || got.Members[0].Agent.Message != reason || got.Conditions[2].Reason != reason {
				t.Fatal("fresh failure diagnostics were replaced", got)
			}
			for _, member := range got.Members[1:] {
				if !member.Health || !member.Agent.Healthy || !member.Agent.LastSeen.Equal(at) {
					t.Fatal("fresh healthy peer was withdrawn", member)
				}
			}
			expired := FreshStatus(got, at.Add(ReadinessMaxAge+time.Nanosecond))
			if expired.Members[1].Health || expired.Members[1].Agent.Healthy {
				t.Fatal("preserved peer health outlived its observation lease")
			}
		})
	}
}

func TestObservationCreatingWithoutAgentsPreservesProgress(t *testing.T) {
	at := time.Now().UTC()
	p := observationProvider{observe: func(context.Context, *Context) (domain.ClusterStatus, error) {
		return domain.ClusterStatus{
			Phase: domain.PhaseCreating, Message: "Deploying member runtimes",
			Conditions: []domain.Condition{{Type: "Ready", Status: "False", Reason: "MembersStarting"}},
			Members:    []domain.MemberStatus{{Name: "m0", State: "starting"}},
		}, nil
	}}
	got, err := ObserveStatus(t.Context(), p, &Context{Clock: observationClock{at}, Cluster: &domain.Cluster{}})
	if err != nil || got.Phase != domain.PhaseCreating || got.Message != "Deploying member runtimes" || !got.ObservedAt.Equal(at) {
		t.Fatal("expected not-yet-deployed agents were treated as a failed sweep", got, err)
	}
	if got.Members[0].Health || got.Members[0].Agent.Healthy || got.Conditions[0].Status != "False" {
		t.Fatal("bootstrap observation asserted availability", got)
	}
}

func TestObservationCancellationDoesNotWaitForEngineTimeout(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	calls := 0
	p := observationProvider{observe: func(ctx context.Context, _ *Context) (domain.ClusterStatus, error) {
		calls++
		<-ctx.Done()
		return domain.ClusterStatus{}, ctx.Err()
	}}
	got, err := ObserveStatus(ctx, p, &Context{Clock: RealClock{}, Cluster: &domain.Cluster{Status: readyObservation(time.Now())}})
	if !errors.Is(err, context.Canceled) || calls != 1 || got.Phase == domain.PhaseReady {
		t.Fatal(got, err, calls)
	}
}

func TestFreshStatusLeaseUsesSampleNotTransitionTime(t *testing.T) {
	at := time.Now().UTC()
	for _, tc := range []struct {
		name   string
		sample time.Time
		ready  bool
	}{
		{"fresh", at, true},
		{"at-bound", at.Add(-ReadinessMaxAge), true},
		{"expired", at.Add(-ReadinessMaxAge - time.Nanosecond), false},
		{"legacy", time.Time{}, false},
		{"future", at.Add(time.Second), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			original := readyObservation(tc.sample)
			original.Conditions[0].LastTransitionTime = at.Add(-24 * time.Hour)
			got := FreshStatus(original, at)
			if (got.Phase == domain.PhaseReady) != tc.ready || got.ObservedGeneration != 7 {
				t.Fatalf("incorrect sample lease: %+v", got)
			}
			if original.Phase != domain.PhaseReady || !original.Members[0].Health || original.Conditions[0].Status != "True" {
				t.Fatal("projection mutated persisted snapshot")
			}
			if got.Conditions[2].Status != "True" {
				t.Fatal("readiness expiry changed unrelated certificate diagnostics")
			}
		})
	}
	updating := readyObservation(at.Add(-time.Hour))
	updating.Phase = domain.PhaseUpdating
	got := FreshStatus(updating, at)
	if got.Phase != domain.PhaseUpdating || got.Conditions[1].Status == "True" {
		t.Fatal("stale WritablePrimary survived an Updating phase", got)
	}
}

func TestFreshStatusExpiresMemberHealthInEveryActivePhase(t *testing.T) {
	at := time.Now().UTC()
	for _, phase := range []domain.Phase{domain.PhaseCreating, domain.PhaseInitializing, domain.PhaseScaling, domain.PhaseUpdating, domain.PhaseUpgrading} {
		for _, sample := range []time.Time{{}, at.Add(-ReadinessMaxAge - time.Second)} {
			status := readyObservation(sample)
			status.Phase = phase
			status.Conditions = []domain.Condition{{Type: "Ready", Status: "False", Reason: "MembersStarting"}}
			got := FreshStatus(status, at)
			if got.Phase != phase || got.Members[0].Health || got.Members[0].Agent.Healthy {
				t.Fatalf("phase %s bypassed positive member expiry: %+v", phase, got)
			}
			if !status.Members[0].Health || !status.Members[0].Agent.Healthy {
				t.Fatal("expiry mutated the persisted member")
			}
		}
	}
}

func TestFreshStatusExpiresAllCurrentAvailabilityConditions(t *testing.T) {
	status := readyObservation(time.Now().Add(-time.Hour))
	availability := []string{"Healthy", "Ready", "ClusterHealth", "Quorum", "EtcdQuorum", "Replication", "Replication/rs0", "ComponentsReady", "EtcdMembers", "Failover", "ReplicationSlots", "WALCapacity", "WALArchive", "ProxyWriter", "CredentialsPublished", "Router/router0"}
	status.Conditions = nil
	for _, name := range availability {
		status.Conditions = append(status.Conditions, domain.Condition{Type: name, Status: "True"})
	}
	for _, name := range []string{"Certificates", "Bootstrapped", "Alarms", "Partition", "DataRecoveryRequired", "IndexWriteBlocked"} {
		status.Conditions = append(status.Conditions, domain.Condition{Type: name, Status: "True"})
	}
	got := FreshStatus(status, time.Now())
	for i, name := range availability {
		if got.Conditions[i].Status != "Unknown" {
			t.Fatal("expired availability remained positive", name, got.Conditions[i])
		}
	}
	for _, condition := range got.Conditions[len(availability):len(status.Conditions)] {
		if condition.Status != "True" {
			t.Fatal("expiry erased durable or negative diagnostic", condition)
		}
	}
	if condition := got.Conditions[len(status.Conditions)]; condition.Type != "AgentsCurrent" || condition.Status != "Unknown" || condition.Reason != "Unreported" || readinessCondition(condition) {
		t.Fatal("expired agent inventory must be informational and unreported", condition)
	}
}

func TestObservationEpochRequiresSuccessfulCurrentSweep(t *testing.T) {
	at := time.Date(2026, time.October, 24, 8, 59, 59, 0, time.UTC)
	for _, outcome := range []string{"healthy", "unhealthy", "failed-sweep", "cached-fallback", "cached-previous-epoch", "cached-current-epoch"} {
		t.Run(outcome, func(t *testing.T) {
			cached := readyObservation(at)
			cached.ObservationEpoch = "previous-process"
			rc := &Context{Clock: observationClock{at}, ObservationEpoch: "current-process", Cluster: &domain.Cluster{Status: cached}}
			p := observationProvider{observe: func(context.Context, *Context) (domain.ClusterStatus, error) {
				if outcome == "cached-fallback" {
					return domain.ClusterStatus{}, errors.New("observation unavailable")
				}
				status := readyObservation(at)
				if outcome == "failed-sweep" {
					status.ObservationEpoch = "previous-process"
					return status, errors.New("observation unavailable")
				}
				if outcome == "cached-previous-epoch" {
					status.ObservationEpoch = "previous-process"
					return status, nil
				}
				if outcome == "cached-current-epoch" {
					status.ObservationEpoch = rc.ObservationEpoch
					return status, nil
				}
				if outcome == "unhealthy" {
					status.Phase = domain.PhaseDegraded
					status.Members[0].Health = false
					status.Members[0].Agent.Healthy = false
					for i := range status.Conditions {
						if readinessCondition(status.Conditions[i]) {
							status.Conditions[i].Status = "False"
						}
					}
				}
				return status, nil
			}}
			got, err := ObserveStatus(t.Context(), p, rc)
			succeeded := outcome == "healthy" || outcome == "unhealthy"
			if succeeded {
				if err != nil || got.ObservationEpoch != rc.ObservationEpoch || !got.ObservedAt.Equal(at) {
					t.Fatal("successful positive or negative sweep lost its current-process provenance", err)
				}
			} else if got.ObservationEpoch != "" || !got.ObservedAt.IsZero() {
				t.Fatal("failed/cached fallback manufactured current-process provenance")
			}
			if rc.Cluster.Status.ObservationEpoch != "previous-process" || !rc.Cluster.Status.ObservedAt.Equal(at) || !rc.Cluster.Status.Members[0].Health {
				t.Fatal("observation stamping rewrote the cached input")
			}
		})
	}
}

func TestObservationErrorsCannotReplaceLifecycleOrInventory(t *testing.T) {
	at := time.Now()
	for _, phase := range []domain.Phase{domain.PhaseReady, domain.PhaseCreating, domain.PhaseInitializing, domain.PhaseUpdating, domain.PhaseRestoring, domain.PhaseDeleting, domain.PhaseSuspended} {
		t.Run(string(phase), func(t *testing.T) {
			before := readyObservation(at)
			before.Phase = phase
			before.Members = append(before.Members, domain.MemberStatus{Name: "m1", Health: true})
			rc := &Context{Clock: observationClock{at}, Cluster: &domain.Cluster{Status: before}}
			p := observationProvider{observe: func(_ context.Context, rc *Context) (domain.ClusterStatus, error) {
				// Neither partial output nor in-place edits of provider input can
				// become a new durable observation after a failed read.
				rc.Cluster.Status.Phase = domain.PhaseCreating
				rc.Cluster.Status.Members[0].Name = "unpublished"
				return domain.ClusterStatus{Phase: domain.PhasePending,
					Conditions: []domain.Condition{{Type: "Storage", Status: "False", Reason: "ReadFailed"}}}, errors.New("platform read failed")
			}}
			got, err := ObserveStatus(t.Context(), p, rc)
			wantPhase := phase
			if phase == domain.PhaseReady {
				wantPhase = domain.PhaseDegraded
			}
			if err == nil || got.Phase != wantPhase || len(got.Members) != 2 || got.Members[0].Name != "m0" || got.Members[1].Name != "m1" {
				t.Fatalf("error replaced inspected lifecycle/inventory: %+v %v", got, err)
			}
			for _, member := range got.Members {
				if member.Health || member.Agent.Healthy {
					t.Fatal("failed observation preserved positive health", member)
				}
			}
			if got.Conditions[0].Reason != "ReadFailed" || !got.ObservedAt.IsZero() || got.ObservationEpoch != "" {
				t.Fatal("lost fresh failure diagnostic or renewed lease", got)
			}
		})
	}
}
