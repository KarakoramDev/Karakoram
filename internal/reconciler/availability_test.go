// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package reconciler

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
)

type blockedSyncProvider struct {
	*fakeProvider
	started      chan struct{}
	startOnce    sync.Once
	unavailable  atomic.Bool
	observations atomic.Int32
}

func (p *blockedSyncProvider) Sync(ctx context.Context, rc *engine.Context) (engine.Result, error) {
	// This state is not checkpointed. The observer must load its own durable
	// snapshot, not copy this concurrently mutated lifecycle context.
	rc.Cluster.Internal = json.RawMessage(`{"sync":"unpublished"}`)
	p.startOnce.Do(func() { close(p.started) })
	select {
	case <-ctx.Done():
		return engine.Result{}, ctx.Err()
	case <-time.After(20 * time.Second):
	}
	return engine.Result{RequeueAfter: time.Minute}, nil
}

func (p *blockedSyncProvider) Status(_ context.Context, rc *engine.Context) (domain.ClusterStatus, error) {
	p.observations.Add(1)
	if p.unavailable.Load() {
		return domain.ClusterStatus{}, errors.New("agent unreachable")
	}
	if rc.AvailabilityOnly {
		if string(rc.Cluster.Internal) != `{"checkpoint":true}` {
			return domain.ClusterStatus{}, errors.New("observer inherited uncheckpointed Sync state")
		}
		// Status may calculate provider drift. An independent availability
		// observer must never save that provider-owned state over Sync.
		rc.Cluster.Internal = json.RawMessage(`{"observation":"must-not-persist"}`)
	}
	return domain.ClusterStatus{
		Phase:      domain.PhaseReady,
		Conditions: []domain.Condition{{Type: "Ready", Status: "True", Reason: "MembersReady"}},
		Members:    []domain.MemberStatus{{Name: "member", Health: true, Agent: domain.AgentStatus{Healthy: true, LastSeen: rc.Clock.Now()}}},
	}, nil
}

func TestAvailabilityRemainsIndependentOfTwentySecondSync(t *testing.T) {
	for _, dead := range []bool{false, true} {
		t.Run(map[bool]string{false: "healthy", true: "dead-agent"}[dead], func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				p := &blockedSyncProvider{fakeProvider: &fakeProvider{}, started: make(chan struct{})}
				m, st := newTestManager(t, p)
				m.opts.Context.ObservationEpoch = "current-process"
				c := &domain.Cluster{Name: "demo", Spec: domain.ClusterSpec{Provider: "fake"},
					Internal: json.RawMessage(`{"checkpoint":true}`),
					Status:   domain.ClusterStatus{Phase: domain.PhaseReady, ObservedGeneration: 1}}
				if err := st.CreateCluster(t.Context(), c); err != nil {
					t.Fatal(err)
				}
				ctx, cancel := context.WithCancel(t.Context())
				w := &worker{trigger: make(chan struct{}, 1)}
				m.wg.Add(1)
				go m.work(ctx, c.Ref(), w)
				defer func() { cancel(); m.wg.Wait() }()
				<-p.started
				p.unavailable.Store(dead)
				for second := 1; second <= 19; second++ {
					time.Sleep(time.Second)
					synctest.Wait()
					stored, err := st.GetCluster(t.Context(), c.Ref())
					if err != nil {
						t.Fatal(err)
					}
					fresh := engine.FreshStatus(stored.Status, m.opts.Context.Clock.Now())
					if !dead && (fresh.Phase != domain.PhaseReady || !fresh.Members[0].Health || !fresh.Members[0].Agent.Healthy || fresh.ObservationEpoch != "current-process") {
						t.Fatalf("healthy availability starved at %ds: %+v", second, fresh)
					}
					if dead && second > int(engine.ReadinessMaxAge/time.Second) && (fresh.Phase == domain.PhaseReady || !fresh.ObservedAt.IsZero() || fresh.Members[0].Agent.Healthy) {
						t.Fatalf("unreachable agent renewed its lease at %ds: %+v", second, fresh)
					}
					if string(stored.Internal) != `{"checkpoint":true}` || stored.Status.ObservedGeneration != 1 {
						t.Fatal("availability changed lifecycle checkpoint or applied generation", stored)
					}
				}
				if p.observations.Load() < 4 {
					t.Fatal("independent observations did not run while Sync was blocked")
				}
				// Let the twenty-second Sync finish. Its final checkpoint, not the
				// observer's provider-local drift, must remain authoritative.
				time.Sleep(2 * time.Second)
				synctest.Wait()
				stored, err := st.GetCluster(t.Context(), c.Ref())
				if err != nil || string(stored.Internal) != `{"sync":"unpublished"}` {
					t.Fatal("Sync checkpoint was overwritten by availability", stored, err)
				}
			})
		})
	}
}

func TestStageTimingUsesMonotonicDurationAndSanitizedFields(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		m, _ := newTestManager(t, &fakeProvider{})
		var logs bytes.Buffer
		m.opts.Context.Logger = slog.New(slog.NewJSONHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug}))
		ref := domain.ClusterRef{Name: "demo"}
		m.logStage(ref, "provider.sync", time.Now())
		if !strings.Contains(logs.String(), `"level":"DEBUG"`) || !strings.Contains(logs.String(), `"stage":"provider.sync"`) {
			t.Fatal("short stages lack debug attribution", logs.String())
		}
		logs.Reset()
		started := time.Now()
		time.Sleep(20 * time.Second)
		m.logStage(ref, "provider.sync", started)
		var record map[string]any
		if err := json.Unmarshal(logs.Bytes(), &record); err != nil {
			t.Fatal(err)
		}
		if record["level"] != "WARN" || record["duration"] != float64(20*time.Second) || record["stage"] != "provider.sync" {
			t.Fatal("long stage did not report monotonic lease overrun", record)
		}
		for key := range record {
			switch key {
			case "time", "level", "msg", "cluster", "stage", "duration":
			default:
				t.Fatal("stage timing exposed non-timing data", key)
			}
		}
	})
}

func TestIndependentAvailabilityRetainsLifecycleOwnership(t *testing.T) {
	for _, scenario := range []string{"creating", "restoring", "suspended", "deleting", "running-operation", "operation-starts-during-probe", "missing-namespace"} {
		t.Run(scenario, func(t *testing.T) {
			p := &operationProvider{fakeProvider: &fakeProvider{}}
			m, st := newTestManager(t, p)
			c := &domain.Cluster{Name: "demo", Spec: domain.ClusterSpec{Provider: "fake"}, Internal: json.RawMessage(`{"checkpoint":true}`),
				Status: domain.ClusterStatus{Phase: domain.PhaseReady, ObservedGeneration: 1}}
			switch scenario {
			case "creating":
				c.Status.Phase = domain.PhaseCreating
			case "restoring":
				c.Status.Phase = domain.PhaseRestoring
			case "suspended":
				c.Status.Phase = domain.PhaseSuspended
			case "deleting":
				c.Deleting = true
			case "missing-namespace":
				c.Namespace = "broken"
				if err := st.CreateNamespace(t.Context(), &domain.Namespace{Name: c.Namespace, Phase: "Ready"}); err != nil {
					t.Fatal(err)
				}
			}
			if err := st.CreateCluster(t.Context(), c); err != nil {
				t.Fatal(err)
			}
			if scenario == "running-operation" {
				if err := st.EnqueueOperation(t.Context(), &domain.Operation{Cluster: c.Name, Kind: domain.OperationRestore, Phase: domain.PhaseRunning}); err != nil {
					t.Fatal(err)
				}
			}
			before, err := st.GetCluster(t.Context(), c.Ref())
			if err != nil {
				t.Fatal(err)
			}
			probes := 0
			p.statusFn = func(*engine.Context) (domain.ClusterStatus, error) {
				probes++
				if scenario != "operation-starts-during-probe" {
					t.Fatal("independent observation bypassed a lifecycle/dependency fence")
				}
				if err := st.EnqueueOperation(t.Context(), &domain.Operation{Cluster: c.Name, Kind: domain.OperationRestore, Phase: domain.PhaseRunning}); err != nil {
					t.Fatal(err)
				}
				return domain.ClusterStatus{Phase: domain.PhaseReady}, nil
			}
			err = m.renewAvailability(t.Context(), c.Ref())
			if (err != nil) != (scenario == "missing-namespace") {
				t.Fatal("unexpected independent observation error", err)
			}
			if probes != map[bool]int{false: 0, true: 1}[scenario == "operation-starts-during-probe"] {
				t.Fatal("unexpected probe count", probes)
			}
			after, err := st.GetCluster(t.Context(), c.Ref())
			if err != nil || after.Status.Phase != before.Status.Phase || !after.UpdatedAt.Equal(before.UpdatedAt) || string(after.Internal) != string(before.Internal) || !after.Status.ObservedAt.IsZero() {
				t.Fatal("independent observer changed lifecycle-owned state", after, err)
			}
		})
	}
}
