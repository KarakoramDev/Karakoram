// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package store_test

import (
	"context"
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/infra"
	"github.com/KarakoramDev/Karakoram/internal/secrets"
	"github.com/KarakoramDev/Karakoram/internal/store"
)

type failureDomainBackend struct{ infra.Backend }

func (failureDomainBackend) Capabilities() infra.Capabilities {
	return infra.Capabilities{Placement: true}
}
func (failureDomainBackend) PlacementSnapshot(context.Context) (infra.PlacementSnapshot, error) {
	return infra.PlacementSnapshot{Eligible: []string{"node-a", "node-b", "node-c"}}, nil
}

type failureDomainRecorder struct{ st *store.Store }

func (r failureDomainRecorder) Record(ctx context.Context, e domain.Event) error {
	return r.st.AppendEvent(ctx, &e)
}

func TestVotingFailureDomainsConcurrentObservationTransition(t *testing.T) {
	for _, writer := range []string{"sync", "availability", "mixed"} {
		t.Run(writer, func(t *testing.T) {
			key, err := secrets.New(strings.Repeat("m", 32))
			if err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(t.TempDir(), "state.db")
			st, err := store.Open(path, key)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { st.Close() })
			c := &domain.Cluster{Name: "demo"}
			if err := st.CreateCluster(t.Context(), c); err != nil {
				t.Fatal(err)
			}
			// All observers start from the same persisted checkpoint, just as
			// Sync and the independent availability worker can after a restart.
			observe := func(shared bool) {
				t.Helper()
				start := make(chan struct{})
				results := make(chan error, 16)
				for i := range 16 {
					before, checkpoint, err := st.GetClusterObservation(t.Context(), c.Ref())
					if err != nil {
						t.Fatal(err)
					}
					go func() {
						<-start
						status := domain.ClusterStatus{Phase: domain.PhaseReady, Members: []domain.MemberStatus{
							{Name: "m0", PlacementGroup: "node-a"},
							{Name: "m1", PlacementGroup: "node-b"},
							{Name: "m2", PlacementGroup: "node-c"},
						}}
						if shared {
							status.Members[2].PlacementGroup = "node-b"
						}
						rc := &engine.Context{Cluster: before, Backend: failureDomainBackend{}, Events: failureDomainRecorder{st}}
						err := engine.ObserveFailureDomains(t.Context(), rc, &status, []string{"m0", "m1", "m2"})
						if err == nil {
							if writer == "availability" || (writer == "mixed" && i%2 == 0) {
								_, err = st.UpdateClusterObservation(t.Context(), before, checkpoint, status)
							} else {
								before.Status = status
								err = st.UpdateClusterState(t.Context(), before)
							}
						}
						results <- err
					}()
				}
				close(start)
				for range 16 {
					if err := <-results; err != nil {
						t.Fatal(err)
					}
				}
			}
			assertWarnings := func(want int) {
				t.Helper()
				events, err := st.ListEvents(t.Context(), store.EventFilter{Cluster: c.Name})
				if err != nil {
					t.Fatal(err)
				}
				got := 0
				for _, event := range events {
					if event.Type == "Warning" && event.Reason == "SharedNode" {
						got++
					}
				}
				if got != want {
					t.Fatalf("SharedNode warning count = %d, want %d", got, want)
				}
			}
			observe(true)
			assertWarnings(1)
			if err := st.Close(); err != nil {
				t.Fatal(err)
			}
			st, err = store.Open(path, key)
			if err != nil {
				t.Fatal(err)
			}
			observe(true)
			assertWarnings(1)
			observe(false)
			assertWarnings(1)
			observe(true)
			assertWarnings(2)
		})
	}
}

func TestVotingFailureDomainsRejectedPublicationDoesNotEmit(t *testing.T) {
	for _, writer := range []string{"sync", "availability"} {
		t.Run(writer, func(t *testing.T) {
			key, err := secrets.New(strings.Repeat("m", 32))
			if err != nil {
				t.Fatal(err)
			}
			st, err := store.Open(filepath.Join(t.TempDir(), "state.db"), key)
			if err != nil {
				t.Fatal(err)
			}
			defer st.Close()
			c := &domain.Cluster{Name: "demo"}
			if err := st.CreateCluster(t.Context(), c); err != nil {
				t.Fatal(err)
			}
			before, checkpoint, err := st.GetClusterObservation(t.Context(), c.Ref())
			if err != nil {
				t.Fatal(err)
			}
			status := domain.ClusterStatus{Members: []domain.MemberStatus{
				{Name: "m0", PlacementGroup: "node-a"},
				{Name: "m1", PlacementGroup: "node-b"},
				{Name: "m2", PlacementGroup: "node-b"},
			}}
			rc := &engine.Context{Cluster: before, Backend: failureDomainBackend{}, Events: failureDomainRecorder{st}}
			if err := engine.ObserveFailureDomains(t.Context(), rc, &status, []string{"m0", "m1", "m2"}); err != nil {
				t.Fatal(err)
			}
			if writer == "availability" {
				// Invalidate the observed checkpoint before publication.
				c.Status.Message = "new lifecycle checkpoint"
				if err := st.UpdateClusterState(t.Context(), c); err != nil {
					t.Fatal(err)
				}
				if published, err := st.UpdateClusterObservation(t.Context(), before, checkpoint, status); err != nil || published {
					t.Fatalf("stale observation published: %v, %v", published, err)
				}
			} else {
				// Force failure after transition recording, proving rollback
				// covers the event as well as the cluster update.
				before.Status = status
				before.Internal = json.RawMessage(`invalid`)
				if err := st.UpdateClusterState(t.Context(), before); err == nil {
					t.Fatal("invalid state unexpectedly published")
				}
			}
			events, err := st.ListEvents(t.Context(), store.EventFilter{Cluster: c.Name})
			if err != nil {
				t.Fatal(err)
			}
			for _, event := range events {
				if event.Reason == "SharedNode" {
					t.Fatal("rejected publication emitted a warning", event)
				}
			}
		})
	}
}
