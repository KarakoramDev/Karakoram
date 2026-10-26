// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package reconciler

import (
	"context"
	"strings"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/infra"
	"github.com/KarakoramDev/Karakoram/internal/store"
)

type stalledCleanupProvider struct{ fakeProvider }

func (*stalledCleanupProvider) Cleanup(context.Context, *engine.Context, domain.DeleteOptions) (bool, error) {
	return false, nil
}

type stalledCleanupBackend struct{ infra.Backend }

func (*stalledCleanupBackend) RuntimeStopFailures(context.Context, []string) ([]infra.RuntimeStopFailure, error) {
	return []infra.RuntimeStopFailure{{Member: "m0", Allocation: "alloc-a", Node: "node-a", Message: "allocation alloc-a node node-a stop stalled; close open nomad alloc exec stdin"}}, nil
}

func TestDeletingClusterPersistsStalledStopConditionAndSingleEvent(t *testing.T) {
	m, s := newTestManager(t, &stalledCleanupProvider{})
	b := &stalledCleanupBackend{}
	m.opts.Namespaces = engine.StaticNamespaces{DefaultBackend: b}
	c := &domain.Cluster{Name: "demo", Spec: domain.ClusterSpec{Provider: "fake"}, Status: domain.ClusterStatus{Members: []domain.MemberStatus{{Name: "m0"}}}}
	if err := s.CreateCluster(t.Context(), c); err != nil {
		t.Fatal(err)
	}
	if err := s.UpdateClusterState(t.Context(), c); err != nil {
		t.Fatal(err)
	}
	if err := s.MarkClusterDeleting(t.Context(), c.Ref(), domain.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if _, err := m.reconcile(t.Context(), c.Ref()); err != nil {
			t.Fatal(err)
		}
	}
	got, err := s.GetCluster(t.Context(), c.Ref())
	if err != nil {
		t.Fatal(err)
	}
	if got.Status.Phase != domain.PhaseDeleting || !strings.Contains(got.Status.Message, "alloc-a") {
		t.Fatalf("generic cleanup wait hid stalled allocation: %+v", got.Status)
	}
	found := false
	for _, condition := range got.Status.Conditions {
		if condition.Reason == "BackendStopStalled" && condition.Type == "RuntimeStop/alloc-a" {
			found = true
		}
	}
	events, err := s.ListEvents(t.Context(), store.EventFilter{Cluster: "demo", Limit: 100})
	if err != nil {
		t.Fatal(err)
	}
	count := 0
	for _, event := range events {
		if event.Reason == "BackendStopStalled" {
			count++
		}
	}
	if !found || count != 1 {
		t.Fatalf("missing or repeated stop diagnostics: conditions=%+v events=%d", got.Status.Conditions, count)
	}
}
