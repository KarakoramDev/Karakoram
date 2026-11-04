// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package reconciler

import (
	"context"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/infra"
)

type cleanupIntentProvider struct {
	*fakeProvider
	observed bool
	intent   infra.CleanupIntent
}

func (p *cleanupIntentProvider) Cleanup(ctx context.Context, _ *engine.Context, _ domain.DeleteOptions) (bool, error) {
	p.intent, p.observed = infra.CleanupIntentFromContext(ctx)
	return false, nil
}

func TestClusterDeletionSuppliesSharedCleanupObservationIntent(t *testing.T) {
	for _, keepVolumes := range []bool{false, true} {
		t.Run(map[bool]string{false: "delete", true: "suspend"}[keepVolumes], func(t *testing.T) {
			p := &cleanupIntentProvider{fakeProvider: &fakeProvider{}}
			m, st := newTestManager(t, p)
			c := &domain.Cluster{Name: "demo", Spec: domain.ClusterSpec{Provider: "fake"}}
			if err := st.CreateCluster(t.Context(), c); err != nil {
				t.Fatal(err)
			}
			if err := st.MarkClusterDeleting(t.Context(), c.Ref(), domain.DeleteOptions{KeepVolumes: keepVolumes, KeepBackups: true}); err != nil {
				t.Fatal(err)
			}
			if _, err := m.reconcile(t.Context(), c.Ref()); err != nil {
				t.Fatal(err)
			}
			if !p.observed {
				t.Fatal("shared deletion dispatch omitted cleanup observation intent")
			}
			if p.intent.FullDelete || p.intent.Services != nil {
				t.Fatal("shared observation intent must not authorize exact-ID escalated deletion")
			}
		})
	}
}
