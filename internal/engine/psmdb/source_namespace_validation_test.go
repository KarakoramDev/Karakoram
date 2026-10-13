// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package psmdb

import (
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/store"
)

func TestValidateNormalizesLegacyDataSourceIdentity(t *testing.T) {
	for _, namespace := range []string{"", "default", "production"} {
		t.Run(namespace, func(t *testing.T) {
			p, rc, _, _ := setup(t)
			st := rc.Store.(*store.Store)
			backup := domain.Backup{Namespace: namespace, Cluster: "source", Name: "saved", Phase: domain.PhaseSucceeded, SourceVersion: rc.Cluster.Spec.Version, SourceTopology: rc.Cluster.Spec.Topology}
			if err := st.CreateBackup(t.Context(), &backup); err != nil {
				t.Fatal(err)
			}
			old := *rc.Cluster
			old.Namespace = namespace
			old.Spec.DataSource = &domain.DataSource{Cluster: "source", Backup: "saved"}
			next := old
			source := *old.Spec.DataSource
			source.Namespace = domain.NormalizedNamespace(namespace)
			next.Spec.DataSource = &source
			next.Spec.Components.Engine.Replicas = 3
			if err := p.Validate(t.Context(), &old, &next); err != nil {
				t.Fatal(err)
			}
			if old.Spec.DataSource.Namespace != "" {
				t.Fatal("validation mutated persisted source")
			}
			source.Namespace = "other"
			if err := p.Validate(t.Context(), &old, &next); err == nil {
				t.Fatal("source namespace change was accepted")
			}
		})
	}
}
