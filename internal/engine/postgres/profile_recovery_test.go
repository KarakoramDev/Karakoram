// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package postgres

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/store"
)

func TestPhysicalRecoveryProfileAdmission(t *testing.T) {
	for _, topology := range []string{"single", "ha"} {
		for _, tc := range []struct {
			name     string
			required []string
			selected string
			allowed  bool
		}{
			{"omitted", []string{"postgis", "pgvector"}, `{}`, false},
			{"partial", []string{"postgis", "pgvector"}, `{"profiles":["postgis"]}`, false},
			{"superset", []string{"pgvector"}, `{"profiles":["postgis","pgvector","postgis"]}`, true},
			{"reordered", []string{"postgis", "pgvector", "postgis"}, `{"profiles":["pgvector","postgis"]}`, true},
			{"legacy", nil, `{}`, true},
		} {
			t.Run(topology+"/"+tc.name, func(t *testing.T) {
				p, rc, _ := backupFixture(t, topology)
				rc.Cluster.Spec.EngineOptions = json.RawMessage(tc.selected)
				// No live source cluster exists: the persisted snapshot is authoritative.
				b := &domain.Backup{Namespace: "default", Cluster: "gone", Name: "base", Storage: "archive", Provider: "postgres", Phase: domain.PhaseSucceeded, SourceVersion: "17.11.1", RecoveryIdentity: "sealed", Postgres: &domain.PostgresBackup{Label: "base", Repository: "prefix/postgres/default/gone-old", Profiles: tc.required}}
				if err := rc.Store.CreateBackup(t.Context(), b); err != nil {
					t.Fatal(err)
				}
				rc.Cluster.Spec.DataSource = &domain.DataSource{Cluster: b.Cluster, Backup: b.Name}
				if err := p.validateBackups(t.Context(), nil, rc.Cluster); (err == nil) != tc.allowed {
					t.Fatalf("clone admission allowed=%v: %v", tc.allowed, err)
				}
				if _, _, err := p.resolveRestore(t.Context(), rc, restoreSource{Namespace: b.Namespace, Cluster: b.Cluster, Backup: b.Name}); (err == nil) != tc.allowed {
					t.Fatalf("restore admission allowed=%v: %v", tc.allowed, err)
				}
			})
		}
	}
}

func TestPITRCloneUsesPersistedProfileRequirements(t *testing.T) {
	p, rc, s := backupFixture(t, "single")
	start := time.Now().UTC().Add(-time.Hour)
	targetTime := start.Add(time.Minute)
	rc.Cluster.Spec.Backup.PITR.Enabled = true
	rc.Cluster.Status.Backup.PITRWindow = &domain.TimeWindow{Start: start, End: targetTime}
	// Today's selection has changed, but it is not provenance for the old base.
	rc.Cluster.Spec.EngineOptions = json.RawMessage(`{"profiles":["pgvector"]}`)
	st := rc.Store.(*store.Store)
	if err := st.UpdateClusterSpec(t.Context(), rc.Cluster.Ref(), rc.Cluster.Spec); err != nil {
		t.Fatal(err)
	}
	if err := st.UpdateClusterState(t.Context(), rc.Cluster); err != nil {
		t.Fatal(err)
	}
	storage, err := p.backupStorage(t.Context(), rc, "archive", "default")
	if err != nil {
		t.Fatal(err)
	}
	b := &domain.Backup{Namespace: "default", Cluster: rc.Cluster.Name, Name: "base", Storage: "archive", Provider: "postgres", Phase: domain.PhaseSucceeded, SourceVersion: rc.Cluster.Spec.Version, CompletedAt: &start, Postgres: &domain.PostgresBackup{Label: "base", Repository: repoPrefix(storage, rc.Cluster, s), Profiles: []string{"postgis"}}}
	if err := rc.Store.CreateBackup(t.Context(), b); err != nil {
		t.Fatal(err)
	}
	target := *rc.Cluster
	target.Name = "clone"
	target.Spec.DataSource = &domain.DataSource{Cluster: b.Cluster, PITRTime: &targetTime}
	if err := p.validateBackups(t.Context(), nil, &target); err == nil || !strings.Contains(err.Error(), "postgis") {
		t.Fatal("PITR clone ignored base profile provenance", err)
	}
	target.Spec.EngineOptions = json.RawMessage(`{"profiles":["postgis"]}`)
	if err := p.validateBackups(t.Context(), nil, &target); err != nil {
		t.Fatal("PITR clone inferred requirements from current source", err)
	}
}
