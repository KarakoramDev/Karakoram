// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package server

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine/postgres"
	"github.com/KarakoramDev/Karakoram/internal/store"
)

func TestRetainedPostgresCloneAfterSourceDeletion(t *testing.T) {
	t.Setenv("KARAKORAM_ARTIFACT_RELEASE", "v0.4.0")
	for _, topology := range []string{"single", "ha", "distributed"} {
		t.Run(topology, func(t *testing.T) {
			s, _, token := testServer(t)
			p := postgres.New(postgres.Options{Store: s.Store})
			if err := s.Registry.Register(p); err != nil {
				t.Fatal(err)
			}
			ctx := t.Context()
			if err := s.Store.CreateBackupStorage(ctx, &domain.BackupStorage{Name: "archive", Type: "s3", EndpointURL: "https://objects.example", Bucket: "backups"}); err != nil {
				t.Fatal(err)
			}
			source := &domain.Cluster{Name: "source", Spec: domain.ClusterSpec{Provider: "postgres", Topology: domain.Topology{Type: topology}}}
			if topology == "distributed" {
				source.Spec.EngineOptions = json.RawMessage(`{"citus":{"workerGroups":2}}`)
			}
			p.ApplyDefaults(&source.Spec)
			source.Spec.Backup.Storage = "archive"
			if err := s.Store.CreateCluster(ctx, source); err != nil {
				t.Fatal(err)
			}
			b := &domain.Backup{Name: "saved", Cluster: source.Name, Provider: "postgres", Type: "full", Storage: "archive", Phase: domain.PhaseSucceeded, SourceTopology: source.Spec.Topology, SourceVersion: source.Spec.Version, RecoveryIdentity: "sealed", Postgres: &domain.PostgresBackup{Repository: "archive/source-old/g0", Label: "base0"}}
			if topology == "distributed" {
				b.Postgres.RestorePoint = "kk_snapshot"
				b.Postgres.Groups = []domain.PostgresBackupGroup{
					{Group: 0, Backup: domain.PostgresBackup{Repository: b.Postgres.Repository, Label: b.Postgres.Label}, LSN: "0/100"},
					{Group: 1, Backup: domain.PostgresBackup{Repository: "archive/source-old/g1", Label: "base1"}, LSN: "0/200"},
					{Group: 2, Backup: domain.PostgresBackup{Repository: "archive/source-old/g2", Label: "base2"}, LSN: "0/300"},
				}
			}
			if err := s.Store.CreateBackup(ctx, b); err != nil {
				t.Fatal(err)
			}
			// Completed --keep-backups deletion leaves inventory, not a source spec.
			if err := s.Store.DeleteCluster(ctx, source.Ref()); err != nil {
				t.Fatal(err)
			}
			if _, err := s.Store.GetCluster(ctx, source.Ref()); err != store.ErrNotFound {
				t.Fatal("source still exists", err)
			}
			spec := map[string]any{
				"provider": "postgres", "version": source.Spec.Version,
				"topology":   source.Spec.Topology,
				"backup":     map[string]any{"storage": "archive"},
				"dataSource": map[string]any{"namespace": "default", "cluster": source.Name, "backup": b.Name},
			}
			if topology == "distributed" {
				spec["engineOptions"] = source.Spec.EngineOptions
			}
			body, err := json.Marshal(map[string]any{"name": "recovered", "spec": spec})
			if err != nil {
				t.Fatal(err)
			}
			if w := call(s, "POST", "/api/v1/clusters", string(body), token); w.Code != 201 {
				t.Fatal("valid retained pgBackRest backup rejected without Location", w.Code, w.Body.String())
			}
			// Location is display metadata, never a substitute for a recorded set.
			b.Location, b.Postgres.Label = "archive/display-only", ""
			if err := s.Store.UpdateBackup(ctx, b); err != nil {
				t.Fatal(err)
			}
			if w := call(s, "POST", "/api/v1/clusters", strings.Replace(string(body), `"recovered"`, `"unsafe"`, 1), token); w.Code != 409 || !strings.Contains(w.Body.String(), "provenance") {
				t.Fatal("unknown retained set accepted", w.Code, w.Body.String())
			}
		})
	}
}

func TestRetainedPostgresProvenanceFailsClosed(t *testing.T) {
	for _, provider := range []string{"postgres", "supabase"} {
		for _, fault := range []string{"metadata", "repository", "label", "version", "topology", "identity", "point", "groups", "group-set", "group-lsn", "duplicate", "coordinator"} {
			t.Run(provider+"/"+fault, func(t *testing.T) {
				s, _, _ := testServer(t)
				b := &domain.Backup{Name: "saved", Cluster: "gone", Provider: provider, Phase: domain.PhaseSucceeded, SourceVersion: "17.11.1", SourceTopology: domain.Topology{Type: "distributed"}, RecoveryIdentity: "sealed", Location: "not-authoritative", Postgres: &domain.PostgresBackup{Repository: "repo/g0", Label: "base", RestorePoint: "kk_point", Groups: []domain.PostgresBackupGroup{{Group: 0, Backup: domain.PostgresBackup{Repository: "repo/g0", Label: "base"}, LSN: "0/100"}, {Group: 1, Backup: domain.PostgresBackup{Repository: "repo/g1", Label: "base"}, LSN: "0/200"}}}}
				switch fault {
				case "metadata":
					b.Postgres = nil
				case "repository":
					b.Postgres.Repository = ""
				case "label":
					b.Postgres.Label = ""
				case "version":
					b.SourceVersion = ""
				case "topology":
					b.SourceTopology.Type = ""
				case "identity":
					b.RecoveryIdentity = ""
				case "point":
					b.Postgres.RestorePoint = ""
				case "groups":
					b.Postgres.Groups = nil
				case "group-set":
					b.Postgres.Groups[1].Backup.Label = ""
				case "group-lsn":
					b.Postgres.Groups[1].LSN = ""
				case "duplicate":
					b.Postgres.Groups[1].Group = 0
				case "coordinator":
					b.Postgres.Groups[0].Backup.Repository = "other"
				}
				if err := s.Store.CreateBackup(t.Context(), b); err != nil {
					t.Fatal(err)
				}
				if failure := s.retainedRestoreSource(t.Context(), domain.ClusterRef{Name: b.Cluster}, b.Name); failure == nil || failure.status != 409 {
					t.Fatal("unsafe retained provenance accepted", failure)
				}
			})
		}
	}
}
