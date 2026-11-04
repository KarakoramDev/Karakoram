// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package server

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/api"
	"github.com/KarakoramDev/Karakoram/internal/domain"
)

func TestRestorePointSelectorValidation(t *testing.T) {
	s, _, token := testServer(t)
	for _, source := range []string{
		`{"backup":"base","restorePoint":"point"}`,
		`{"pitrTime":"2025-01-15T12:00:00Z","restorePoint":"point"}`,
		`{"backup":"base","pitrTime":"2025-01-15T12:00:00Z","restorePoint":"point"}`,
	} {
		requireDependencyError(t, call(s, "POST", "/api/v1/clusters/prod/restores", `{"name":"recover","source":`+source+`}`, token), 400, "invalid_request", map[string]string{"field": "source", "reason": "conflict"})
	}
	requireDependencyError(t, call(s, "POST", "/api/v1/clusters/prod/restores", `{"name":"recover","source":{"restorePoint":"point"}}`, token), 400, "invalid_request", map[string]string{"field": "source.restorePoint", "reason": "unsupported"})
}

func restorePointDependencyFixture(t *testing.T) (*Server, string) {
	t.Helper()
	s, _, token := testServer(t)
	if err := s.Registry.Register(&restoreTestProvider{fakeProvider: &fakeProvider{}, name: "postgres"}); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"prod", "dev"} {
		ref := domain.ClusterRef{Name: name}
		c, err := s.Store.GetCluster(t.Context(), ref)
		if err != nil {
			t.Fatal(err)
		}
		c.Spec.Provider = "postgres"
		c.Spec.Version = "17.11.1"
		c.Spec.Topology.Type = "distributed"
		if err := s.Store.UpdateClusterSpec(t.Context(), ref, c.Spec); err != nil {
			t.Fatal(err)
		}
	}
	backup := domain.Backup{Name: "coordinated", Cluster: "prod", Provider: "postgres", Type: "full", Phase: domain.PhaseSucceeded, Postgres: &domain.PostgresBackup{Label: "coordinator-base", Repository: "repository/g0", RestorePoint: "checkpoint"}}
	if err := s.Store.CreateBackup(t.Context(), &backup); err != nil {
		t.Fatal(err)
	}
	return s, token
}

func TestRestorePointPinsBackingBackupDependency(t *testing.T) {
	s, token := restorePointDependencyFixture(t)
	body := `{"name":"restore-point","source":{"sourceCluster":"prod","restorePoint":"checkpoint"}}`
	preview := previewBody(t, call(s, "POST", "/api/v1/dry-run/clusters/dev/restores", body, token))
	if preview.Restore == nil || preview.Restore.Source.Backup != nil || value(preview.Restore.Source.RestorePoint) != "checkpoint" {
		t.Fatalf("preview changed the requested restore-point selector: %+v", preview.Restore)
	}
	created := call(s, "POST", "/api/v1/clusters/dev/restores", body, token)
	if created.Code != 202 {
		t.Fatalf("restore rejected: %d %s", created.Code, created.Body.String())
	}
	var result api.Restore
	if err := json.Unmarshal(created.Body.Bytes(), &result); err != nil {
		t.Fatal(err)
	}
	if result.Source.Backup != nil || value(result.Source.RestorePoint) != "checkpoint" {
		t.Fatalf("create response changed the requested restore-point selector: %+v", result.Source)
	}
	restore, err := s.Store.GetRestore(t.Context(), domain.ClusterRef{Name: "dev"}, "restore-point")
	if err != nil || restore.Source.Backup != "coordinated" || restore.Source.RestorePoint != "checkpoint" || restore.Phase != domain.PhasePending {
		t.Fatalf("pending restore lost its backing inventory dependency: %+v %v", restore, err)
	}
	for _, path := range []string{"/api/v1/clusters/prod/backups/coordinated", "/api/v1/clusters/prod"} {
		w := call(s, "DELETE", path, "", token)
		if w.Code != 409 || !strings.Contains(w.Body.String(), restore.OperationID) {
			t.Fatalf("%s deletion ignored pending named-point restore: %d %s", path, w.Code, w.Body.String())
		}
	}
	source, err := s.Store.GetCluster(t.Context(), domain.ClusterRef{Name: "prod"})
	if err != nil || source.Deleting {
		t.Fatalf("rejected source deletion persisted: %+v %v", source, err)
	}
	backup, err := s.Store.GetBackup(t.Context(), source.Ref(), "coordinated")
	if err != nil || backup.Phase != domain.PhaseSucceeded {
		t.Fatalf("rejected backup deletion persisted: %+v %v", backup, err)
	}
}

func TestRestorePointDryRunMatchesDestructiveSourceAdmission(t *testing.T) {
	for _, path := range []string{"/api/v1/clusters/prod/backups/coordinated", "/api/v1/clusters/prod"} {
		t.Run(path, func(t *testing.T) {
			s, token := restorePointDependencyFixture(t)
			if w := call(s, "DELETE", path, "", token); w.Code != 202 {
				t.Fatalf("source deletion intent rejected: %d %s", w.Code, w.Body.String())
			}
			body := `{"name":"restore-point","source":{"sourceCluster":"prod","restorePoint":"checkpoint"}}`
			dry := call(s, "POST", "/api/v1/dry-run/clusters/dev/restores", body, token)
			real := call(s, "POST", "/api/v1/clusters/dev/restores", body, token)
			if dry.Code < 400 || dry.Code != real.Code || dry.Body.String() != real.Body.String() {
				t.Fatalf("named-point admission differs: dry=%d %s real=%d %s", dry.Code, dry.Body.String(), real.Code, real.Body.String())
			}
		})
	}
}
