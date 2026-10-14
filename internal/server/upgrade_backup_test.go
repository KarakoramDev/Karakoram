// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package server

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
)

type backupRequiredProvider struct {
	fakeProvider
}

func (*backupRequiredProvider) Describe() domain.ProviderInfo {
	info := (&fakeProvider{}).Describe()
	info.UpgradePaths = []domain.UpgradePath{
		{From: "8.0", To: "9.0", RequiresBackup: true},
		{From: "8.0", To: "8.1"},
		{From: "7.0", To: "8.2", RequiresBackup: true},
	}
	return info
}

func TestClusterVersionChangeRequiresCatalogBackup(t *testing.T) {
	for _, method := range []string{"PUT", "PATCH"} {
		for _, tc := range []struct {
			name       string
			version    string
			phase      domain.Phase
			age        time.Duration
			incomplete bool
			cluster    string
			status     int
		}{
			{name: "no backup", version: "9.0", status: 409},
			{name: "old backup", version: "9.0", phase: domain.PhaseSucceeded, age: 25 * time.Hour, status: 409},
			{name: "fresh succeeded backup", version: "9.0", phase: domain.PhaseSucceeded, status: 200},
			{name: "failed backup", version: "9.0", phase: domain.PhaseFailed, status: 409},
			{name: "running backup", version: "9.0", phase: domain.PhaseRunning, status: 409},
			{name: "pending backup", version: "9.0", phase: domain.PhasePending, status: 409},
			{name: "cancelled backup", version: "9.0", phase: domain.PhaseCancelled, status: 409},
			{name: "missing completion", version: "9.0", phase: domain.PhaseSucceeded, incomplete: true, status: 409},
			{name: "future completion", version: "9.0", phase: domain.PhaseSucceeded, age: -time.Hour, status: 409},
			{name: "other cluster backup", version: "9.0", phase: domain.PhaseSucceeded, cluster: "dev", status: 409},
			{name: "path without requirement", version: "8.1", status: 200},
			{name: "different source path", version: "8.2", status: 200},
			{name: "unchanged version", version: "8.0", status: 200},
		} {
			t.Run(method+"/"+tc.name, func(t *testing.T) {
				s, _, token := testServer(t)
				var err error
				s.Registry, err = engine.NewRegistry(&backupRequiredProvider{})
				if err != nil {
					t.Fatal(err)
				}
				ref := domain.ClusterRef{Name: "prod"}
				before, err := s.Store.GetCluster(t.Context(), ref)
				if err != nil {
					t.Fatal(err)
				}
				if tc.phase != "" {
					completed := time.Now().UTC().Add(-tc.age)
					backup := domain.Backup{Name: "preupgrade", Cluster: "prod", Phase: tc.phase, CompletedAt: &completed}
					if tc.cluster != "" {
						backup.Cluster = tc.cluster
					}
					if tc.incomplete {
						backup.CompletedAt = nil
					}
					if err := s.Store.CreateBackup(t.Context(), &backup); err != nil {
						t.Fatal(err)
					}
				}
				body := clusterVersionRequest(t, method, before.Spec, tc.version)
				w := call(s, method, "/api/v1/clusters/prod", body, token)
				if w.Code != tc.status {
					t.Fatalf("status %d, want %d: %s", w.Code, tc.status, w.Body.String())
				}
				if tc.status == 409 {
					assertUpgradeBackupConflict(t, w.Body.Bytes())
					after, err := s.Store.GetCluster(t.Context(), ref)
					if err != nil || after.Spec.Version != before.Spec.Version || after.Generation != before.Generation {
						t.Fatalf("rejected change persisted: %+v, %v", after, err)
					}
					ops, err := s.Store.ListOperations(t.Context(), ref)
					if err != nil || len(ops) != 0 {
						t.Fatalf("rejected change enqueued: %+v, %v", ops, err)
					}
					if calls := s.Trigger.(*triggerSpy).names; len(calls) != 0 {
						t.Fatalf("rejected change triggered reconciliation: %+v", calls)
					}
				} else {
					after, err := s.Store.GetCluster(t.Context(), ref)
					if err != nil || after.Spec.Version != tc.version {
						t.Fatalf("accepted version not persisted: %+v, %v", after, err)
					}
					if tc.version != before.Spec.Version {
						ops, err := s.Store.ListOperations(t.Context(), ref)
						if err != nil || len(ops) != 1 || ops[0].Kind != domain.OperationUpgrade || w.Header().Get("Karakoram-Operation-Id") != ops[0].ID {
							t.Fatalf("upgrade not tracked: %+v, %v", ops, err)
						}
					}
				}
			})
		}
	}
}

func TestUpgradeBackupFreshnessWindow(t *testing.T) {
	for _, age := range []time.Duration{23 * time.Hour, 25 * time.Hour} {
		t.Run(age.String(), func(t *testing.T) {
			s, _, _ := testServer(t)
			c, err := s.Store.GetCluster(t.Context(), domain.ClusterRef{Name: "prod"})
			if err != nil {
				t.Fatal(err)
			}
			// Model an older cluster to isolate the freshness cutoff from creation.
			c.CreatedAt = time.Now().Add(-72 * time.Hour)
			completed := time.Now().Add(-age)
			if err := s.Store.CreateBackup(t.Context(), &domain.Backup{Name: "preupgrade", Cluster: c.Name, Phase: domain.PhaseSucceeded, CompletedAt: &completed}); err != nil {
				t.Fatal(err)
			}
			err = s.validateUpgradeBackup(t.Context(), c, "9.0", &backupRequiredProvider{})
			if (err != nil) != (age > upgradeBackupMaxAge) {
				t.Fatalf("backup age %s: %v", age, err)
			}
		})
	}
}

func TestUpgradeBackupMustFollowKnownVersionConvergence(t *testing.T) {
	for _, method := range []string{"PUT", "PATCH"} {
		for _, kind := range []string{domain.OperationCreate, domain.OperationUpgrade, domain.OperationUpdate} {
			for _, phase := range []domain.Phase{domain.PhaseSucceeded, domain.PhaseFailed} {
				t.Run(method+"/"+kind+"/"+string(phase), func(t *testing.T) {
					s, _, token := testServer(t)
					var err error
					s.Registry, err = engine.NewRegistry(&backupRequiredProvider{})
					if err != nil {
						t.Fatal(err)
					}
					ref := domain.ClusterRef{Name: "prod"}
					c, err := s.Store.GetCluster(t.Context(), ref)
					if err != nil {
						t.Fatal(err)
					}
					c.Status.Version = c.Spec.Version
					if err := s.Store.UpdateClusterState(t.Context(), c); err != nil {
						t.Fatal(err)
					}
					backupCompleted := time.Now().UTC()
					if err := s.Store.CreateBackup(t.Context(), &domain.Backup{Name: "beforeversion", Cluster: c.Name, Phase: domain.PhaseSucceeded, CompletedAt: &backupCompleted}); err != nil {
						t.Fatal(err)
					}
					versionCompleted := time.Now().UTC()
					op := domain.Operation{Cluster: c.Name, Kind: kind, Phase: phase, TargetGeneration: c.Generation, CompletedAt: &versionCompleted}
					if err := s.Store.EnqueueOperation(t.Context(), &op); err != nil {
						t.Fatal(err)
					}
					body := clusterVersionRequest(t, method, c.Spec, "9.0")
					w := call(s, method, "/api/v1/clusters/prod", body, token)
					if phase != domain.PhaseSucceeded || kind == domain.OperationUpdate {
						if w.Code != 200 {
							t.Fatalf("unrelated or failed convergence invalidated backup: %d %s", w.Code, w.Body.String())
						}
						return
					}
					if w.Code != 409 {
						t.Fatalf("backup before version convergence accepted: %d %s", w.Code, w.Body.String())
					}
					assertUpgradeBackupConflict(t, w.Body.Bytes())
					// The earlier Succeeded record must not hide a qualifying newer backup.
					afterCompleted := time.Now().UTC()
					if err := s.Store.CreateBackup(t.Context(), &domain.Backup{Name: "afterversion", Cluster: c.Name, Phase: domain.PhaseSucceeded, CompletedAt: &afterCompleted}); err != nil {
						t.Fatal(err)
					}
					w = call(s, method, "/api/v1/clusters/prod", body, token)
					if w.Code != 200 {
						t.Fatalf("backup after version convergence rejected: %d %s", w.Code, w.Body.String())
					}
				})
			}
		}
	}
}

func clusterVersionRequest(t *testing.T, method string, spec domain.ClusterSpec, version string) string {
	t.Helper()
	var body any = map[string]any{"spec": map[string]any{"version": version}}
	if method == "PUT" {
		spec.Version = version
		body = map[string]any{"spec": specAPI(spec)}
	}
	data, err := json.Marshal(body)
	if err != nil {
		t.Fatal(err)
	}
	return string(data)
}

func assertUpgradeBackupConflict(t *testing.T, body []byte) {
	t.Helper()
	var got struct {
		Code    string         `json:"code"`
		Message string         `json:"message"`
		Details map[string]any `json:"details"`
	}
	if err := json.Unmarshal(body, &got); err != nil {
		t.Fatal(err)
	}
	if got.Code != "conflict" || got.Details["resource"] != "cluster" || got.Details["name"] != "prod" || got.Details["reason"] != "invalid_state" {
		t.Fatalf("unexpected conflict contract: %s", body)
	}
	for _, part := range []string{"8.0 → 9.0", "Succeeded", "24 hours", "POST /api/v1/clusters/prod/backups", "retry"} {
		if !strings.Contains(got.Message, part) {
			t.Fatalf("conflict omits %q: %s", part, body)
		}
	}
}
