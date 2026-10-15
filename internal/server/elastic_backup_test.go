// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package server

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/elastic"
	"github.com/KarakoramDev/Karakoram/internal/store"
)

func nativeElasticTestServer(t *testing.T) (*Server, *elastic.Provider, *domain.Cluster, string) {
	t.Helper()
	s, _, token := testServer(t)
	p := elastic.New(elastic.ProviderOptions{Store: s.Store})
	var err error
	s.Registry, err = engine.NewRegistry(&fakeProvider{}, p)
	if err != nil {
		t.Fatal(err)
	}
	c := &domain.Cluster{Name: "search", Spec: domain.ClusterSpec{Version: "8.19.23"}, Status: domain.ClusterStatus{Phase: domain.PhaseReady, Version: "8.19.23", ObservedGeneration: 1}}
	p.ApplyDefaults(&c.Spec)
	if err := s.Store.CreateCluster(t.Context(), c); err != nil {
		t.Fatal(err)
	}
	return s, p, c, token
}

func TestCreateElasticClusterExplainsDedicatedMasterTopology(t *testing.T) {
	s, p, _, token := nativeElasticTestServer(t)
	var spec domain.ClusterSpec
	p.ApplyDefaults(&spec)
	resources, _ := elastic.DefaultResources("master")
	spec.Components.Extra["master"] = domain.ComponentSpec{Replicas: 3, Resources: resources}
	body, err := json.Marshal(map[string]any{"name": "combined", "spec": specAPI(spec)})
	if err != nil {
		t.Fatal(err)
	}
	w := call(s, "POST", "/api/v1/clusters", string(body), token)
	if w.Code != 400 || !strings.Contains(w.Body.String(), `"field":"spec.components.extra.master"`) || !strings.Contains(w.Body.String(), "dedicated masters require the dedicatedMasters topology") {
		t.Fatalf("unexpected topology response: %d %s", w.Code, w.Body.String())
	}
	if _, err := s.Store.GetCluster(t.Context(), domain.ClusterRef{Name: "combined"}); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("rejected cluster persisted: %v", err)
	}
	ops, err := s.Store.ListOperations(t.Context(), domain.ClusterRef{Name: "combined"})
	if err != nil || len(ops) != 0 {
		t.Fatalf("rejected create enqueued work: %+v %v", ops, err)
	}
}

func TestElasticBackupTypesUseNativeSnapshotCatalog(t *testing.T) {
	s, _, c, token := nativeElasticTestServer(t)
	storage := domain.BackupStorage{Name: "archive", Type: "s3", Bucket: "backups", EndpointURL: "https://s3.example.com", Region: "us-east-1"}
	if err := s.Store.CreateBackupStorage(t.Context(), &storage); err != nil {
		t.Fatal(err)
	}
	w := call(s, "POST", "/api/v1/clusters/search/backups", `{"name":"logical","type":"logical","storage":"archive"}`, token)
	if w.Code != 400 || !strings.Contains(w.Body.String(), `"field":"type"`) || !strings.Contains(w.Body.String(), "supported backup types: snapshot") {
		t.Fatalf("unexpected backup diagnostic: %d %s", w.Code, w.Body.String())
	}
	backups, err := s.Store.ListBackups(t.Context(), c.Ref())
	if err != nil || len(backups) != 0 {
		t.Fatalf("unsupported backup persisted: %+v %v", backups, err)
	}
	w = call(s, "POST", "/api/v1/clusters/search/backups", `{"name":"snapshot","type":"snapshot","storage":"archive"}`, token)
	if w.Code != 202 {
		t.Fatalf("native snapshot rejected: %d %s", w.Code, w.Body.String())
	}
	backup, err := s.Store.GetBackup(t.Context(), c.Ref(), "snapshot")
	if err != nil || backup.Provider != "elasticsearch" || backup.Type != "snapshot" || backup.Phase != domain.PhasePending || w.Header().Get("Karakoram-Operation-Id") == "" {
		t.Fatalf("native snapshot not enqueued: %+v %v", backup, err)
	}
}

func TestElasticSnapshotsRequireFormedClusterAndAllowPendingRecordDeletion(t *testing.T) {
	s, _, c, token := nativeElasticTestServer(t)
	c.Status.Phase = domain.PhaseCreating
	if err := s.Store.UpdateClusterState(t.Context(), c); err != nil {
		t.Fatal(err)
	}
	w := call(s, "POST", "/api/v1/clusters/search/backups", `{"name":"early","type":"snapshot","storage":"archive"}`, token)
	for _, part := range []string{"formed, active cluster", "Creating", `"reason":"invalid_state"`, `"state":"Creating"`} {
		if w.Code != 409 || !strings.Contains(w.Body.String(), part) {
			t.Fatalf("missing creation-state explanation %q: %d %s", part, w.Code, w.Body.String())
		}
	}
	backups, err := s.Store.ListBackups(t.Context(), c.Ref())
	if err != nil || len(backups) != 0 {
		t.Fatalf("early snapshot persisted: %+v %v", backups, err)
	}
	ops, err := s.Store.ListOperations(t.Context(), c.Ref())
	if err != nil || len(ops) != 0 {
		t.Fatalf("early snapshot enqueued: %+v %v", ops, err)
	}
	pending := domain.Backup{Name: "neverstarted", Cluster: c.Name, Provider: "elasticsearch", Type: "snapshot", Phase: domain.PhasePending}
	if err := s.Store.CreateBackup(t.Context(), &pending); err != nil {
		t.Fatal(err)
	}
	w = call(s, "DELETE", "/api/v1/clusters/search/backups/neverstarted", "", token)
	if w.Code != 202 {
		t.Fatalf("pending snapshot cannot be removed: %d %s", w.Code, w.Body.String())
	}
	if _, err := s.Store.GetBackup(t.Context(), c.Ref(), pending.Name); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("never-started snapshot still exists: %v", err)
	}
	ops, err = s.Store.ListOperations(t.Context(), c.Ref())
	if err != nil || len(ops) != 1 || ops[0].Kind != domain.OperationDeleteBackup || ops[0].Phase != domain.PhaseSucceeded || ops[0].CompletedAt == nil {
		t.Fatalf("record deletion not completed: %+v %v", ops, err)
	}
}

func TestElasticMajorUpgradeRequiresSuccessfulNativeSnapshot(t *testing.T) {
	for _, method := range []string{"PUT", "PATCH"} {
		for _, tc := range []struct {
			name       string
			phase      domain.Phase
			age        time.Duration
			incomplete bool
			message    string
			status     int
		}{
			{name: "no snapshot", status: 409},
			{name: "native success", phase: domain.PhaseSucceeded, status: 200},
			{name: "failed", phase: domain.PhaseFailed, message: "Elasticsearch snapshot state FAILED", status: 409},
			{name: "partial", phase: domain.PhaseFailed, message: "Elasticsearch snapshot state PARTIAL", status: 409},
			{name: "pending", phase: domain.PhasePending, incomplete: true, status: 409},
			{name: "running", phase: domain.PhaseRunning, incomplete: true, status: 409},
			{name: "stale", phase: domain.PhaseSucceeded, age: 25 * time.Hour, status: 409},
			{name: "no completion", phase: domain.PhaseSucceeded, incomplete: true, status: 409},
		} {
			t.Run(method+"/"+tc.name, func(t *testing.T) {
				s, _, c, token := nativeElasticTestServer(t)
				if tc.phase != "" {
					completed := time.Now().UTC().Add(-tc.age)
					backup := domain.Backup{Name: "preupgrade", Cluster: c.Name, Provider: "elasticsearch", Type: "snapshot", Storage: "archive", Location: "s3://backups/search?repository=archive#preupgrade", SourceVersion: c.Spec.Version, Phase: tc.phase, CompletedAt: &completed, Error: tc.message}
					if tc.incomplete {
						backup.CompletedAt = nil
					}
					if err := s.Store.CreateBackup(t.Context(), &backup); err != nil {
						t.Fatal(err)
					}
				}
				w := call(s, method, "/api/v1/clusters/search", clusterVersionRequest(t, method, c.Spec, "9.5.5"), token)
				if w.Code != tc.status {
					t.Fatalf("upgrade status %d, want %d: %s", w.Code, tc.status, w.Body.String())
				}
				after, err := s.Store.GetCluster(t.Context(), c.Ref())
				if err != nil {
					t.Fatal(err)
				}
				ops, err := s.Store.ListOperations(t.Context(), c.Ref())
				if err != nil {
					t.Fatal(err)
				}
				if tc.status == 409 {
					for _, part := range []string{"8.19.23 → 9.5.5", "Succeeded", "24 hours", "POST /api/v1/clusters/search/backups", "retry", `"reason":"invalid_state"`} {
						if !strings.Contains(w.Body.String(), part) {
							t.Fatalf("conflict omits %q: %s", part, w.Body.String())
						}
					}
					if after.Spec.Version != c.Spec.Version || after.Generation != c.Generation || len(ops) != 0 {
						t.Fatalf("rejected upgrade changed cluster or queued work: %+v %+v", after, ops)
					}
				} else if after.Spec.Version != "9.5.5" || len(ops) != 1 || ops[0].Kind != domain.OperationUpgrade {
					t.Fatalf("snapshot did not unlock tracked upgrade: %+v %+v", after, ops)
				}
			})
		}
	}
}
