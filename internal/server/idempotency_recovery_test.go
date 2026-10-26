// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package server

import (
	"context"
	"database/sql"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/api"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/store"
)

func TestIdempotencyReplaysCommittedOperationAfterCrash(t *testing.T) {
	for _, fault := range []string{"crash", "response-write-failure"} {
		t.Run(fault, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "state.db")
			s, sealer, token := testServerWithDatabase(t, path)
			db, err := sql.Open("sqlite", "file:"+path)
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			if fault == "response-write-failure" {
				if _, err = db.Exec("CREATE TRIGGER fail_final_response BEFORE UPDATE OF response ON idempotency_requests WHEN OLD.response<>'' BEGIN SELECT RAISE(FAIL,'response persistence unavailable'); END"); err != nil {
					t.Fatal(err)
				}
				first := idempotentCall(s, "POST", "/api/v1/clusters/prod/restart", "", token, "committed")
				if first.Code != 500 {
					t.Fatalf("missing response write fault: %d %s", first.Code, first.Body.String())
				}
				if _, err = db.Exec("DROP TRIGGER fail_final_response"); err != nil {
					t.Fatal(err)
				}
			} else {
				r := httptest.NewRequest("POST", "/api/v1/clusters/prod/restart", nil)
				r.Header.Set("Authorization", "Bearer "+token)
				r.Header.Set("Idempotency-Key", "committed")
				r.Pattern = "POST /api/v1/clusters/{cluster}/restart"
				w := httptest.NewRecorder()
				r, _ = s.captureIdempotencyBody(w, r)
				func() {
					defer func() {
						if recover() == nil {
							t.Error("crash hook did not run")
						}
					}()
					_, _ = s.middleware(func(ctx context.Context, _ http.ResponseWriter, _ *http.Request, _ any) (any, error) {
						op := &domain.Operation{Cluster: "prod", Kind: domain.OperationRestart}
						if err := s.Store.EnqueueOperation(ctx, op); err != nil {
							t.Fatal(err)
						}
						panic("lost response after committed mutation")
					}, "RestartCluster")(r.Context(), w, r, api.RestartClusterRequestObject{Cluster: "prod"})
				}()
			}
			// Expire only abandoned leases, not the successful recovery response.
			if err = s.Store.PurgeIdempotency(t.Context(), time.Now().Add(2*store.IdempotencyLease)); err != nil {
				t.Fatal(err)
			}
			body := ""
			reopened, err := store.Open(path, sealer)
			if err != nil {
				t.Fatal(err)
			}
			defer reopened.Close()
			restarted := *s
			restarted.Store = reopened
			restarted.Context.Store = reopened
			restarted.Context.Secrets = reopened
			registerConformance(t, &restarted)
			replay := idempotentCall(&restarted, "POST", "/api/v1/clusters/prod/restart", body, token, "committed")
			if replay.Code != 202 || replay.Header().Get("Idempotent-Replayed") != "true" {
				t.Fatalf("not replayed: %d %s", replay.Code, replay.Body.String())
			}
			ops, err := restarted.Store.ListOperations(t.Context(), domain.ClusterRef{Name: "prod"})
			if err != nil {
				t.Fatal(err)
			}
			count := 0
			for _, op := range ops {
				if op.Kind == domain.OperationRestart {
					count++
					if replay.Header().Get("Karakoram-Operation-Id") != op.ID {
						t.Fatal("changed accepted operation")
					}
				}
			}
			if count != 1 {
				t.Fatalf("imperative mutation ran %d times", count)
			}
		})
	}
}

func TestIdempotencyRecoveryPreservesResourceResponseShapes(t *testing.T) {
	for _, tc := range []struct {
		name, method, path, body string
		status                   int
	}{
		{"create", "POST", "/api/v1/clusters", `{"name":"accepted","spec":{"provider":"fake","version":"8.0","topology":{"type":"replicaSet"}}}`, 201},
		{"patch", "PATCH", "/api/v1/clusters/prod", `{"spec":{"engineConfig":"changed"}}`, 200},
		{"backup", "POST", "/api/v1/clusters/prod/backups", `{"name":"daily","type":"logical","storage":"archive"}`, 202},
		{"restore", "POST", "/api/v1/clusters/dev/restores", `{"name":"recovery","source":{"backup":"sourcebackup","sourceCluster":"dev"}}`, 202},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "state.db")
			s, _, token := testServerWithDatabase(t, path)
			if err := s.Store.CreateBackupStorage(t.Context(), &domain.BackupStorage{Name: "archive", Type: "s3", EndpointURL: "https://storage.example", Region: "region", Bucket: "backups", VerifyTLS: true}); err != nil {
				t.Fatal(err)
			}
			if err := s.Store.CreateBackup(t.Context(), &domain.Backup{Name: "sourcebackup", Cluster: "dev", Provider: "fake", Storage: "archive", Type: "logical", Phase: domain.PhaseSucceeded, SourceVersion: "8.0", SourceTopology: domain.Topology{Type: "replicaSet"}}); err != nil {
				t.Fatal(err)
			}
			db, err := sql.Open("sqlite", "file:"+path)
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			if _, err = db.Exec("CREATE TRIGGER fail_final_response BEFORE UPDATE OF response ON idempotency_requests WHEN OLD.response<>'' BEGIN SELECT RAISE(FAIL,'response persistence unavailable'); END"); err != nil {
				t.Fatal(err)
			}
			first := idempotentCall(s, tc.method, tc.path, tc.body, token, "accepted")
			if first.Code != 500 {
				t.Fatalf("mutation did not reach final response fault: %d %s", first.Code, first.Body.String())
			}
			replayed := idempotentCall(s, tc.method, tc.path, tc.body, token, "accepted")
			if replayed.Code != tc.status || replayed.Header().Get("Idempotent-Replayed") != "true" || replayed.Header().Get("Karakoram-Operation-Id") == "" {
				t.Fatal(replayed.Code, replayed.Body.String())
			}
			var resource struct {
				Name        string `json:"name"`
				OperationID string `json:"operationId"`
			}
			if err = json.Unmarshal(replayed.Body.Bytes(), &resource); err != nil || resource.Name == "" || resource.OperationID != replayed.Header().Get("Karakoram-Operation-Id") {
				t.Fatal("recovery changed response schema", replayed.Body.String(), err)
			}
		})
	}
}
