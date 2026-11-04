// SPDX-License-Identifier: Apache-2.0
package server

import (
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"testing"
)

func TestControlPlaneBackupIdempotency(t *testing.T) {
	s, _, token := testServer(t)
	storage := domain.BackupStorage{Namespace: "default", Name: "state", Type: "s3", Bucket: "bucket"}
	if err := s.Store.CreateBackupStorage(t.Context(), &storage); err != nil {
		t.Fatal(err)
	}
	policy := `{"enabled":false,"storage":{"namespace":"default","name":"state"},"schedule":"0 3 * * *","retainCount":14}`
	if w := call(s, "PUT", "/api/v1/control-plane/backup-policy", policy, token); w.Code != 200 {
		t.Fatal(w.Code, w.Body.String())
	}
	first := idempotentCall(s, "POST", "/api/v1/control-plane/backups", "", token, "backup-request")
	if first.Code != 202 {
		t.Fatal(first.Code, first.Body.String())
	}
	again := idempotentCall(s, "POST", "/api/v1/control-plane/backups", "", token, "backup-request")
	if again.Code != 202 || again.Body.String() != first.Body.String() {
		t.Fatalf("replayed response: %d %s", again.Code, again.Body.String())
	}
	runs, err := s.Store.ListControlPlaneBackups(t.Context())
	if err != nil || len(runs) != 1 {
		t.Fatalf("runs=%+v err=%v", runs, err)
	}
}
