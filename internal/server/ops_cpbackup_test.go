// SPDX-License-Identifier: Apache-2.0
package server

import (
	"encoding/json"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"testing"
)

func TestControlPlaneBackupPolicyAndStorageReference(t *testing.T) {
	s, _, token := testServer(t)
	for _, body := range []string{
		`{"enabled":true,"storage":{"namespace":"default","name":"missing"},"schedule":"0 3 * * *","retainCount":14}`,
		`{"enabled":true,"storage":{"namespace":"default","name":"archive"},"schedule":"@daily","retainCount":14}`,
	} {
		if w := call(s, "PUT", "/api/v1/control-plane/backup-policy", body, token); w.Code != 422 {
			t.Fatalf("invalid policy: %d %s", w.Code, w.Body.String())
		}
	}
	if w := call(s, "PUT", "/api/v1/control-plane/backup-policy", `{"enabled":true,"storage":{"namespace":"default","name":"archive"},"schedule":"0 3 * * *","retainCount":1001}`, token); w.Code != 400 {
		t.Fatalf("schema-bound policy range: %d %s", w.Code, w.Body.String())
	}
	storage := domain.BackupStorage{Namespace: "default", Name: "archive", Type: "s3", Bucket: "bucket"}
	if err := s.Store.CreateBackupStorage(t.Context(), &storage); err != nil {
		t.Fatal(err)
	}
	body := `{"enabled":true,"storage":{"namespace":"default","name":"archive"},"schedule":"0 3 * * *","retainCount":14}`
	if w := call(s, "PUT", "/api/v1/control-plane/backup-policy", body, token); w.Code != 200 {
		t.Fatal(w.Code, w.Body.String())
	}
	if w := call(s, "DELETE", "/api/v1/backup-storages/archive", "", token); w.Code != 409 {
		t.Fatal(w.Code, w.Body.String())
	}
	if w := call(s, "GET", "/api/v1/control-plane/backup-policy", "", token); w.Code != 200 {
		t.Fatal(w.Code, w.Body.String())
	}
	w := call(s, "POST", "/api/v1/control-plane/backups", "", token)
	if w.Code != 202 {
		t.Fatal(w.Code, w.Body.String())
	}
	var b domain.ControlPlaneBackup
	if err := json.Unmarshal(w.Body.Bytes(), &b); err != nil {
		t.Fatal(err)
	}
	if b.Phase != domain.PhasePending || b.Trigger != "manual" {
		t.Fatalf("run=%+v", b)
	}
	if w = call(s, "POST", "/api/v1/control-plane/backups", "", token); w.Code != 409 {
		t.Fatal(w.Code, w.Body.String())
	}
	if w = call(s, "GET", "/api/v1/control-plane/backups?limit=1", "", token); w.Code != 200 {
		t.Fatal(w.Code, w.Body.String())
	}
}
