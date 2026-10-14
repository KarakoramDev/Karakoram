// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package server

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/api"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
)

func requireDependencyError(t *testing.T, w *httptest.ResponseRecorder, status int, code string, details map[string]string) api.Error {
	t.Helper()
	var e api.Error
	if w.Code != status || json.Unmarshal(w.Body.Bytes(), &e) != nil || e.Code != code || e.Message == "" || e.Details == nil {
		t.Fatalf("response %d: %s", w.Code, w.Body.String())
	}
	for key, want := range details {
		if (*e.Details)[key] != want {
			t.Fatalf("%s: got %v, want %s: %s", key, (*e.Details)[key], want, w.Body.String())
		}
	}
	return e
}

func TestNamespaceNameAndRequiredFieldDiagnostics(t *testing.T) {
	s, token := namespaceTestServer(t)
	for _, name := range []string{"x", "Upper", "1namespace", "bad_name", "trailing-", strings.Repeat("a", 33)} {
		body := fmt.Sprintf(`{"name":%q,"projectId":"project-extra","token":"private-value"}`, name)
		e := requireDependencyError(t, call(s, "POST", "/api/v1/namespaces", body, token), 400, "invalid_request", map[string]string{"field": "name", "reason": "invalid"})
		if !strings.Contains(e.Message, "2–32") || !strings.Contains(e.Message, "start with a letter") || strings.Contains(e.Message, "private-value") {
			t.Fatal(e)
		}
	}
	for _, field := range []string{"name", "projectId", "token"} {
		body := map[string]string{"name": "testing", "projectId": "project-extra", "token": "private-value"}
		delete(body, field)
		raw, _ := json.Marshal(body)
		requireDependencyError(t, call(s, "POST", "/api/v1/namespaces", string(raw), token), 400, "invalid_request", map[string]string{"field": field, "reason": "required"})
	}
	body := `{"name":"testing","projectId":"project-extra","token":"private-value"}`
	if w := call(s, "POST", "/api/v1/namespaces", body, token); w.Code != 201 {
		t.Fatal(w.Code, w.Body.String())
	}
	for _, name := range []string{"testing", "default"} {
		raw := fmt.Sprintf(`{"name":%q,"projectId":"project-extra","token":"private-value"}`, name)
		requireDependencyError(t, call(s, "POST", "/api/v1/namespaces", raw, token), 409, "conflict", map[string]string{"resource": "namespace", "name": name, "reason": "already_exists"})
	}
}

func TestUnknownNamespaceAcrossCollectionsAndSubresources(t *testing.T) {
	s, _, token := testServer(t)
	for _, path := range []string{"/api/v1/clusters", "/api/v1/events", "/api/v1/events/stream", "/api/v1/operations", "/api/v1/backup-storages", "/api/v1/orphaned-volumes", "/metrics", "/api/v1/clusters/prod", "/api/v1/clusters/prod/backups", "/api/v1/clusters/prod/restores", "/api/v1/clusters/prod/database-users", "/api/v1/clusters/prod/operations", "/api/v1/backup-storages/archive"} {
		t.Run(path, func(t *testing.T) {
			requireDependencyError(t, call(s, "GET", path+"?namespace=missing", "", token), 404, "not_found", map[string]string{"resource": "namespace", "name": "missing", "reason": "not_found"})
		})
	}
	for _, path := range []string{"/api/v1/clusters", "/api/v1/events", "/api/v1/operations", "/api/v1/backup-storages", "/api/v1/orphaned-volumes", "/metrics"} {
		if w := call(s, "GET", path, "", token); w.Code != 200 {
			t.Fatal(path, w.Code, w.Body.String())
		}
	}
}

func TestBackupRestoreAndCloneDependencyDiagnostics(t *testing.T) {
	s, _, token := testServer(t)
	requireDependencyError(t, call(s, "POST", "/api/v1/clusters/prod/backups", `{"name":"baseline","storage":"absent","type":"logical"}`, token), 404, "not_found", map[string]string{"resource": "backupStorage", "name": "absent"})
	for _, source := range []string{`{"backup":"missing","pitrTime":"2025-01-15T12:00:00Z","sourceCluster":"prod"}`, `{"sourceCluster":"prod"}`} {
		e := requireDependencyError(t, call(s, "POST", "/api/v1/clusters/prod/restores", `{"name":"recover","source":`+source+`}`, token), 400, "invalid_request", map[string]string{"field": "source", "reason": "conflict"})
		if !strings.Contains(e.Message, "set exactly one of source.backup or source.pitrTime") {
			t.Fatal(e)
		}
	}
	requireDependencyError(t, call(s, "POST", "/api/v1/clusters/prod/restores", `{"name":"recover","source":{"backup":"absent","sourceCluster":"prod"}}`, token), 404, "not_found", map[string]string{"resource": "backup", "name": "absent", "cluster": "prod", "namespace": "default"})
	requireDependencyError(t, call(s, "POST", "/api/v1/clusters", `{"name":"copy",`+namespaceClusterSpec+`,"dataSource":{"cluster":"prod","backup":"absent"}}}`, token), 404, "not_found", map[string]string{"resource": "backup", "name": "absent", "cluster": "prod", "namespace": "default"})
	storage := domain.BackupStorage{Name: "archive", Type: "s3", EndpointURL: "https://s3.example.test", Bucket: "bucket"}
	if err := s.Store.CreateBackupStorage(t.Context(), &storage); err != nil {
		t.Fatal(err)
	}
	backupBody := `{"name":"baseline","storage":"archive","type":"logical"}`
	if w := call(s, "POST", "/api/v1/clusters/prod/backups", backupBody, token); w.Code != 202 {
		t.Fatal(w.Code, w.Body.String())
	}
	requireDependencyError(t, call(s, "POST", "/api/v1/clusters/prod/backups", backupBody, token), 409, "conflict", map[string]string{"resource": "backup", "name": "baseline", "reason": "already_exists"})
	restoreBody := `{"name":"recover","source":{"backup":"baseline","sourceCluster":"prod"}}`
	if w := call(s, "POST", "/api/v1/clusters/prod/restores", restoreBody, token); w.Code != 202 {
		t.Fatal(w.Code, w.Body.String())
	}
	requireDependencyError(t, call(s, "POST", "/api/v1/clusters/prod/restores", restoreBody, token), 409, "conflict", map[string]string{"resource": "restore", "name": "recover", "reason": "already_exists"})
	requireDependencyError(t, call(s, "DELETE", "/api/v1/clusters/prod/backups/baseline", "", token), 409, "conflict", map[string]string{"resource": "backup", "name": "baseline", "reason": "invalid_state", "state": "Pending"})
}

type noPITRProvider struct{ *fakeProvider }

func (*noPITRProvider) Describe() domain.ProviderInfo {
	return domain.ProviderInfo{Name: "fake", DisplayName: "Example engine", BackupTypes: []string{"physical"}}
}
func (*noPITRProvider) PITRWindow(context.Context, *engine.Context) ([]domain.TimeWindow, error) {
	return nil, engine.ErrNotSupported
}

func TestProviderDrivenBackupAndPITRDiagnostics(t *testing.T) {
	s, _, token := testServer(t)
	var err error
	s.Registry, err = engine.NewRegistry(&noPITRProvider{&fakeProvider{}})
	if err != nil {
		t.Fatal(err)
	}
	e := requireDependencyError(t, call(s, "POST", "/api/v1/clusters/prod/backups", `{"name":"baseline","storage":"archive","type":"logical"}`, token), 400, "invalid_request", map[string]string{"field": "type", "reason": "unsupported"})
	if !strings.Contains(e.Message, "physical") || !strings.Contains(e.Message, "Example engine") {
		t.Fatal(e)
	}
	e = requireDependencyError(t, call(s, "GET", "/api/v1/clusters/prod/pitr", "", token), 400, "invalid_request", map[string]string{"field": "source.pitrTime", "reason": "unsupported"})
	if !strings.Contains(e.Message, "Example engine") || !strings.Contains(e.Message, "does not support PITR") {
		t.Fatal(e)
	}
}

func TestStorageFieldConflictAndVerificationDiagnostics(t *testing.T) {
	s, _, token := testServer(t)
	base := map[string]any{"name": "archive", "type": "s3", "endpointURL": "https://s3.example.test", "region": "us-east-1", "bucket": "bucket", "credentials": map[string]string{"accessKeyId": "private-access", "secretAccessKey": "private-secret"}}
	for _, name := range []string{"bad/name", "bad:name", "", strings.Repeat("a", 129), "   "} {
		body := make(map[string]any)
		for k, v := range base {
			body[k] = v
		}
		body["name"] = name
		raw, _ := json.Marshal(body)
		reason := "invalid"
		if name == "" {
			reason = "required"
		}
		requireDependencyError(t, call(s, "POST", "/api/v1/backup-storages", string(raw), token), 400, "invalid_request", map[string]string{"field": "name", "reason": reason})
	}
	for _, field := range []string{"region", "bucket", "endpointURL", "type"} {
		body := make(map[string]any)
		for k, v := range base {
			body[k] = v
		}
		delete(body, field)
		raw, _ := json.Marshal(body)
		requireDependencyError(t, call(s, "POST", "/api/v1/backup-storages", string(raw), token), 400, "invalid_request", map[string]string{"field": field, "reason": "required"})
	}
	for _, field := range []string{"accessKeyId", "secretAccessKey"} {
		body := make(map[string]any)
		for k, v := range base {
			body[k] = v
		}
		creds := map[string]string{"accessKeyId": "private-access", "secretAccessKey": "private-secret"}
		delete(creds, field)
		body["credentials"] = creds
		raw, _ := json.Marshal(body)
		requireDependencyError(t, call(s, "POST", "/api/v1/backup-storages", string(raw), token), 400, "invalid_request", map[string]string{"field": "credentials." + field, "reason": "required"})
	}
	raw, _ := json.Marshal(base)
	if w := call(s, "POST", "/api/v1/backup-storages", string(raw), token); w.Code != 201 {
		t.Fatal(w.Code, w.Body.String())
	}
	requireDependencyError(t, call(s, "POST", "/api/v1/backup-storages", string(raw), token), 409, "conflict", map[string]string{"resource": "backupStorage", "name": "archive", "reason": "already_exists"})
	storage, err := s.Store.GetBackupStorage(t.Context(), "default", "archive")
	if err != nil {
		t.Fatal(err)
	}
	storage.EndpointURL = "http://s3.example.test/path"
	if err = s.Store.UpdateBackupStorage(t.Context(), storage); err != nil {
		t.Fatal(err)
	}
	w := call(s, "POST", "/api/v1/backup-storages/archive/verify", "", token)
	var result api.StorageVerification
	if w.Code != 200 || json.Unmarshal(w.Body.Bytes(), &result) != nil || result.Verified || result.FailureReason == nil || string(*result.FailureReason) != "endpoint_not_allowed" || !strings.Contains(result.Message, "HTTP(S)") {
		t.Fatal(w.Code, w.Body.String())
	}
	if strings.Contains(w.Body.String(), "private-secret") || strings.Contains(w.Body.String(), "private-access") {
		t.Fatal("credentials leaked")
	}
}

func TestObservationFilterValidationAndExplicitAuditDenial(t *testing.T) {
	s, _, admin := testServer(t)
	for _, path := range []string{"/api/v1/events", "/api/v1/events/stream", "/api/v1/operations"} {
		requireDependencyError(t, call(s, "GET", path+"?cluster=INVALID", "", admin), 400, "invalid_request", map[string]string{"field": "cluster"})
	}
	for _, path := range []string{"/api/v1/events", "/api/v1/events/stream"} {
		for _, field := range []string{"type", "resource"} {
			e := requireDependencyError(t, call(s, "GET", path+"?"+field+"=unknown", "", admin), 400, "invalid_request", map[string]string{"field": field, "reason": "unsupported"})
			if !strings.Contains(e.Message, "must be one of:") {
				t.Fatal(e)
			}
		}
	}
	if _, err := s.Auth.CreateAccount(t.Context(), domain.Account{Name: "viewer", Roles: []string{"role:viewer"}}, "viewer-test-password"); err != nil {
		t.Fatal(err)
	}
	viewer, _, err := s.Auth.CreateToken(t.Context(), "viewer", "test", nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{"/api/v1/events", "/api/v1/events/stream"} {
		e := requireDependencyError(t, call(s, "GET", path+"?resource=auth", "", viewer), 403, "forbidden", map[string]string{"resource": "auth", "permission": "audit/read"})
		if !strings.Contains(e.Message, "audit/read") {
			t.Fatal(e)
		}
	}
	for _, kind := range []string{"Normal", "Warning", "Audit"} {
		if w := call(s, "GET", "/api/v1/events?type="+kind, "", admin); w.Code != 200 {
			t.Fatal(w.Code, w.Body.String())
		}
	}
	if w := call(s, "GET", "/api/v1/events", "", viewer); w.Code != 200 {
		t.Fatal(w.Code, w.Body.String())
	}
}
