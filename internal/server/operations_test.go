// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package server

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/api"
	"github.com/KarakoramDev/Karakoram/internal/domain"
)

func clusterTracking(t *testing.T, w *httptest.ResponseRecorder, status int) string {
	t.Helper()
	var c api.Cluster
	if err := json.Unmarshal(w.Body.Bytes(), &c); err != nil {
		t.Fatal(w.Code, w.Body.String(), err)
	}
	if w.Code != status || c.OperationId == nil || *c.OperationId == "" || w.Header().Get("Karakoram-Operation-Id") != *c.OperationId {
		t.Fatal(w.Code, w.Header(), w.Body.String())
	}
	if c.Status.Phase == api.ClusterStatusPhase(domain.PhaseCreating) && value(c.Status.Message) == "" {
		t.Fatal("Creating has no stage message", w.Body.String())
	}
	return *c.OperationId
}
func TestClusterAcceptanceAndDeleteOperationBodies(t *testing.T) {
	s, _, token := testServer(t)
	created := call(s, "POST", "/api/v1/clusters", `{"name":"newdemo","spec":{"provider":"fake","version":"8.0","topology":{"type":"replicaSet"}}}`, token)
	first := clusterTracking(t, created, 201)
	for _, method := range []string{"PATCH", "PUT"} {
		body := `{"spec":{"provider":"fake","version":"8.0","topology":{"type":"replicaSet"},"tls":{"mode":"preferTLS","ca":"selfSigned"}}}`
		id := clusterTracking(t, call(s, method, "/api/v1/clusters/newdemo", body, token), 200)
		op, err := s.Store.GetOperation(context.Background(), id)
		if err != nil || op.Target.Name != "newdemo" || op.TargetGeneration < 1 {
			t.Fatal(op, err)
		}
	}
	old, _ := s.Store.GetOperation(context.Background(), first)
	if old.Phase != domain.PhaseCancelled || !strings.Contains(old.Message, "Superseded by operation") {
		t.Fatal(old)
	}
	w := call(s, "DELETE", "/api/v1/clusters/newdemo?keepVolumes=true", "", token)
	var deletion api.Operation
	if err := json.Unmarshal(w.Body.Bytes(), &deletion); err != nil {
		t.Fatal(err, w.Body.String())
	}
	if w.Code != 202 || deletion.Type != api.OperationType(domain.OperationDelete) || deletion.Id != w.Header().Get("Karakoram-Operation-Id") || deletion.Target.Name != "newdemo" {
		t.Fatal(w.Code, w.Body.String())
	}
	if value(deletion.Message) == "" {
		t.Fatal("accepted deletion has no queued summary", deletion)
	}
	c, err := s.Store.GetCluster(context.Background(), domain.ClusterRef{Name: "newdemo"})
	if err != nil {
		t.Fatal(err)
	}
	if c.Status.Phase != domain.PhaseDeleting || c.Status.Message == "" {
		t.Fatal(c.Status)
	}
	if err = s.Store.SuspendCluster(context.Background(), c); err != nil {
		t.Fatal(err)
	}
	clusterTracking(t, call(s, "POST", "/api/v1/clusters/newdemo/resume", "", token), 202)
}

func TestBackupRestoreTrackingAndNotReadyRejection(t *testing.T) {
	s, _, token := testServer(t)
	ctx := context.Background()
	if err := s.Store.CreateBackupStorage(ctx, &domain.BackupStorage{Name: "archive", Type: "s3"}); err != nil {
		t.Fatal(err)
	}
	c, err := s.Store.GetCluster(ctx, domain.ClusterRef{Name: "prod"})
	if err != nil {
		t.Fatal(err)
	}
	c.Status.ObservedGeneration = c.Generation
	if err = s.Store.UpdateClusterState(ctx, c); err != nil {
		t.Fatal(err)
	}
	w := call(s, "POST", "/api/v1/clusters/prod/backups", `{"name":"daily","storage":"archive","type":"logical"}`, token)
	var backup api.Backup
	if err = json.Unmarshal(w.Body.Bytes(), &backup); err != nil {
		t.Fatal(err)
	}
	if w.Code != 202 || backup.OperationId == nil || *backup.OperationId != w.Header().Get("Karakoram-Operation-Id") {
		t.Fatal(w.Code, w.Body.String())
	}
	op, _ := s.Store.GetOperation(ctx, *backup.OperationId)
	if op.Target.Kind != "backup" || op.Target.Name != "daily" {
		t.Fatal(op)
	}
	b, err := s.Store.GetBackup(ctx, c.Ref(), "daily")
	if err != nil {
		t.Fatal(err)
	}
	b.Phase = domain.PhaseSucceeded
	if err = s.Store.UpdateBackup(ctx, b); err != nil {
		t.Fatal(err)
	}
	w = call(s, "POST", "/api/v1/clusters/prod/restores", `{"name":"recover","source":{"backup":"daily","sourceCluster":"prod"}}`, token)
	var restore api.Restore
	if err = json.Unmarshal(w.Body.Bytes(), &restore); err != nil {
		t.Fatal(err)
	}
	if w.Code != 202 || restore.OperationId == nil || *restore.OperationId != w.Header().Get("Karakoram-Operation-Id") {
		t.Fatal(w.Code, w.Body.String())
	}
	op, _ = s.Store.GetOperation(ctx, *restore.OperationId)
	if op.Target.Kind != "restore" || op.Target.Name != "recover" {
		t.Fatal(op)
	}
	c.Status.Phase = domain.PhaseCreating
	if err = s.Store.UpdateClusterState(ctx, c); err != nil {
		t.Fatal(err)
	}
	w = call(s, "POST", "/api/v1/clusters/prod/backups", `{"name":"never","storage":"archive","type":"logical"}`, token)
	if w.Code != 409 || !strings.Contains(w.Body.String(), "Creating") || !strings.Contains(w.Body.String(), "invalid_state") {
		t.Fatal(w.Code, w.Body.String())
	}
}

func TestCancellationAuthorizesPersistedClusterAndRejectsUnsafeRunning(t *testing.T) {
	s, _, admin := testServer(t)
	ctx := context.Background()
	if err := s.Store.CreateNamespace(ctx, &domain.Namespace{Name: "other", ProjectID: "project-other", Phase: "Ready"}); err != nil {
		t.Fatal(err)
	}
	op := &domain.Operation{Cluster: "prod", Kind: domain.OperationRestart}
	if err := s.Store.EnqueueOperation(ctx, op); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Auth.CreateAccount(ctx, domain.Account{Name: "limited", Roles: []string{"role:limited"}}, "limited-test-password"); err != nil {
		t.Fatal(err)
	}
	if err := s.RBAC.ReplacePolicy(ctx, s.RBAC.Policy()+"\np, role:limited, clusters, update, default/prod, allow\n"); err != nil {
		t.Fatal(err)
	}
	limited, _, err := s.Auth.CreateToken(ctx, "limited", "test", nil)
	if err != nil {
		t.Fatal(err)
	}
	w := call(s, "POST", "/api/v1/operations/"+op.ID+"/cancel?namespace=missing", "", limited)
	if w.Code != 404 || !strings.Contains(w.Body.String(), `"resource":"namespace"`) {
		t.Fatal(w.Code, w.Body.String())
	}
	w = call(s, "POST", "/api/v1/operations/"+op.ID+"/cancel?namespace=other", "", limited)
	if w.Code != 202 || !strings.Contains(w.Body.String(), `"phase":"Cancelled"`) || w.Header().Get("Karakoram-Operation-Id") != op.ID {
		t.Fatal(w.Code, w.Body.String())
	}
	if w = call(s, "POST", "/api/v1/operations/"+op.ID+"/cancel", "", admin); w.Code != 409 || !strings.Contains(w.Body.String(), "invalid_state") {
		t.Fatal(w.Code, w.Body.String())
	}
	running := &domain.Operation{Cluster: "prod", Kind: domain.OperationDelete, Phase: domain.PhaseRunning}
	if err = s.Store.EnqueueOperation(ctx, running); err != nil {
		t.Fatal(err)
	}
	if w = call(s, "POST", "/api/v1/operations/"+running.ID+"/cancel", "", admin); w.Code != 409 || !strings.Contains(w.Body.String(), "cannot be cancelled safely") {
		t.Fatal(w.Code, w.Body.String())
	}
}

func TestRetainedNeverStartedBackupRemovalDoesNotRequireStorage(t *testing.T) {
	s, _, token := testServer(t)
	ctx := context.Background()
	backup := &domain.Backup{Cluster: "gone", Name: "never", Storage: "missing-storage", Type: "logical", RecoveryIdentity: "snapshot-recovery"}
	op := &domain.Operation{Cluster: backup.Cluster, Kind: domain.OperationBackup, Payload: json.RawMessage(`{"backup":"never"}`)}
	if err := s.Store.EnqueueBackup(ctx, backup, op); err != nil {
		t.Fatal(err)
	}
	if err := s.Store.SetClusterSecret(ctx, domain.ClusterRef{Name: backup.RecoveryIdentity}, "users/userAdmin", []byte("protected-test-password")); err != nil {
		t.Fatal(err)
	}
	w := call(s, "DELETE", "/api/v1/clusters/gone/backups/never", "", token)
	if w.Code != 409 || !strings.Contains(w.Body.String(), "active work") {
		t.Fatal(w.Code, w.Body.String())
	}
	if _, err := s.Store.RequestOperationCancellation(ctx, op.ID); err != nil {
		t.Fatal(err)
	}
	w = call(s, "DELETE", "/api/v1/clusters/gone/backups/never", "", token)
	if w.Code != 204 {
		t.Fatal(w.Code, w.Body.String())
	}
	if _, err := s.Store.GetClusterSecret(ctx, domain.ClusterRef{Name: backup.RecoveryIdentity}, "users/userAdmin"); err == nil {
		t.Fatal("recovery secret was retained after inventory deletion")
	}
	if err := s.Store.CreateBackup(ctx, &domain.Backup{Cluster: backup.Cluster, Name: backup.Name, Type: "logical"}); err != nil {
		t.Fatal("name was not reusable", err)
	}
}
