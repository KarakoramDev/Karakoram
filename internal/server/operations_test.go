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
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/psmdb"
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
		body := `{"spec":{"provider":"fake","version":"8.0","topology":{"type":"replicaSet"},"tls":{"mode":"preferTLS","ca":"selfSigned"},"engineConfig":"` + method + `"}}`
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

func TestRepeatedClusterDeleteReusesInFlightOperation(t *testing.T) {
	for _, phase := range []domain.Phase{domain.PhasePending, domain.PhaseRunning} {
		for _, options := range []string{
			"keepVolumes=false&keepBackups=false",
			"keepVolumes=true&keepBackups=false",
			"keepVolumes=false&keepBackups=true",
			"keepVolumes=true&keepBackups=true",
		} {
			t.Run(string(phase)+"/"+options, func(t *testing.T) {
				s, _, token := testServer(t)
				ctx := context.Background()
				path := "/api/v1/clusters/prod?" + options
				first := call(s, "DELETE", path, "", token)
				id := first.Header().Get("Karakoram-Operation-Id")
				if first.Code != 202 || id == "" {
					t.Fatal(first.Code, first.Body.String())
				}
				op, err := s.Store.GetOperation(ctx, id)
				if err != nil {
					t.Fatal(err)
				}
				op.Phase, op.Message = phase, "Original deletion progress"
				if err = s.Store.UpdateOperation(ctx, op); err != nil {
					t.Fatal(err)
				}
				before, err := s.Store.GetCluster(ctx, domain.ClusterRef{Name: "prod"})
				if err != nil {
					t.Fatal(err)
				}
				paths := []string{path}
				if options == "keepVolumes=false&keepBackups=false" {
					paths = append(paths, "/api/v1/clusters/prod")
				}
				for _, repeatPath := range paths {
					repeat := call(s, "DELETE", repeatPath, "", token)
					var body api.Operation
					if err = json.Unmarshal(repeat.Body.Bytes(), &body); err != nil {
						t.Fatal(err, repeat.Body.String())
					}
					if repeat.Code != 202 || repeat.Header().Get("Karakoram-Operation-Id") != id || body.Id != id || string(body.Phase) != string(phase) || value(body.Message) != op.Message {
						t.Fatal(repeat.Code, repeat.Header(), repeat.Body.String())
					}
				}
				after, err := s.Store.GetCluster(ctx, before.Ref())
				if err != nil || after.Generation != before.Generation || !after.UpdatedAt.Equal(before.UpdatedAt) {
					t.Fatal("identical delete changed cluster intent", after, err)
				}
				operations, err := s.Store.ListOperations(ctx, before.Ref())
				if err != nil || len(operations) != 1 || operations[0].ID != id || operations[0].Phase != phase || operations[0].CompletedAt != nil {
					t.Fatal("identical delete superseded tracking", operations, err)
				}
				if before.DeleteOptions.KeepVolumes {
					err = s.Store.SuspendCluster(ctx, after)
				} else {
					err = s.Store.DeleteCluster(ctx, before.Ref())
				}
				if err != nil {
					t.Fatal(err)
				}
				completed, err := s.Store.GetOperation(ctx, id)
				if err != nil || completed.Phase != domain.PhaseSucceeded {
					t.Fatal("original delete did not complete", completed, err)
				}
			})
		}
	}
}

func TestRepeatedClusterDeleteWithDifferentOptionsSupersedes(t *testing.T) {
	for _, options := range []string{"keepVolumes=true", "keepBackups=true"} {
		t.Run(options, func(t *testing.T) {
			s, _, token := testServer(t)
			ctx := context.Background()
			first := call(s, "DELETE", "/api/v1/clusters/prod", "", token)
			id := first.Header().Get("Karakoram-Operation-Id")
			if first.Code != 202 || id == "" {
				t.Fatal(first.Code, first.Body.String())
			}
			repeat := call(s, "DELETE", "/api/v1/clusters/prod?"+options, "", token)
			nextID := repeat.Header().Get("Karakoram-Operation-Id")
			var body api.Operation
			if err := json.Unmarshal(repeat.Body.Bytes(), &body); err != nil {
				t.Fatal(err, repeat.Body.String())
			}
			if repeat.Code != 202 || nextID == "" || nextID == id || body.Id != nextID {
				t.Fatal(repeat.Code, repeat.Header(), repeat.Body.String())
			}
			old, err := s.Store.GetOperation(ctx, id)
			if err != nil || old.Phase != domain.PhaseCancelled || old.Message != "Superseded by operation "+nextID+" (delete)" {
				t.Fatal(old, err)
			}
			current, err := s.Store.GetOperation(ctx, nextID)
			if err != nil || current.Phase != domain.PhasePending || current.TargetGeneration != old.TargetGeneration+1 {
				t.Fatal(current, err)
			}
		})
	}
}

func TestRepeatedClusterDeleteRejectsRetentionAfterCleanupStarts(t *testing.T) {
	for _, options := range []string{"keepVolumes=true", "keepBackups=true"} {
		t.Run(options, func(t *testing.T) {
			s, _, token := testServer(t)
			ctx := context.Background()
			first := call(s, "DELETE", "/api/v1/clusters/prod", "", token)
			id := first.Header().Get("Karakoram-Operation-Id")
			if first.Code != 202 || id == "" {
				t.Fatal(first.Code, first.Body.String())
			}
			c, err := s.Store.GetCluster(ctx, domain.ClusterRef{Name: "prod"})
			if err != nil {
				t.Fatal(err)
			}
			if err = s.Store.StartClusterCleanup(ctx, c); err != nil {
				t.Fatal(err)
			}
			repeat := call(s, "DELETE", "/api/v1/clusters/prod?"+options, "", token)
			if repeat.Code != 409 || !strings.Contains(repeat.Body.String(), "retention cannot be promised") {
				t.Fatal(repeat.Code, repeat.Body.String())
			}
			current, err := s.Store.GetCluster(ctx, c.Ref())
			if err != nil || current.Generation != c.Generation || current.DeleteOptions != c.DeleteOptions || !current.Status.CleanupStarted {
				t.Fatal("rejected retention changed cleanup intent", current, err)
			}
			op, err := s.Store.GetOperation(ctx, id)
			if err != nil || op.Phase != domain.PhasePending {
				t.Fatal("rejected retention retired accepted deletion", op, err)
			}
		})
	}
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
	limited, _, err := s.Auth.CreateToken(ctx, "limited", "test", nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	w := call(s, "POST", "/api/v1/operations/"+op.ID+"/cancel?namespace=missing", "", admin)
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
	cluster := &domain.Cluster{Name: "gone", Spec: domain.ClusterSpec{Provider: "fake"}}
	if err := s.Store.CreateCluster(ctx, cluster); err != nil {
		t.Fatal(err)
	}
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
	// Retained inventory outlives the cluster, but admission of new backup
	// work must still start from a persisted live cluster.
	if err := s.Store.DeleteCluster(ctx, cluster.Ref()); err != nil {
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

type requestCancellationProvider struct {
	*psmdb.Provider
	sawRequest bool
}

func (*requestCancellationProvider) Name() string { return "fake" }
func (p *requestCancellationProvider) CanCancelOperation(ctx context.Context, op *domain.Operation) error {
	p.sawRequest = request(ctx) != nil
	return p.Provider.CanCancelOperation(ctx, op)
}

func TestCancellationMissingBackupReturnsSafeConflictWithRequestContext(t *testing.T) {
	s, _, token := testServer(t)
	p := &requestCancellationProvider{Provider: psmdb.New(psmdb.Options{Store: s.Store})}
	s.Registry, _ = engine.NewRegistry(p)
	op := &domain.Operation{Cluster: "prod", Kind: domain.OperationBackup, Phase: domain.PhaseRunning, Payload: json.RawMessage(`{"backup":"missing-legacy-base"}`)}
	if err := s.Store.EnqueueOperation(context.Background(), op); err != nil {
		t.Fatal(err)
	}
	w := call(s, "POST", "/api/v1/operations/"+op.ID+"/cancel", "", token)
	if w.Code != 409 || !p.sawRequest || !strings.Contains(w.Body.String(), "cannot verify backup inventory") || strings.Contains(w.Body.String(), "resource not found") {
		t.Fatal("unsafe cancellation conflict", w.Code, w.Body.String(), p.sawRequest)
	}
	got, err := s.Store.GetOperation(context.Background(), op.ID)
	if err != nil || got.CancelRequested {
		t.Fatal("unverified backup cancellation accepted", got, err)
	}
}
