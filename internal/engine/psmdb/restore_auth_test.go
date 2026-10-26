// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package psmdb

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/store"
	"go.mongodb.org/mongo-driver/v2/bson"
)

func TestRestoreAuthenticationFailureRetainsOperatorRecoveryAcrossRestart(t *testing.T) {
	for _, cleanup := range []bool{false, true} {
		t.Run(map[bool]string{false: "describe", true: "failure-cleanup"}[cleanup], func(t *testing.T) {
			p, rc, backend, _ := backupCleanupSetup(t)
			s := retryState(t, rc)
			rc.Cluster.Spec.Backup.Storage = "archive"
			rc.Cluster.Spec.TLS.Mode, s.TLSMode = "disabled", "disabled"
			s.Phase, s.Sets["rs0"].Stage = "ready", "ready"
			if err := rc.Store.(*store.Store).UpdateClusterSpec(t.Context(), rc.Cluster.Ref(), rc.Cluster.Spec); err != nil {
				t.Fatal(err)
			}
			for _, m := range s.Members {
				m.Stage, m.Version = "active", rc.Cluster.Spec.Version
				activateConfig(rc.Cluster, s, m)
			}
			seedFormedServices(t, p, rc, backend, s)
			if err := save(rc.Cluster, s); err != nil {
				t.Fatal(err)
			}
			native := nativeHealingFixture(t, rc, s)
			replication, err := healthyTestReplication(t.Context(), rc, s, s.Members[0])
			if err != nil {
				t.Fatal(err)
			}
			native.observed = &replication
			native.command = func(command bson.Raw) bson.M {
				if command.Lookup("getParameter").Type != 0 {
					return bson.M{"ok": 1, "featureCompatibilityVersion": bson.M{"version": rc.Cluster.Spec.Version}}
				}
				return bson.M{"ok": 1}
			}
			const authError = "(AuthenticationFailed) unable to authenticate using mechanism SCRAM-SHA-256"
			rc.Agents = responder(t, func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/v1/status" {
					_ = json.NewEncoder(w).Encode(agentapi.Status{EngineVersion: rc.Cluster.Spec.Version, ConfigVersion: 999, Processes: map[string]agentapi.ProcessStatus{"mongod": {State: "running"}, "pbm-agent": {State: "running"}}})
					return
				}
				_ = json.NewEncoder(w).Encode(agentapi.PBMResponse{ExitCode: 1, Output: json.RawMessage(`{"Error":"` + authError + `"}`)})
			})
			op := &domain.Operation{ID: "restore-operation", Cluster: rc.Cluster.Name, Kind: domain.OperationRestore, Phase: domain.PhaseRunning, Payload: json.RawMessage(`{"restore":"recover"}`), Checkpoint: json.RawMessage(`{"step":"poll","member":"` + s.Members[0].Name + `","pbmName":"native-restore"}`)}
			if err := rc.Store.EnqueueRestore(t.Context(), &domain.Restore{Name: "recover", Cluster: rc.Cluster.Name}, op); err != nil {
				t.Fatal(err)
			}
			if cleanup {
				op.Step, op.LastError = "failure-cleanup", authError
			}
			run := p.RunOperation
			if cleanup {
				run = p.CancelOperation
			}
			result, runErr := run(t.Context(), rc, op)
			if !result.Done || (!cleanup && runErr == nil) {
				t.Fatal("authentication failure did not terminate", result, runErr)
			}
			if err := rc.Store.(*store.Store).UpdateClusterState(t.Context(), rc.Cluster); err != nil {
				t.Fatal(err)
			}
			cluster, err := rc.Store.GetCluster(t.Context(), rc.Cluster.Ref())
			if err != nil {
				t.Fatal(err)
			}
			rc.Cluster = cluster
			p = New(p.options)
			availability := *rc
			availability.AvailabilityOnly = true
			status, err := p.Status(t.Context(), &availability)
			if err != nil || status.Phase != domain.PhaseDegraded {
				t.Fatal("healthy processes cleared consistency uncertainty", status, err)
			}
			found := false
			for _, c := range status.Conditions {
				if c.Type == "RestoreConsistency" && c.Status == "False" && c.Reason == "OperatorRecoveryRequired" && strings.Contains(c.Message, authError) && strings.Contains(c.Message, "fresh name") {
					found = true
				}
			}
			if !found {
				t.Fatal("missing durable actionable recovery condition", status.Conditions)
			}
		})
	}
}

func TestRestoreFailureCleanupExpiresWithoutBackupStorage(t *testing.T) {
	p, rc, _, _ := backupCleanupSetup(t)
	s := retryState(t, rc)
	if err := save(rc.Cluster, s); err != nil {
		t.Fatal(err)
	}
	started := time.Now().UTC().Add(-2 * time.Minute)
	op := &domain.Operation{ID: "restore-operation", Cluster: rc.Cluster.Name, Kind: domain.OperationRestore, Phase: domain.PhaseRunning, Step: "failure-cleanup", LastError: "(AuthenticationFailed) Authentication failed", FailureCleanupStartedAt: &started, Payload: json.RawMessage(`{"restore":"recover"}`), Checkpoint: json.RawMessage(`{"step":"poll","pbmName":"native-restore","storage":"missing"}`)}
	if err := rc.Store.EnqueueRestore(t.Context(), &domain.Restore{Name: "recover", Cluster: rc.Cluster.Name}, op); err != nil {
		t.Fatal(err)
	}
	result, err := p.CancelOperation(t.Context(), rc, op)
	if err != nil || !result.Done {
		t.Fatal("missing storage prevented bounded failure cleanup", result, err)
	}
	if retryState(t, rc).RestoreConsistency == nil {
		t.Fatal("missing storage bypassed the consistency fence")
	}
	r, err := rc.Store.GetRestore(t.Context(), rc.Cluster.Ref(), "recover")
	if err != nil || r.Phase != domain.PhaseFailed || !strings.Contains(r.Error, "AuthenticationFailed") || !strings.Contains(r.Error, "one-minute deadline") {
		t.Fatal("missing storage lost truthful terminal failure", r, err)
	}
}
