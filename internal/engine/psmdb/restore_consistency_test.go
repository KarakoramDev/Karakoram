// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package psmdb

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/store"
	"go.mongodb.org/mongo-driver/v2/bson"
)

func TestRestoreConsistencySurvivesRecoveryAndClearsOnlyAfterNewRestore(t *testing.T) {
	p, rc, backend, _ := backupCleanupSetup(t)
	st := rc.Store.(*store.Store)
	p.ensureManagedUsers = func(context.Context, *engine.Context, *internalState) error { return nil }
	s := retryState(t, rc)
	rc.Cluster.Spec.Backup.Storage = "archive"
	rc.Cluster.Spec.TLS.Mode, s.TLSMode = "disabled", "disabled"
	s.Phase, s.Sets["rs0"].Stage = "ready", "ready"
	if err := st.UpdateClusterSpec(t.Context(), rc.Cluster.Ref(), rc.Cluster.Spec); err != nil {
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
	// The wire fixture defaults to demom1 as primary, but this cluster has one
	// member. Both Sync and Status must observe the actual healthy replica set.
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
	if err := rc.Store.CreateBackup(t.Context(), &domain.Backup{Name: "base", Cluster: rc.Cluster.Name, Storage: "archive", Type: "logical", Phase: domain.PhaseSucceeded, PBMName: "snapshot"}); err != nil {
		t.Fatal(err)
	}
	starts := 0
	nativeStatus := "starting"
	nativeName := "native-failed"
	const nativeError = "restore roles: NotWritablePrimary; successes: 0 / fails: 0"
	rc.Agents = responder(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/status" {
			_ = json.NewEncoder(w).Encode(agentapi.Status{EngineVersion: rc.Cluster.Spec.Version, ConfigVersion: 999, Processes: map[string]agentapi.ProcessStatus{"mongod": {State: "running"}, "pbm-agent": {State: "running"}}})
			return
		}
		var req agentapi.PBMRequest
		_ = json.NewDecoder(r.Body).Decode(&req)
		out := json.RawMessage(`{}`)
		switch req.Command {
		case "restore":
			starts++
			out, _ = json.Marshal(map[string]string{"name": nativeName})
		case "describe-restore":
			out, _ = json.Marshal(map[string]string{"status": nativeStatus, "error": nativeError})
		case "list":
			out = json.RawMessage(`{"snapshots":[],"pitr":{"ranges":[]}}`)
		}
		_ = json.NewEncoder(w).Encode(agentapi.PBMResponse{Output: out})
	})
	// Persist and reload both objects, as a new controller would. Do not rely on
	// mutations to the old in-memory Cluster or Operation surviving a restart.
	reopen := func(op *domain.Operation, saveCluster bool) *domain.Operation {
		t.Helper()
		if saveCluster {
			if err := st.UpdateClusterState(t.Context(), rc.Cluster); err != nil {
				t.Fatal(err)
			}
		}
		if err := st.UpdateOperation(t.Context(), op); err != nil {
			t.Fatal(err)
		}
		cluster, err := rc.Store.GetCluster(t.Context(), rc.Cluster.Ref())
		if err != nil {
			t.Fatal(err)
		}
		rc.Cluster = cluster
		loaded, err := rc.Store.GetOperation(t.Context(), op.ID)
		if err != nil {
			t.Fatal(err)
		}
		fresh := New(p.options)
		fresh.ensureManagedUsers = p.ensureManagedUsers
		p = fresh
		return loaded
	}
	newRestore := func(name string) *domain.Operation {
		t.Helper()
		op := &domain.Operation{ID: name + "-operation", Cluster: rc.Cluster.Name, Kind: domain.OperationRestore, Phase: domain.PhaseRunning, Payload: json.RawMessage(`{"restore":"` + name + `"}`), Checkpoint: json.RawMessage(`{"step":"start","member":"` + s.Members[0].Name + `","snapshot":"snapshot"}`)}
		if err := rc.Store.EnqueueRestore(t.Context(), &domain.Restore{Name: name, Cluster: rc.Cluster.Name, Source: domain.RestoreSource{Backup: "base"}}, op); err != nil {
			t.Fatal(err)
		}
		return op
	}
	run := func(op *domain.Operation) (engine.OpResult, error) {
		t.Helper()
		result, err := p.RunOperation(t.Context(), rc, op)
		if result.Checkpoint != nil {
			op.Checkpoint = result.Checkpoint
		}
		if result.Done {
			op.Phase = domain.PhaseSucceeded
			if err != nil {
				op.Phase, op.Message = domain.PhaseFailed, err.Error()
			}
		}
		return result, err
	}
	failed := newRestore("failed")
	if result, err := run(failed); err != nil || result.Step != "poll" {
		t.Fatal(result, err)
	}
	failed = reopen(failed, true)
	if result, err := run(failed); err != nil || result.Step != "poll" {
		t.Fatal(result, err)
	}
	nativeStatus = "error"
	if result, err := run(failed); err != nil || result.Done {
		t.Fatal(result, err)
	}
	if retryState(t, rc).RestoreConsistency == nil {
		t.Fatal("native failure did not fence consistency")
	}
	// Simulate checkpoint persistence succeeding while cluster persistence was
	// lost: replay must reconstruct the fence before compensation can complete.
	failed = reopen(failed, false)
	if retryState(t, rc).RestoreConsistency != nil {
		t.Fatal("fixture retained unpersisted marker")
	}
	for range 30 {
		result, err := run(failed)
		if retryState(t, rc).RestoreConsistency == nil {
			t.Fatal("checkpoint replay lost consistency fence")
		}
		failed = reopen(failed, true)
		if result.Done {
			if err == nil || !strings.Contains(err.Error(), nativeError) {
				t.Fatal("native failure lost", err)
			}
			break
		}
		if err != nil {
			t.Fatal(err)
		}
	}
	if failed.Phase != domain.PhaseFailed || starts != 1 {
		t.Fatal("failure replay reinitiated native restore", failed, starts)
	}
	// Runtime recovery also leaves ordinary platform/configuration maintenance
	// to reconcile. Drive the public provider path to completion, persisting and
	// recreating the controller after every step rather than manufacturing ready.
	settle := func(op *domain.Operation) domain.ClusterStatus {
		t.Helper()
		fenced := retryState(t, rc).RestoreConsistency != nil
		for range 100 {
			if _, err := p.Sync(t.Context(), rc); err != nil {
				t.Fatal(err)
			}
			reopen(op, true)
			current := retryState(t, rc)
			if (current.RestoreConsistency != nil) != fenced {
				t.Fatal("ordinary runtime maintenance changed restore consistency", current.RestoreConsistency)
			}
			if current.Phase != "ready" || current.Pending != nil || current.Retry != nil || rc.Cluster.Status.ObservedGeneration != rc.Cluster.Generation {
				continue
			}
			availability := *rc
			availability.AvailabilityOnly = true
			status, err := p.Status(t.Context(), &availability)
			if err != nil {
				t.Fatal(err)
			}
			healthy, writable := false, false
			for _, condition := range status.Conditions {
				if condition.Type == "Healthy" && condition.Status == "True" {
					healthy = true
				}
				if condition.Type == "Replication/rs0" && condition.Status == "True" {
					writable = true
				}
			}
			if !healthy || !writable || len(status.Members) != len(current.Members) {
				t.Fatal("settled runtime is not healthy with writable replication", status)
			}
			for _, member := range status.Members {
				if !member.Health || !member.Agent.Healthy {
					t.Fatal("settled member is unhealthy", member)
				}
			}
			return status
		}
		t.Fatal("runtime maintenance did not converge", retryState(t, rc), rc.Cluster.Status)
		return domain.ClusterStatus{}
	}
	assertDegraded := func() {
		t.Helper()
		status := settle(failed)
		if rc.Cluster.Status.Phase != domain.PhaseDegraded || status.Phase != domain.PhaseDegraded {
			t.Fatal("healthy settled runtime used liveness as consistency proof", rc.Cluster.Status, status)
		}
		found := false
		for _, c := range status.Conditions {
			if c.Type == "RestoreConsistency" && c.Status == "False" && c.Reason == "OperatorRecoveryRequired" && strings.Contains(c.Message, nativeError) {
				found = true
			}
		}
		if !found {
			t.Fatal("missing actionable consistency condition", status.Conditions)
		}
	}
	assertDegraded()
	// A failed no-start restore and unrelated configuration changes cannot clear
	// a previous positively observed native failure.
	noStart := newRestore("never-started")
	noStart.Checkpoint = json.RawMessage(`{"step":"complete","failure":"RestoreNeverStarted: no native metadata"}`)
	if result, err := run(noStart); !result.Done || err == nil {
		t.Fatal(result, err)
	}
	noStart = reopen(noStart, true)
	if noStart.ID != "never-started-operation" || noStart.Phase != domain.PhaseFailed || !strings.Contains(noStart.Message, "RestoreNeverStarted") {
		t.Fatal("recreated controller lost failed no-start history", noStart)
	}
	rc.Cluster.Generation++
	assertDegraded()
	if starts != 1 {
		t.Fatal("no-start restore initiated native work", starts)
	}
	nativeStatus, nativeName = "done", "native-success"
	success := newRestore("fresh-success")
	for range 30 {
		result, err := run(success)
		if err != nil {
			t.Fatal(err)
		}
		if !result.Done && retryState(t, rc).RestoreConsistency == nil {
			t.Fatal("cleared before target recovery completed", result.Step)
		}
		success = reopen(success, true)
		if result.Done {
			break
		}
	}
	if success.Phase != domain.PhaseSucceeded || starts != 2 || retryState(t, rc).RestoreConsistency != nil {
		t.Fatal("fresh success did not clear fence", success, starts)
	}
	status := settle(success)
	if status.Phase != domain.PhaseReady {
		t.Fatal("fresh safe success did not recover readiness", status)
	}
	oldRestore, err := rc.Store.GetRestore(t.Context(), rc.Cluster.Ref(), "failed")
	if err != nil || oldRestore.Phase != domain.PhaseFailed || !strings.Contains(oldRestore.Error, nativeError) {
		t.Fatal(oldRestore, err)
	}
	oldOperation, err := rc.Store.GetOperation(t.Context(), failed.ID)
	if err != nil || oldOperation.Phase != domain.PhaseFailed {
		t.Fatal(oldOperation, err)
	}
}
