// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package neo4j

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine/neo4j/neo4jspec"
)

func TestFailedRestoreReleasesMaintenanceAfterReadiness(t *testing.T) {
	p, rc, _, _ := setup(t)
	converge(t, p, rc)
	storage, s3 := setupDumpStore(t, rc)
	b := successfulDump(t, rc, storage, s3)
	installJobAgent(t, rc, func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(neo4jspec.JobResponse{State: neo4jspec.JobFailed, Message: "untrusted-secret"})
	})
	op := createRestore(t, rc, b, "restore-failed")
	first, err := p.RunOperation(t.Context(), rc, op)
	if err != nil {
		t.Fatal(err)
	}
	op.Checkpoint = first.Checkpoint
	// Ordinary convergence recovers the exact durable receipt instead of
	// restarting the member while the offline job owns it.
	if _, err = p.Sync(t.Context(), rc); err != nil {
		t.Fatal(err)
	}
	s, _ := load(rc.Cluster)
	if s.Maintenance != op.ID || s.RestoreRecovery == nil {
		t.Fatal("failed job released maintenance before readiness")
	}
	if _, err = p.RunOperation(t.Context(), rc, op); err == nil || err.Error() != "Neo4j offline restore failed" {
		t.Fatal(err)
	}
	s, _ = load(rc.Cluster)
	if s.Maintenance != "" || s.RestoreRecovery != nil {
		t.Fatal("failed restore kept maintenance after readiness")
	}
	got, err := rc.Store.GetRestore(t.Context(), rc.Cluster.Ref(), "restore-failed")
	if err != nil || got.Phase != domain.PhaseFailed || got.Error != "Neo4j offline restore failed" {
		t.Fatal(got, err)
	}
	if _, err = p.RunOperation(t.Context(), rc, op); err == nil {
		t.Fatal("failed restore replay succeeded")
	}
	s, _ = load(rc.Cluster)
	if s.Maintenance != "" {
		t.Fatal("failed replay reentered maintenance")
	}
}
