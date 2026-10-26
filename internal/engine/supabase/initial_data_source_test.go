// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package supabase

import (
	"encoding/json"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/domain"
)

func TestInitialDataSourceGatesSupabaseStackAcrossRestartAndAmendment(t *testing.T) {
	p, rc, _ := setupProvider(t)
	convergeProvider(t, p, rc)
	rc.Cluster.Spec.DataSource = &domain.DataSource{Cluster: "source", Backup: "base"}
	rc.Cluster.Generation++
	var root map[string]json.RawMessage
	if err := json.Unmarshal(rc.Cluster.Internal, &root); err != nil {
		t.Fatal(err)
	}
	root["postgresBackup"] = json.RawMessage(`{"initialQueued":true,"cloneComplete":true}`)
	var err error
	rc.Cluster.Internal, err = json.Marshal(root)
	if err != nil {
		t.Fatal(err)
	}
	// The application stack and PG can both be healthy while source credential
	// cleanup/publication has not completed. Reconstructing the provider cannot
	// turn the early PostgreSQL repository-cutover marker into success.
	p = New(p.options)
	status, err := p.Status(t.Context(), rc)
	if err != nil {
		t.Fatal(err)
	}
	if status.Phase == domain.PhaseReady {
		t.Fatal("Supabase became Ready before initial restore completed")
	}
	applied, err := p.InitialDataSourceApplied(rc.Cluster)
	if err != nil || applied {
		t.Fatalf("early cutover treated as restored data: %v %v", applied, err)
	}
	rc.Cluster.Status.Conditions = append(rc.Cluster.Status.Conditions, domain.Condition{Type: "InitialDataSource", Status: "False", Reason: "RestoreAborted", Message: "initial restore cancelled"})
	status, err = p.Status(t.Context(), rc)
	if status.Phase != domain.PhaseFailed {
		t.Fatalf("aborted Supabase restore not Failed: %+v %v", status, err)
	}
	root["postgresBackup"] = json.RawMessage(`{"initialQueued":true,"cloneComplete":true,"initialApplied":true}`)
	rc.Cluster.Internal, err = json.Marshal(root)
	if err != nil {
		t.Fatal(err)
	}
	status, err = p.Status(t.Context(), rc)
	if status.Phase != domain.PhaseFailed {
		t.Fatalf("late native success cleared aborted Supabase restore: %+v %v", status, err)
	}
	rc.Cluster.Status.Conditions = nil
	status, err = p.Status(t.Context(), rc)
	if err != nil || status.Phase != domain.PhaseReady {
		t.Fatalf("completed restore did not release Supabase readiness: %+v %v", status, err)
	}
}
