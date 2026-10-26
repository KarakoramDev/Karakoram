// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package server

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/domain"
)

func TestLegacyDiagnosticsNeverServedByAPI(t *testing.T) {
	s, _, token := testServer(t)
	ctx := context.Background()
	text := `AuthenticationFailed credentials: {"SCRAM-SHA-256":{"salt":"fake-old-salt","storedKey":"fake-old-stored","serverKey":"fake-old-server"}}`
	// These model conversions exercise legacy records returned by any repository,
	// independently of both migration and the store's defensive read sanitizer.
	assertSafe := func(v any) {
		t.Helper()
		data, err := json.Marshal(v)
		if err != nil {
			t.Fatal(err)
		}
		for _, secret := range []string{"fake-old-salt", "fake-old-stored", "fake-old-server"} {
			if strings.Contains(string(data), secret) {
				t.Fatal("raw legacy verifier served")
			}
		}
	}
	assertSafe(operationAPI(domain.Operation{Message: text, Step: text}))
	assertSafe(backupAPI(domain.Backup{Error: text}))
	assertSafe(restoreAPI(domain.Restore{Error: text}))
	assertSafe(eventAPI(domain.Event{Message: text}))
	assertSafe(s.statusAPI(domain.ClusterRef{Name: "prod"}, domain.ClusterStatus{Message: text, Conditions: []domain.Condition{{Type: "Ready", Status: "False", Message: text}}}))
	op := &domain.Operation{Cluster: "prod", Kind: domain.OperationRestore, Message: text, Step: text, Phase: domain.PhaseFailed}
	if err := s.Store.EnqueueOperation(ctx, op); err != nil {
		t.Fatal(err)
	}
	if err := s.Store.CreateBackup(ctx, &domain.Backup{Cluster: "prod", Name: "legacybackup", Storage: "archive", Type: "logical", Trigger: "manual", Error: text, Phase: domain.PhaseFailed}); err != nil {
		t.Fatal(err)
	}
	if err := s.Store.CreateRestore(ctx, &domain.Restore{Cluster: "prod", Name: "legacyrestore", Source: domain.RestoreSource{Backup: "legacybackup", SourceCluster: "prod"}, Error: text, Phase: domain.PhaseFailed}); err != nil {
		t.Fatal(err)
	}
	if err := s.Store.AppendEvent(ctx, &domain.Event{Cluster: "prod", Type: "Warning", Message: text}); err != nil {
		t.Fatal(err)
	}
	c, err := s.Store.GetCluster(ctx, domain.ClusterRef{Name: "prod"})
	if err != nil {
		t.Fatal(err)
	}
	c.Status.Message = text
	c.Status.Conditions = []domain.Condition{{Type: "Ready", Status: "False", Message: text}}
	if err = s.Store.UpdateClusterState(ctx, c); err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{"/api/v1/operations/" + op.ID, "/api/v1/operations", "/api/v1/events", "/api/v1/clusters/prod/restores", "/api/v1/clusters/prod/restores/legacyrestore", "/api/v1/clusters/prod/backups", "/api/v1/clusters/prod/backups/legacybackup", "/api/v1/clusters/prod", "/api/v1/clusters"} {
		response := call(s, "GET", path, "", token)
		if response.Code != 200 {
			t.Fatal(path, response.Code)
		}
		for _, secret := range []string{"fake-old-salt", "fake-old-stored", "fake-old-server"} {
			if strings.Contains(response.Body.String(), secret) {
				t.Fatal(path, "raw legacy verifier served")
			}
		}
	}
	if got := operationAPI(domain.Operation{Message: "ordinary progress", Step: "prepare"}); value(got.Message) != "ordinary progress" || value(got.Step) != "prepare" {
		t.Fatal("ordinary operation changed")
	}
}
