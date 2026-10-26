// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package server

import (
	"encoding/json"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/domain"
)

func TestSuspendedClusterLifecycleRemainsAdmissible(t *testing.T) {
	for _, action := range []string{"resume", "delete", "delete-backup"} {
		t.Run(action, func(t *testing.T) {
			s, _, token := testServer(t)
			ctx := t.Context()
			ref := domain.ClusterRef{Name: "prod"}
			if err := s.Store.MarkClusterDeleting(ctx, ref, domain.DeleteOptions{KeepVolumes: true, KeepBackups: true}); err != nil {
				t.Fatal(err)
			}
			c, err := s.Store.GetCluster(ctx, ref)
			if err != nil {
				t.Fatal(err)
			}
			if err = s.Store.SuspendCluster(ctx, c); err != nil {
				t.Fatal(err)
			}
			method, path := "POST", "/api/v1/clusters/prod/resume"
			switch action {
			case "delete":
				method, path = "DELETE", "/api/v1/clusters/prod"
			case "delete-backup":
				b := &domain.Backup{Cluster: "prod", Name: "daily", Type: "logical", PBMName: "remote-backup", Phase: domain.PhaseSucceeded}
				if err = s.Store.CreateBackup(ctx, b); err != nil {
					t.Fatal(err)
				}
				method, path = "DELETE", "/api/v1/clusters/prod/backups/daily"
			}
			result := call(s, method, path, "", token)
			if result.Code != 202 {
				t.Fatalf("suspended %s rejected: %d %s", action, result.Code, result.Body.String())
			}
			id := result.Header().Get("Karakoram-Operation-Id")
			op, err := s.Store.GetOperation(ctx, id)
			if err != nil || op.Terminal() {
				t.Fatalf("missing accepted lifecycle work: %+v %v", op, err)
			}
			c, err = s.Store.GetCluster(ctx, ref)
			if err != nil {
				t.Fatal(err)
			}
			switch action {
			case "resume":
				if c.Status.Phase != domain.PhaseCreating || c.Deleting || op.Kind != domain.OperationResume {
					t.Fatal(c, op)
				}
			case "delete":
				if !c.Deleting || c.DeleteOptions.KeepVolumes || op.Kind != domain.OperationDelete {
					t.Fatal(c, op)
				}
			case "delete-backup":
				if op.Kind != domain.OperationDeleteBackup || c.Status.Phase != domain.PhaseSuspended {
					t.Fatal(c, op)
				}
			}
			if !json.Valid(result.Body.Bytes()) {
				t.Fatal("invalid lifecycle response")
			}
		})
	}
}
