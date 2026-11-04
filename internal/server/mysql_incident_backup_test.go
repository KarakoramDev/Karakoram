// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package server

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/mysql"
	"github.com/KarakoramDev/Karakoram/internal/engine/pxc"
)

type incidentBackupRaceProvider struct {
	*mysql.Core
	afterAdmission func()
}

func (p *incidentBackupRaceProvider) IncidentBackupReady(ctx context.Context, c *domain.Cluster, secrets engine.SecretStore, kind string) bool {
	allowed := p.Core.IncidentBackupReady(ctx, c, secrets, kind)
	if allowed {
		p.afterAdmission()
	}
	return allowed
}

func TestPXCIncidentFullBackupWithPendingUpdate(t *testing.T) {
	for _, scenario := range []string{"health-wait", "incremental", "no-incident", "acknowledged", "rolling", "draining", "reforming", "unformed", "unapplied", "legacy-state", "racing-spec"} {
		t.Run(scenario, func(t *testing.T) {
			s, _, token := testServer(t)
			p := pxc.New(mysql.Options{Store: s.Store})
			var err error
			s.Registry, err = engine.NewRegistry(p)
			if err != nil {
				t.Fatal(err)
			}
			c := &domain.Cluster{Name: "recovering", Generation: 3}
			p.ApplyDefaults(&c.Spec)
			member := map[string]any{"name": "recoveringm0", "role": "data", "stage": "joined"}
			formation := map[string]any{"state": "formed"}
			state := map[string]any{"topology": "galera", "formation": formation, "members": []any{member}, "nativeConvergedGeneration": 3}
			switch scenario {
			case "unapplied":
				state["nativeConvergedGeneration"] = 2
			case "legacy-state":
				delete(state, "nativeConvergedGeneration")
			case "rolling":
				state["rolling"] = map[string]any{"stage": "wait"}
			case "draining":
				member["proxyDraining"] = true
			case "reforming":
				formation["reformation"] = map[string]any{"state": "collect"}
			case "unformed":
				formation["state"] = "joining"
			}
			c.Internal, err = json.Marshal(map[string]any{"mysql": state})
			if err != nil {
				t.Fatal(err)
			}
			c.Status = domain.ClusterStatus{Phase: domain.PhaseDegraded, ObservedGeneration: 2,
				Conditions: []domain.Condition{{Type: "GTIDHistorySafe", Status: "False", Reason: "GTIDHistoryDiscontinuityLatched"}}}
			if err = s.Store.CreateCluster(t.Context(), c); err != nil {
				t.Fatal(err)
			}
			c.Spec.Components.Engine.Replicas++
			if err = s.Store.UpdateClusterSpec(t.Context(), c.Ref(), c.Spec); err != nil {
				t.Fatal(err)
			}
			c.Spec.Components.Engine.Replicas--
			if err = s.Store.UpdateClusterSpec(t.Context(), c.Ref(), c.Spec); err != nil {
				t.Fatal(err)
			}
			c, err = s.Store.GetCluster(t.Context(), c.Ref())
			if err != nil {
				t.Fatal(err)
			}
			if scenario == "racing-spec" {
				s.Registry, err = engine.NewRegistry(&incidentBackupRaceProvider{Core: p, afterAdmission: func() {
					c.Spec.Components.Engine.Resources.MinRAM++
					c.Spec.Components.Engine.Resources.MaxRAM++
					if err := s.Store.UpdateClusterSpec(t.Context(), c.Ref(), c.Spec); err != nil {
						t.Fatal(err)
					}
				}})
				if err != nil {
					t.Fatal(err)
				}
			}
			incident := map[string]any{"detectedAt": time.Now().UTC().Add(-time.Minute)}
			if scenario == "acknowledged" {
				incident["baselineAt"] = time.Now().UTC()
			}
			incidents := []any{incident}
			if scenario == "no-incident" {
				incidents = nil
			}
			journal, err := json.Marshal(map[string]any{"incidents": incidents})
			if err != nil {
				t.Fatal(err)
			}
			if err = s.Store.SetClusterSecret(t.Context(), c.Ref(), "mysql/gtid/history", journal); err != nil {
				t.Fatal(err)
			}
			update := &domain.Operation{Cluster: c.Name, Kind: domain.OperationUpdate, Phase: domain.PhaseRunning, TargetGeneration: c.Generation, Message: "Waiting for cluster health"}
			if err = s.Store.EnqueueOperation(t.Context(), update); err != nil {
				t.Fatal(err)
			}
			if err = s.Store.CreateBackupStorage(t.Context(), &domain.BackupStorage{Name: "archive", Type: "s3", EndpointURL: "https://objects.example", Bucket: "backups"}); err != nil {
				t.Fatal(err)
			}
			kind := "full"
			if scenario == "incremental" {
				kind = "incremental"
			}
			w := call(s, "POST", "/api/v1/clusters/recovering/backups", `{"name":"acknowledgement","storage":"archive","type":"`+kind+`"}`, token)
			want := 409
			if scenario == "health-wait" {
				want = 202
			}
			if w.Code != want {
				t.Fatalf("backup admission: %d, want %d: %s", w.Code, want, w.Body.String())
			}
			after, err := s.Store.GetCluster(t.Context(), c.Ref())
			if err != nil || after.Status.ObservedGeneration != 2 {
				t.Fatalf("admission acknowledged desired state: %+v %v", after, err)
			}
			op, err := s.Store.GetOperation(t.Context(), update.ID)
			if err != nil || op.Phase != domain.PhaseRunning {
				t.Fatalf("admission changed update outcome: %+v %v", op, err)
			}
			if scenario == "racing-spec" {
				backups, err := s.Store.ListBackups(t.Context(), c.Ref())
				if err != nil || len(backups) != 0 || after.Generation != 4 {
					t.Fatalf("racing spec edit did not fence backup admission: %+v %+v %v", after, backups, err)
				}
			}
		})
	}
}
