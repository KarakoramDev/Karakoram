// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package server

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/mysql"
)

func TestMySQLBackupAdmissionDuringMajorUpgrade(t *testing.T) {
	for _, scenario := range []string{"version-only", "completed-reformation", "active-reformation", "scale", "parameters", "already-preparing", "deleting"} {
		t.Run(scenario, func(t *testing.T) {
			s, _, token := testServer(t)
			p := mysql.New(mysql.Options{Store: s.Store})
			var err error
			s.Registry, err = engine.NewRegistry(p)
			if err != nil {
				t.Fatal(err)
			}
			c := &domain.Cluster{Name: "upgrading", Generation: 2, Spec: domain.ClusterSpec{Provider: "mysql", Version: "8.0.46-37", Topology: domain.Topology{Type: "single"}}}
			p.ApplyDefaults(&c.Spec)
			c.Spec.Backup.Storage = "archive"
			raw, err := json.Marshal(c.Spec)
			if err != nil {
				t.Fatal(err)
			}
			digest := sha256.Sum256(raw)
			stage := "backup"
			formation := map[string]any{"state": "formed"}
			if scenario == "completed-reformation" {
				formation["reformation"] = map[string]any{"state": "formed"}
			}
			if scenario == "active-reformation" {
				formation["reformation"] = map[string]any{"state": "collect"}
			}
			c.Spec.Version = "8.4.11-11"
			switch scenario {
			case "scale":
				c.Spec.Components.Engine.Replicas++
			case "parameters":
				c.Spec.EngineOptions = json.RawMessage(`{"parameters":{"max_connections":"500"}}`)
			case "already-preparing":
				stage = "prepare"
			case "deleting":
				c.Deleting = true
			}
			c.Internal, err = json.Marshal(map[string]any{"mysql": map[string]any{
				"configHash": hex.EncodeToString(digest[:]), "topology": "single",
				"formation": formation,
				"members":   []map[string]any{{"name": "upgradingm0", "role": "data", "stage": "joined", "version": "8.0.46-37"}},
				"rolling":   map[string]any{"from": "8.0.46-37", "to": "8.4.11-11", "major": true, "stage": stage, "requestedAt": time.Now().UTC()},
			}})
			if err != nil {
				t.Fatal(err)
			}
			c.Status.Phase = domain.PhaseUpgrading
			c.Status.ObservedGeneration = 0
			if err = s.Store.CreateCluster(t.Context(), c); err != nil {
				t.Fatal(err)
			}
			if err = s.Store.CreateBackupStorage(t.Context(), &domain.BackupStorage{Name: "archive", Type: "s3", EndpointURL: "https://objects.example", Bucket: "backups"}); err != nil {
				t.Fatal(err)
			}
			w := call(s, "POST", "/api/v1/clusters/upgrading/backups", `{"name":"prerequisite","storage":"archive","type":"full"}`, token)
			want := 409
			if scenario == "version-only" || scenario == "completed-reformation" {
				want = 202
			}
			if w.Code != want {
				t.Fatalf("backup admission: %d, want %d: %s", w.Code, want, w.Body.String())
			}
		})
	}
}
