// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package server

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/psmdb"
)

func TestClusterPatchAfterSourceRemoval(t *testing.T) {
	for _, sourceKind := range []string{"backup", "pitr"} {
		t.Run(sourceKind, func(t *testing.T) {
			s, _, token := testServer(t)
			p := psmdb.New(psmdb.Options{Store: s.Store})
			var err error
			s.Registry, err = engine.NewRegistry(p)
			if err != nil {
				t.Fatal(err)
			}
			spec := domain.ClusterSpec{}
			p.ApplyDefaults(&spec)
			spec.Components.Engine.Replicas = 1
			now := time.Now().UTC()
			source := &domain.Cluster{Name: "source", Spec: spec, Status: domain.ClusterStatus{Backup: domain.BackupStatus{PITRWindow: &domain.TimeWindow{Start: now.Add(-time.Hour), End: now.Add(time.Hour)}}}}
			if err = s.Store.CreateCluster(t.Context(), source); err != nil {
				t.Fatal(err)
			}
			backup := &domain.Backup{Cluster: source.Name, Name: "baseline", Phase: domain.PhaseSucceeded, SourceTopology: spec.Topology, SourceVersion: spec.Version}
			if err = s.Store.CreateBackup(t.Context(), backup); err != nil {
				t.Fatal(err)
			}
			spec.DataSource = &domain.DataSource{Cluster: source.Name}
			if sourceKind == "backup" {
				spec.DataSource.Backup = backup.Name
			} else {
				spec.DataSource.PITRTime = &now
			}
			raw, err := json.Marshal(map[string]any{"name": "copy", "spec": specAPI(spec)})
			if err != nil {
				t.Fatal(err)
			}
			if w := call(s, "POST", "/api/v1/clusters", string(raw), token); w.Code != 201 {
				t.Fatal(w.Code, w.Body.String())
			}
			copy, err := s.Store.GetCluster(t.Context(), domain.ClusterRef{Name: "copy"})
			if err != nil {
				t.Fatal(err)
			}
			copy.Status.Phase = domain.PhaseReady
			if err = s.Store.UpdateClusterState(t.Context(), copy); err != nil {
				t.Fatal(err)
			}
			if err = s.Store.DeleteBackup(t.Context(), source.Ref(), backup.Name); err != nil {
				t.Fatal(err)
			}
			if err = s.Store.DeleteCluster(t.Context(), source.Ref()); err != nil {
				t.Fatal(err)
			}
			for _, patch := range []string{`{"spec":{"components":{"engine":{"replicas":3}}}}`, `{"spec":{"monitoring":{"enabled":false}}}`, `{"spec":{"backup":{"schedules":[]}}}`} {
				if w := call(s, "PATCH", "/api/v1/clusters/copy", patch, token); w.Code != 200 {
					t.Fatal(w.Code, w.Body.String())
				}
			}
			if w := call(s, "PATCH", "/api/v1/clusters/copy", `{"spec":{"dataSource":null}}`, token); w.Code != 400 || !strings.Contains(w.Body.String(), "immutable") {
				t.Fatal(w.Code, w.Body.String())
			}
			w := call(s, "POST", "/api/v1/clusters", strings.Replace(string(raw), `"copy"`, `"other"`, 1), token)
			if w.Code != 400 && w.Code != 404 {
				t.Fatal(w.Code, w.Body.String())
			}
			for _, name := range []string{"source", "default"} {
				if !strings.Contains(w.Body.String(), name) {
					t.Fatal(w.Body.String())
				}
			}
			if sourceKind == "backup" && !strings.Contains(w.Body.String(), "baseline") {
				t.Fatal(w.Body.String())
			}
		})
	}
}
