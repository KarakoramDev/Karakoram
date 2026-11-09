// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package postgres

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/infra"
)

// Supabase embeds this same Core and uses its replacement/import lifecycle.
func TestReplacementPlacementSurvivesFrozenImport(t *testing.T) {
	for _, lost := range []bool{false, true} {
		for _, relocate := range []bool{false, true} {
			t.Run(fmt.Sprintf("lost=%v/relocate=%v", lost, relocate), func(t *testing.T) {
				p, rc, base := setup(t, "ha")
				converge(t, p, rc)
				s, err := load(rc.Cluster)
				if err != nil {
					t.Fatal(err)
				}
				old := s.find("demop1")
				predecessor := old.Volume
				old.RuntimeRecovery.VolumeLost = lost
				if err := save(rc.Cluster, s); err != nil {
					t.Fatal(err)
				}
				backend := &interruptedImportBackend{fakeBackend: base}
				rc.Backend = backend
				payload, _ := json.Marshal(map[string]any{"member": old.Name, "relocate": relocate, "avoidGroup": "node-a"})
				op := &domain.Operation{ID: "placement", Kind: domain.OperationReplaceMember, Payload: payload}
				if _, err := p.RunOperation(t.Context(), rc, op); err != nil {
					t.Fatal(err)
				}
				s, _ = load(rc.Cluster)
				next := s.find(s.Replacement.New)
				raw, _ := json.Marshal(next)
				var recorded struct {
					ReplacesVolume string `json:"replacesVolume"`
				}
				if err := json.Unmarshal(raw, &recorded); err != nil {
					t.Fatal(err)
				}
				if recorded.ReplacesVolume != predecessor {
					t.Fatalf("durable predecessor = %q, want %q", recorded.ReplacesVolume, predecessor)
				}
				p = New(p.options)
				for range 30 {
					if _, err := p.Sync(t.Context(), rc); err != nil {
						t.Fatal(err)
					}
					s, _ = load(rc.Cluster)
					next = s.find(next.Name)
					if next.ImportBatch != nil {
						break
					}
				}
				if next.ImportBatch == nil {
					t.Fatal("replacement import was not frozen")
				}
				assertPlacement := func(specs []infra.ServiceSpec) {
					t.Helper()
					for _, spec := range specs {
						if spec.Kind != infra.KindVolume {
							continue
						}
						if spec.SpreadReplaces != predecessor || spec.SpreadGroup != rc.Cluster.Name {
							t.Fatalf("wrong replacement placement: %+v", spec)
						}
						if relocate && (len(spec.SpreadAvoid) != 1 || spec.SpreadAvoid[0] != "node-a") || !relocate && len(spec.SpreadAvoid) != 0 {
							t.Fatal("wrong exclusion", spec.SpreadAvoid)
						}
						return
					}
					t.Fatal("replacement volume missing")
				}
				assertPlacement(next.ImportBatch.Specs)
				frozen, _ := json.Marshal(next.ImportBatch.Specs)
				s.find(old.Name).Volume = "changed-predecessor-volume"
				if err := save(rc.Cluster, s); err != nil {
					t.Fatal(err)
				}
				s, err = load(rc.Cluster)
				if err != nil {
					t.Fatal(err)
				}
				token, err := rc.Secrets.GetAgentToken(t.Context(), rc.Cluster.Ref(), next.Name)
				if err != nil {
					t.Fatal(err)
				}
				if _, err := s.find(next.Name).ImportBatch.Submit(t.Context(), rc, next.Name, token.Token); err != nil {
					t.Fatal(err)
				}
				if len(backend.batches) != 1 {
					t.Fatalf("import submissions: %d", len(backend.batches))
				}
				assertPlacement(backend.batches[0])
				submitted, _ := json.Marshal(engine.NewImportPlan(backend.batches[0]).Specs)
				if string(frozen) != string(submitted) {
					t.Fatal("restart changed frozen import")
				}
			})
		}
	}
}
