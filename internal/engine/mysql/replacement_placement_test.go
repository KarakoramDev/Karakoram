// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package mysql

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/infra"
)

type placementImportBackend struct {
	*fakeBackend
	batches [][]infra.ServiceSpec
}

func (b *placementImportBackend) ImportServices(ctx context.Context, specs []infra.ServiceSpec) (infra.Op, error) {
	b.batches = append(b.batches, specs)
	return b.fakeBackend.ImportServices(ctx, specs)
}

func TestReplacementPlacementSurvivesFrozenImport(t *testing.T) {
	for _, provider := range []string{"mysql", "pxc"} {
		for _, lost := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/lost=%v", provider, lost), func(t *testing.T) {
				var p *Core
				var rc *engine.Context
				var base *fakeBackend
				if provider == "pxc" {
					p, rc, base = setupPXC(t)
				} else {
					p, rc, base = setup(t, "group")
				}
				converge(t, p, rc, base)
				s, err := load(rc.Cluster)
				if err != nil {
					t.Fatal(err)
				}
				old := s.data()[2]
				predecessor := old.Volume
				if lost {
					old.ConfirmedVolumeLost, old.RuntimeRecovery.VolumeLost = true, true
					old.LastGTIDExecuted = base.native[old.Name].GTIDExecuted
					for _, m := range s.data() {
						st := base.native[m.Name]
						if provider == "mysql" {
							old.LastGTIDExecuted = recoverySID + ":1-10"
							st.GroupName, st.GTIDExecuted = s.Formation.GroupName, recoverySID+":1-20"
						}
						base.native[m.Name] = st
					}
				}
				if err := save(rc.Cluster, s); err != nil {
					t.Fatal(err)
				}
				backend := &placementImportBackend{fakeBackend: base}
				rc.Backend = backend
				payload, _ := json.Marshal(map[string]string{"member": old.Name})
				op := &domain.Operation{ID: "placement", Kind: domain.OperationReplaceMember, Payload: payload}
				if _, err := p.RunOperation(t.Context(), rc, op); err != nil {
					t.Fatal(err)
				}
				s, _ = load(rc.Cluster)
				next := s.find(s.Replacement.New)
				// The predecessor's volume identity must be captured at allocation, not
				// derived from its name or looked up when the import is eventually planned.
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
				p = New(Options{})
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
						if spec.Kind == infra.KindVolume {
							if spec.SpreadReplaces != predecessor || spec.SpreadGroup != rc.Cluster.Name {
								t.Fatalf("wrong replacement placement: %+v", spec)
							}
							return
						}
					}
					t.Fatal("replacement volume missing")
				}
				assertPlacement(next.ImportBatch.Specs)
				frozen, _ := json.Marshal(next.ImportBatch.Specs)
				// Resume the persisted batch directly after a restart; changes to mutable
				// predecessor state cannot change an already frozen import request.
				s.find(old.Name).Volume = "changed-again"
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
