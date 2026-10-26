// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package psmdb

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine/psmdb/pbm"
)

func TestMemberConfigPBMGoBudgetUsesAppliedResourcesNotPendingResize(t *testing.T) {
	p, rc, _, _ := backupCleanupSetup(t)
	s := retryState(t, rc)
	m := s.Members[0]
	m.Resources = domain.Resources{MinRAM: 1, MaxRAM: 1}
	m.CacheGB = 0.25
	rc.Cluster.Spec.Components.Engine.Resources.MinRAM = 4
	rc.Cluster.Spec.Components.Engine.Resources.MaxRAM = 4
	if err := save(rc.Cluster, s); err != nil {
		t.Fatal(err)
	}
	config, err := p.MemberConfig(t.Context(), rc, m.Name)
	if err != nil {
		t.Fatal(err)
	}
	if config.PBMGoMemoryLimitBytes != 384<<20 {
		t.Fatal("pending RAM increase escaped the applied-resource boundary", config.PBMGoMemoryLimitBytes)
	}
	env, err := p.pbmEnvForStorage(t.Context(), rc, s, rc.Cluster.Ref().Namespace, "archive")
	if err != nil || len(env.Nodes) != 1 || env.Nodes[0].Resources.MinRAM != 1 || env.Nodes[0].CacheGB != 0.25 {
		t.Fatal("PBM environment did not carry applied memory", env.Nodes, err)
	}
	rendered, err := pbm.RenderConfig(env)
	if err != nil || !strings.Contains(string(rendered), "numParallelCollections: 1") || !strings.Contains(string(rendered), "numInsertionWorkers: 1") {
		t.Fatal("pending desired increase inflated restore concurrency", err)
	}
	m.Resources.MinRAM, m.Resources.MaxRAM, m.CacheGB = 2, 2, 0.75
	if err := save(rc.Cluster, s); err != nil {
		t.Fatal(err)
	}
	config, err = p.MemberConfig(t.Context(), rc, m.Name)
	if err != nil || config.PBMGoMemoryLimitBytes != 896<<20 {
		t.Fatal("observed applied resource budget was not delivered", config.PBMGoMemoryLimitBytes, err)
	}
}

func TestLegacyPBMRestoreAdmissionReadsAuthoritativeBackendMemory(t *testing.T) {
	for _, known := range []bool{true, false} {
		name := "backend-applied"
		if !known {
			name = "backend-unknown"
		}
		t.Run(name, func(t *testing.T) {
			p, rc, backend, _ := backupCleanupSetup(t)
			s := retryState(t, rc)
			m := s.Members[0]
			m.Resources, m.CacheGB = domain.Resources{}, 0 // Pre-field legacy state.
			rc.Cluster.Spec.Components.Engine.Resources = domain.Resources{MinRAM: 4, MaxRAM: 4}
			m.Stage, s.Sets["rs0"].Stage = "active", "ready"
			if known {
				backend.services[m.Name].Resources = domain.Resources{MinRAM: 1, MaxRAM: 1}
			}
			if err := save(rc.Cluster, s); err != nil {
				t.Fatal(err)
			}
			config, err := p.MemberConfig(t.Context(), rc, m.Name)
			want := uint64(0)
			if known {
				want = 384 << 20
			}
			if err != nil || config.PBMGoMemoryLimitBytes != want {
				t.Fatal("legacy member invented desired memory or lost backend sizing", config.PBMGoMemoryLimitBytes, err)
			}
			if err := rc.Store.CreateBackup(t.Context(), &domain.Backup{Name: "base", Cluster: rc.Cluster.Name, Storage: "archive", Type: "logical", PBMName: "snapshot", Phase: domain.PhaseSucceeded}); err != nil {
				t.Fatal(err)
			}
			if err := rc.Store.CreateRestore(t.Context(), &domain.Restore{Name: "recover", Cluster: rc.Cluster.Name, Source: domain.RestoreSource{Backup: "base"}, Phase: domain.PhasePending}); err != nil {
				t.Fatal(err)
			}
			rc.Agents = responder(t, func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path != "/v1/status" {
					t.Error("memory admission unexpectedly reached a native command", r.URL.Path)
				}
				_ = json.NewEncoder(w).Encode(agentapi.Status{Processes: map[string]agentapi.ProcessStatus{"mongod": {State: "running"}, "pbm-agent": {State: "running"}}})
			})
			op := &domain.Operation{ID: "recover-op", Kind: domain.OperationRestore, Payload: json.RawMessage(`{"restore":"recover"}`)}
			result, err := p.RunOperation(t.Context(), rc, op)
			if known {
				if err != nil || result.Done || result.Step != "configure-source" {
					t.Fatal("legacy backend-applied 1 GiB was newly refused", result, err)
				}
			} else if err == nil || !result.Done || !strings.Contains(err.Error(), "PBMRestoreMemoryBudgetUnknown") {
				t.Fatal("unknown backend sizing was replaced with desired capacity", result, err)
			}
		})
	}
}
