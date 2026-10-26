// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package elastic

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine/elastic/agentspec"
	"github.com/KarakoramDev/Karakoram/internal/engine/elastic/esapi"
)

func TestFormedIdentityNeverRepublishesBootstrapAfterNodeLoss(t *testing.T) {
	p, rc, f, _ := coreSetup(t, 3)
	s := coreConverge(t, p, rc, f)
	uuid := s.ClusterUUID
	clear(f.nodes)
	// Even an old activated payload containing bootstrap names must be fenced by
	// the durable cluster UUID on every configuration pull after all nodes vanish.
	for _, m := range s.esNodes() {
		cfg, err := p.MemberConfig(t.Context(), rc, m.Name)
		if err != nil {
			t.Fatal(err)
		}
		var spec agentspec.Spec
		if err := json.Unmarshal(cfg.EngineSpec, &spec); err != nil {
			t.Fatal(err)
		}
		if len(spec.Elasticsearch.InitialMasterNodes) != 0 {
			t.Fatal("formed identity republished bootstrap nodes")
		}
	}
	if _, err := p.Sync(t.Context(), rc); err != nil {
		t.Fatal(err)
	}
	s, err := load(rc.Cluster)
	if err != nil || s.ClusterUUID != uuid {
		t.Fatalf("lost identity after quorum loss: uuid=%q err=%v", s.ClusterUUID, err)
	}
	status, err := p.Status(t.Context(), rc)
	if err != nil || status.Phase == domain.PhaseReady {
		t.Fatalf("missing quorum reported Ready: %+v %v", status, err)
	}
}

func TestRestoreRecoveryHoldsOnStorageAndShardFailures(t *testing.T) {
	for _, mode := range []string{"storage-error", "active-recovery", "unavailable-primary"} {
		t.Run(mode, func(t *testing.T) {
			p, rc, s, st := opsFixture(t, func(w http.ResponseWriter, r *http.Request) {
				switch {
				case r.URL.Path == "/_cat/recovery":
					if mode == "storage-error" {
						http.Error(w, "storage unavailable", http.StatusServiceUnavailable)
						return
					}
					if mode == "active-recovery" {
						opsJSON(w, []map[string]string{{"stage": "index"}})
						return
					}
					opsJSON(w, []any{})
				case r.URL.Path == "/_cluster/health":
					opsJSON(w, esapi.Health{Status: "red"})
				case strings.HasSuffix(r.URL.Path, "/_restore"):
					t.Error("recovery checkpoint resubmitted restore")
				default:
					opsJSON(w, map[string]any{"acknowledged": true, "nodes": map[string]any{}})
				}
			})
			st.backup.Phase, st.backup.Provider, st.backup.SourceVersion = domain.PhaseSucceeded, "elasticsearch", "9.5.5"
			st.backup.Location = snapshotLocation(&st.storage, clusterBasePath(&st.storage, rc.Cluster, s), "kk-archive", st.backup.Name)
			st.restore = domain.Restore{Name: "restore-one", Source: domain.RestoreSource{Backup: st.backup.Name}, Phase: domain.PhaseRunning}
			s.RestoreActive = true
			if err := putSecret(t.Context(), rc, "operations/restore-recovery/restore-submission", []byte(`{"completed":true}`)); err != nil {
				t.Fatal(err)
			}
			result, err := p.runRestore(t.Context(), rc, s, &domain.Operation{ID: "restore-recovery"}, operationState{Stage: "recover"}, st.restore.Name)
			if result.Done || opsCheckpoint(t, result).Stage != "recover" || !s.RestoreActive || st.restore.Phase == domain.PhaseSucceeded {
				t.Fatalf("unsafe recovery completion: %+v %v", result, err)
			}
			if (err != nil) != (mode == "storage-error") {
				t.Fatalf("storage observation error=%v", err)
			}
		})
	}
}
