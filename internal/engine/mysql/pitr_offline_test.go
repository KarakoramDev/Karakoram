// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package mysql

import (
	"encoding/json"
	"errors"
	"net/http"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/mysql/myspec"
	"github.com/KarakoramDev/Karakoram/internal/store"
)

func TestPITRWithoutWriter(t *testing.T) {
	for _, topology := range []string{"single", "group"} {
		t.Run(topology, func(t *testing.T) {
			p, rc, backend := backupFixture(t, topology)
			rc.Cluster.Spec.Backup.PITR.Enabled = true
			if err := rc.Store.(*store.Store).UpdateClusterSpec(t.Context(), rc.Cluster.Ref(), rc.Cluster.Spec); err != nil {
				t.Fatal(err)
			}
			if err := rc.Store.(*store.Store).UpdateClusterState(t.Context(), rc.Cluster); err != nil {
				t.Fatal(err)
			}
			base := inventoryBackup(t, rc, "base", "full", "", 10, time.Unix(10, 0))
			s, err := load(rc.Cluster)
			if err != nil {
				t.Fatal(err)
			}
			first := s.data()[0].Name
			end := time.Unix(50, 0)
			coverageCalls := 0
			archiveUnavailable := false
			backend.action = func(w http.ResponseWriter, r *http.Request, name string) bool {
				switch r.URL.Path {
				case "/v1/mysql/status":
					http.Error(w, "mysqld stopped", http.StatusServiceUnavailable)
					return true
				case "/v1/mysql/pitr-coverage":
					coverageCalls++
					if archiveUnavailable || topology == "group" && name == first {
						http.Error(w, "agent unavailable", http.StatusServiceUnavailable)
						return true
					}
					var request myspec.PITRCoverageRequest
					if err := json.NewDecoder(r.Body).Decode(&request); err != nil || len(request.ObjectPaths) != 1 || request.ObjectPaths[0] != base.MySQL.ObjectPaths[0] || len(request.ObjectManifests) != 1 || request.ObjectManifests[0].Digest != fixtureManifestDigest {
						t.Error("offline coverage omitted sealed baseline provenance", err)
					}
					json.NewEncoder(w).Encode(myspec.PITRCoverageResponse{Windows: []myspec.PITRWindow{{Start: base.MySQL.EndTime, End: end}}})
					return true
				}
				return false
			}
			// A stale displayed window must not override object-store proof.
			rc.Cluster.Status.Backup.PITRWindow = &domain.TimeWindow{Start: time.Unix(10, 0), End: time.Unix(100, 0)}
			if _, err := p.ValidatePITRRestore(t.Context(), rc, time.Unix(40, 0)); err != nil {
				t.Fatal("offline admission rejected proven coverage", err)
			}
			if coverageCalls == 0 {
				t.Fatal("offline admission did not verify the archive")
			}
			for _, target := range []time.Time{time.Unix(9, 0), time.Unix(51, 0)} {
				_, err := p.ValidatePITRRestore(t.Context(), rc, target)
				var field *engine.ValidationError
				if !errors.As(err, &field) || field.Reason != domain.ReasonOutOfRange || errors.Is(err, engine.ErrUnavailable) {
					t.Fatalf("uncovered target must be a validation error: %v", err)
				}
			}
			p.options.Store = rc.Store
			clone := *rc.Cluster
			clone.Name = "copy"
			targetTime := time.Unix(40, 0)
			clone.Spec.DataSource = &domain.DataSource{Cluster: rc.Cluster.Name, PITRTime: &targetTime}
			if err := p.validateBackups(t.Context(), nil, &clone); err != nil {
				t.Fatal("clone must not require a cached source status window", err)
			}
			source := restoreSource{Namespace: rc.Cluster.Namespace, Cluster: rc.Cluster.Name, Time: time.Unix(40, 0).UTC().Format(time.RFC3339Nano)}
			for _, clone := range []bool{false, true} {
				worker := *rc
				if clone {
					target := *rc.Cluster
					target.Name = "clone"
					worker.Cluster = &target
				}
				selected, request, err := p.resolveRestore(t.Context(), &worker, source)
				if err != nil || selected == nil || selected.Name != base.Name || request.Source.Path != base.MySQL.Repository || request.TargetTime != source.Time {
					t.Fatalf("offline worker (clone=%v): selected=%v request=%+v err=%v", clone, selected, request, err)
				}
			}
			// Revalidation must reject coverage lost since admission, not trust
			// the status window or the earlier successful request.
			end = time.Unix(30, 0)
			if _, _, err := p.resolveRestore(t.Context(), rc, source); !errors.Is(err, engine.ErrInvalid) {
				t.Fatalf("worker accepted target beyond current proof: %v", err)
			}
			archiveUnavailable = true
			if _, err := p.ValidatePITRRestore(t.Context(), rc, time.Unix(20, 0)); !errors.Is(err, engine.ErrUnavailable) {
				t.Fatalf("archive unavailable through every agent must remain unavailable: %v", err)
			}
			rc.Agents = nil
			if _, err := p.ValidatePITRRestore(t.Context(), rc, time.Unix(20, 0)); !errors.Is(err, engine.ErrUnavailable) {
				t.Fatalf("no agent and no proof must remain unavailable: %v", err)
			}
		})
	}
}
