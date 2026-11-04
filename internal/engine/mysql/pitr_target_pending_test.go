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

func TestPITRTargetInPendingBaselineDoesNotBecomeOutOfRange(t *testing.T) {
	for _, stage := range []string{"sidecars", "queue-busy"} {
		t.Run(stage, func(t *testing.T) {
			p, rc, backend := backupFixture(t, "single")
			rc.Cluster.Spec.Backup.PITR.Enabled = true
			if err := rc.Store.(*store.Store).UpdateClusterSpec(t.Context(), rc.Cluster.Ref(), rc.Cluster.Spec); err != nil {
				t.Fatal(err)
			}
			if err := rc.Store.(*store.Store).UpdateClusterState(t.Context(), rc.Cluster); err != nil {
				t.Fatal(err)
			}
			inventoryBackup(t, rc, "base-a", "full", "", 10, time.Unix(10, 0))
			b := inventoryBackup(t, rc, "base-b", "full", "", 20, time.Unix(30, 0))
			pending := true
			backend.action = func(w http.ResponseWriter, r *http.Request, _ string) bool {
				if r.URL.Path != "/v1/mysql/pitr-coverage" {
					return false
				}
				var request myspec.PITRCoverageRequest
				if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
					t.Error(err)
				}
				response := myspec.PITRCoverageResponse{VerificationStartedAt: time.Now(), VerifiedAt: time.Now(), Fresh: request.RequireFresh}
				if request.ObjectPaths[0] == b.MySQL.ObjectPaths[0] {
					if pending {
						response.Pending = true
						response.Progress = &myspec.PITRCoverageProgress{Stage: stage, Processed: 16, Total: 32}
					} else {
						response.Windows = []myspec.PITRWindow{{Start: time.Unix(30, 0), End: time.Unix(40, 0)}}
					}
				} else {
					response.Windows = []myspec.PITRWindow{{Start: time.Unix(10, 0), End: time.Unix(20, 0)}}
				}
				_ = json.NewEncoder(w).Encode(response)
				return true
			}
			windows, err := p.PITRWindow(t.Context(), rc)
			if err != nil || len(windows) != 1 || !windows[0].End.Equal(time.Unix(20, 0)) {
				t.Fatalf("status lost completed baseline A: windows=%v err=%v", windows, err)
			}
			if _, err := p.ValidatePITRRestore(t.Context(), rc, time.Unix(15, 0)); err != nil {
				t.Fatalf("proven target rejected due to another pending root: %v", err)
			}
			assertPending := func(err error) {
				t.Helper()
				var progress *pitrCoveragePending
				var resource *domain.ResourceError
				if !errors.As(err, &progress) || !errors.As(err, &resource) || resource.Reason != domain.ReasonInvalidState {
					t.Fatalf("pending baseline became out-of-range: %v", err)
				}
			}
			_, err = p.ValidatePITRRestore(t.Context(), rc, time.Unix(35, 0))
			assertPending(err)
			_, _, err = p.resolveRestore(t.Context(), rc, restoreSource{Namespace: rc.Cluster.Namespace, Cluster: rc.Cluster.Name, Time: time.Unix(35, 0).UTC().Format(time.RFC3339Nano)})
			assertPending(err)
			pending = false
			if _, err := p.ValidatePITRRestore(t.Context(), rc, time.Unix(35, 0)); err != nil {
				t.Fatalf("completed baseline B did not become admissible: %v", err)
			}
			_, err = p.ValidatePITRRestore(t.Context(), rc, time.Unix(45, 0))
			if err == nil {
				t.Fatal("uncovered target admitted after all roots completed")
			}
			var field *engine.ValidationError
			if !errors.As(err, &field) || field.Reason != domain.ReasonOutOfRange {
				t.Fatalf("fully observed out-of-range target lost classification: %v", err)
			}
		})
	}
}
