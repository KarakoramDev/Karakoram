// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package mysql

import (
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/mysql/myspec"
)

func TestPITRReanchorCachesVerifiedBaseline(t *testing.T) {
	for _, pending := range []bool{false, true} {
		t.Run(map[bool]string{false: "verified", true: "pending-backoff"}[pending], func(t *testing.T) {
			p, rc, b := backupFixture(t, "single")
			rc.Cluster.Spec.Backup.PITR.Enabled = true
			inventoryBackup(t, rc, "base", "full", "", 10, time.Unix(10, 0))
			calls := 0
			prior := b.action
			b.action = func(w http.ResponseWriter, r *http.Request, name string) bool {
				if r.URL.Path == "/v1/mysql/pitr-coverage" {
					calls++
					response := myspec.PITRCoverageResponse{SourceUUID: b.native[name].ServerUUID, Pending: pending && calls == 1}
					json.NewEncoder(w).Encode(response)
					return true
				}
				return prior != nil && prior(w, r, name)
			}
			start := time.Now().UTC()
			pass := func(elapsed time.Duration, want int) {
				t.Helper()
				rc.Clock = trafficRenewalClock{at: start.Add(elapsed)}
				s, err := load(rc.Cluster)
				if err != nil {
					t.Fatal(err)
				}
				if err = p.reconcilePITRReanchor(engine.WithObservationPass(t.Context()), rc, s); err != nil {
					t.Fatal(err)
				}
				if err = save(rc.Cluster, s); err != nil {
					t.Fatal(err)
				}
				if calls != want {
					t.Fatalf("at %s: coverage calls=%d, want %d", elapsed, calls, want)
				}
			}
			pass(0, 0)
			pass(4*time.Minute, 0)
			pass(5*time.Minute, 1)
			pass(6*time.Minute, 1)
			count := 1
			if pending {
				count = 2
			}
			pass(10*time.Minute, count)
			pass(20*time.Minute, count)
			inventoryBackup(t, rc, "new-base", "full", "", 20, time.Unix(20, 0))
			pass(21*time.Minute, count+1)
			pass(30*time.Minute, count+1)
		})
	}
}
