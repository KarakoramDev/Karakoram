// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package mysql

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/engine/mysql/myspec"
)

func TestPITRFortyEligibleFullsDoNotHideEachOthersCoverage(t *testing.T) {
	for _, topology := range []string{"single", "pxc"} {
		t.Run(topology, func(t *testing.T) {
			p, rc, backend := backupFixture(t, topology)
			rc.Cluster.Spec.Backup.PITR.Enabled = true
			roots := map[string]time.Time{}
			held := ""
			for i := 0; i < 40; i++ {
				stamp := time.Unix(int64(10+100*i), 0)
				backup := inventoryBackup(t, rc, fmt.Sprintf("base-%02d", i), "full", "", uint64(10+i), stamp)
				roots[backup.MySQL.ObjectPaths[0]] = stamp
				if i == 0 {
					held = backup.MySQL.ObjectPaths[0]
				}
			}
			phase := "ready"
			calls := 0
			oldAction := backend.action
			backend.action = func(w http.ResponseWriter, r *http.Request, member string) bool {
				if r.URL.Path != "/v1/mysql/pitr-coverage" {
					if oldAction != nil {
						return oldAction(w, r, member)
					}
					return false
				}
				var request myspec.PITRCoverageRequest
				if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
					t.Error(err)
				}
				if !request.AcceptPending {
					t.Error("CP omitted pending negotiation")
				}
				root := request.ObjectPaths[0]
				stamp, ok := roots[root]
				if !ok {
					t.Errorf("unexpected root%s", root)
				}
				calls++
				pending := phase == "pending" || phase == "mixed" && root == held
				response := myspec.PITRCoverageResponse{Pending: pending, Progress: &myspec.PITRCoverageProgress{Stage: "queued"}}
				if !pending {
					response.Windows = []myspec.PITRWindow{{Start: stamp, End: stamp.Add(10 * time.Second)}}
					response.VerificationStartedAt = time.Now()
					response.VerifiedAt = time.Now()
				}
				_ = json.NewEncoder(w).Encode(response)
				return true
			}
			windows, byFull, err := p.pitrWindowsByFull(t.Context(), rc)
			if err != nil || len(windows) != 40 || len(byFull) != 40 {
				t.Fatalf("initial completed roots: windows=%d roots=%d err=%v", len(windows), len(byFull), err)
			}
			phase = "pending"
			calls = 0
			_, _, err = p.pitrWindowsByFull(t.Context(), rc)
			var pending *pitrCoveragePending
			if !errors.As(err, &pending) || calls != 40 {
				t.Fatalf("pending sweep aborted or published remembered windows: calls=%d err=%v", calls, err)
			}
			phase = "mixed"
			calls = 0
			windows, byFull, err = p.pitrWindowsByFull(t.Context(), rc)
			if err != nil || calls != 40 || len(windows) != 39 || len(byFull) != 39 {
				t.Fatalf("pending root hid completed roots or leaked remembered proof: calls=%d windows=%d roots=%d err=%v", calls, len(windows), len(byFull), err)
			}
			phase = "ready"
			calls = 0
			started := time.Now()
			for sweep := 0; sweep < 100; sweep++ {
				windows, byFull, err = p.pitrWindowsByFull(t.Context(), rc)
				if err != nil || len(windows) != 40 || len(byFull) != 40 {
					t.Fatalf("sweep%d: windows=%d roots=%d err=%v", sweep, len(windows), len(byFull), err)
				}
			}
			if calls != 4000 {
				t.Fatalf("100 sweeps visited%d roots", calls)
			}
			t.Logf("40 eligible fulls, 100 sweeps: all4000 root observations completed in%s", time.Since(started))
		})
	}
}
