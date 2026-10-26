// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package nomad

import (
	"net/http"
	"strings"
	"testing"
)

func TestNeverPlacedObservationRequiresOriginalBlockedHistory(t *testing.T) {
	for _, mode := range []string{"blocked", "terminal-allocation", "evaluation-gone", "superseded-job", "allocation-read-error"} {
		t.Run(mode, func(t *testing.T) {
			f, b, _ := blockedDeployment(t, true)
			f.setAutoTick(false)
			r, err := b.load(t.Context(), "runtime")
			if err != nil {
				t.Fatal(err)
			}
			f.mu.Lock()
			switch mode {
			case "terminal-allocation":
				f.allocs["old"] = &fakeAlloc{row: fakeRow{"ID": "old", "Namespace": "default", "JobID": "runtime", "ClientStatus": "complete"}}
			case "evaluation-gone":
				delete(f.evals, r.Pending.EvalID)
			case "superseded-job":
				f.jobs[fakeKey("default", "runtime")].row["Version"] = r.Pending.JobVersion + 1
			case "allocation-read-error":
				f.requestHook = func(w http.ResponseWriter, r *http.Request) (bool, func()) {
					if strings.HasSuffix(r.URL.Path, "/allocations") {
						http.Error(w, "unavailable", http.StatusServiceUnavailable)
						return true, nil
					}
					return false, nil
				}
			}
			f.mu.Unlock()
			unplaced, err := b.RuntimeNeverPlaced(t.Context(), "runtime")
			if mode == "allocation-read-error" {
				if err == nil || unplaced {
					t.Fatal("unavailable history became absence")
				}
				return
			}
			if err != nil || unplaced != (mode == "blocked") {
				t.Fatalf("%s proof=%v err=%v", mode, unplaced, err)
			}
		})
	}
}
