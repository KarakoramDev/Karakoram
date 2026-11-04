// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package postgres

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/postgres/pgspec"
)

func TestPITRStatusDoesNotProbeResumingMembers(t *testing.T) {
	for _, stage := range []string{"planned", "imported"} {
		t.Run(stage, func(t *testing.T) {
			p, rc, s := backupFixture(t, "ha")
			rc.Cluster.Spec.Backup.PITR.Enabled = true
			for _, m := range s.Members {
				m.Stage = stage
			}
			if err := save(rc.Cluster, s); err != nil {
				t.Fatal(err)
			}
			var reads atomic.Int32
			installBackupAgent(t, rc, func(w http.ResponseWriter, r *http.Request) {
				reads.Add(1)
				<-r.Context().Done()
			})
			// A resumed HA formation has no native endpoint yet. Even with a
			// short caller budget its negative observation must finish, not run
			// serial four-second writer probes and poison the outer sweep.
			ctx, cancel := context.WithTimeout(t.Context(), time.Second)
			defer cancel()
			status, err := engine.ObserveStatus(ctx, p, rc)
			if err != nil || ctx.Err() != nil {
				t.Fatalf("resuming status exhausted its budget: %v (caller %v)", err, ctx.Err())
			}
			if reads.Load() != 0 {
				t.Fatalf("probed %d not-yet-deployed members", reads.Load())
			}
			assertPITRCondition(t, status, "Unknown", "ObservationUnavailable")
			if status.Backup.PITRWindow != nil {
				t.Fatal("published an unobserved recovery watermark")
			}
		})
	}
}

func TestPITRStatusReusesFreshUniqueWriter(t *testing.T) {
	for _, scenario := range []string{"unique", "no-writer", "multiple-writers", "unreachable-writer", "archive-failing", "no-watermark", "invalid-watermark"} {
		t.Run(scenario, func(t *testing.T) {
			p, rc, s := backupFixture(t, "ha")
			rc.Cluster.Spec.Backup.PITR.Enabled = true
			start := time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)
			end := start.Add(time.Minute)
			storage, err := p.backupStorage(t.Context(), rc, "archive", rc.Cluster.Ref().Namespace)
			if err != nil {
				t.Fatal(err)
			}
			backup := &domain.Backup{Namespace: rc.Cluster.Namespace, Cluster: rc.Cluster.Name, Name: "base", Provider: rc.Cluster.Spec.Provider, Storage: "archive", Type: "full", Phase: domain.PhaseSucceeded, CompletedAt: &start, Postgres: &domain.PostgresBackup{Repository: repoPrefix(storage, rc.Cluster, s), SystemID: "system"}}
			if err := rc.Store.CreateBackup(t.Context(), backup); err != nil {
				t.Fatal(err)
			}
			var nativeReads atomic.Int32
			installBackupAgent(t, rc, func(w http.ResponseWriter, r *http.Request) {
				name := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
				switch r.URL.Path {
				case "/v1/status":
					if scenario == "unreachable-writer" && name == "demop2" {
						http.Error(w, "unavailable", http.StatusServiceUnavailable)
						return
					}
					_ = json.NewEncoder(w).Encode(agentapi.Status{})
				case "/v1/postgres/status":
					if !strings.HasPrefix(name, "demopx") {
						nativeReads.Add(1)
					}
					st := pgspec.StatusResponse{Running: true, Role: "replica", SystemID: "system", RecoverableUntil: end.Format(time.RFC3339Nano)}
					if strings.HasPrefix(name, "demopx") {
						st.Role = "proxy"
					} else if name == "demop2" && scenario != "no-writer" || name == "demop0" && scenario == "multiple-writers" {
						st.Role = "primary"
					}
					if name == "demop2" {
						st.ArchiveFailing = scenario == "archive-failing"
						if scenario == "no-watermark" {
							st.RecoverableUntil = ""
						} else if scenario == "invalid-watermark" {
							st.RecoverableUntil = "invalid"
						}
					}
					_ = json.NewEncoder(w).Encode(st)
				default:
					http.NotFound(w, r)
				}
			})
			status, err := p.Status(t.Context(), rc)
			if err != nil {
				t.Fatal(err)
			}
			if got := nativeReads.Load(); got != int32(len(s.data())) {
				t.Fatalf("native data reads=%d; want one per data member (%d), no PITR reprobe", got, len(s.data()))
			}
			switch scenario {
			case "unique":
				assertPITRCondition(t, status, "True", "WindowAvailable")
				if window := status.Backup.PITRWindow; window == nil || !window.Start.Equal(start) || !window.End.Equal(end) {
					t.Fatalf("wrong observed recovery window: %+v", window)
				}
				// The explicit PITR endpoint still discovers the live writer; only
				// status sampling reuses its own already-collected observation.
				windows, err := p.PITRWindow(t.Context(), rc)
				if err != nil || len(windows) != 1 || !windows[0].End.Equal(end) || nativeReads.Load() != int32(len(s.data())+3) {
					t.Fatalf("explicit PITR discovery changed: %+v %v reads=%d", windows, err, nativeReads.Load())
				}
			case "archive-failing", "no-watermark":
				assertPITRCondition(t, status, "False", "WindowUnavailable")
			default:
				assertPITRCondition(t, status, "Unknown", "ObservationUnavailable")
			}
			if scenario != "unique" && status.Backup.PITRWindow != nil {
				t.Fatal("unsafe PITR window from unavailable/ambiguous writer")
			}
		})
	}
}

func assertPITRCondition(t *testing.T, status domain.ClusterStatus, value, reason string) {
	t.Helper()
	for _, condition := range status.Conditions {
		if condition.Type == "PITR" {
			if condition.Status != value || condition.Reason != reason {
				t.Fatalf("PITR = %+v; want %s/%s", condition, value, reason)
			}
			return
		}
	}
	t.Fatal("missing PITR condition")
}
