// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package elastic

import (
	"context"
	"net/http"
	"strings"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine/elastic/esapi"
)

func TestBackupCheckpointReentryAfterCreate(t *testing.T) {
	exists := false
	creates := 0
	p, rc, s, st := opsFixture(t, func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.HasSuffix(r.URL.Path, "/snapshot-one"):
			if r.Method == "PUT" {
				creates++
				exists = true
				opsJSON(w, map[string]any{"accepted": true})
				return
			}
			if !exists {
				w.WriteHeader(http.StatusNotFound)
				opsJSON(w, map[string]any{"error": map[string]any{"type": "snapshot_missing_exception", "reason": "missing"}})
				return
			}
			opsJSON(w, map[string]any{"snapshots": []esapi.SnapshotInfo{{Snapshot: "snapshot-one", State: "SUCCESS", Version: "9.5.5"}}})
		case strings.HasSuffix(r.URL.Path, "/_status"):
			opsJSON(w, map[string]any{"snapshots": []any{map[string]any{"stats": map[string]any{"total": map[string]any{"size_in_bytes": 123}}}}})
		default:
			opsJSON(w, map[string]any{"acknowledged": true, "nodes": map[string]any{}})
		}
	})
	op := &domain.Operation{ID: "backup-one", Kind: domain.OperationBackup}
	result, err := p.runBackup(context.Background(), rc, s, op, operationState{}, st.backup.Name)
	if err != nil {
		t.Fatal(err)
	}
	cp := opsCheckpoint(t, result)
	st.failUpdate = true
	result, err = p.runBackup(context.Background(), rc, s, op, cp, st.backup.Name)
	if err == nil || creates != 1 {
		t.Fatalf("expected interrupted inventory write after one start, creates=%d err=%v", creates, err)
	}
	// Re-enter the same start checkpoint, as after losing the reconciler write.
	result, err = p.runBackup(context.Background(), rc, s, op, cp, st.backup.Name)
	if err != nil || creates != 1 {
		t.Fatalf("re-entry creates=%d err=%v", creates, err)
	}
	result, err = p.runBackup(context.Background(), rc, s, op, opsCheckpoint(t, result), st.backup.Name)
	if err != nil || !result.Done || st.backup.Phase != domain.PhaseSucceeded || st.backup.Provider != "elasticsearch" || st.backup.Type != "snapshot" || st.backup.Location == "" || st.backup.CompletedAt == nil {
		t.Fatalf("backup result %+v backup %+v err %v", result, st.backup, err)
	}
}

func TestBackupTerminalSnapshotFailures(t *testing.T) {
	for _, status := range []string{"PARTIAL", "FAILED", "INCOMPATIBLE"} {
		t.Run(status, func(t *testing.T) {
			p, rc, s, st := opsFixture(t, func(w http.ResponseWriter, r *http.Request) {
				if strings.HasSuffix(r.URL.Path, "/snapshot-one") {
					opsJSON(w, map[string]any{"snapshots": []esapi.SnapshotInfo{{Snapshot: "snapshot-one", State: status, Failures: []esapi.SnapshotFailure{{Reason: "unavailable shard"}}}}})
					return
				}
				opsJSON(w, map[string]any{"acknowledged": true, "nodes": map[string]any{}})
			})
			result, err := p.runBackup(context.Background(), rc, s, &domain.Operation{ID: "failed"}, operationState{Stage: "poll"}, st.backup.Name)
			if err == nil || !result.Done || st.backup.Phase != domain.PhaseFailed || !strings.Contains(st.backup.Error, "unavailable shard") || st.backup.CompletedAt == nil {
				t.Fatalf("result %+v backup %+v err %v", result, st.backup, err)
			}
		})
	}
}

func TestDeleteSnapshotNotFoundIsIdempotent(t *testing.T) {
	deletes := 0
	p, rc, s, st := opsFixture(t, func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/snapshot-one") {
			if r.Method == "DELETE" {
				deletes++
			}
			w.WriteHeader(http.StatusNotFound)
			opsJSON(w, map[string]any{"error": map[string]any{"type": "snapshot_missing_exception", "reason": "missing"}})
			return
		}
		opsJSON(w, map[string]any{"acknowledged": true, "nodes": map[string]any{}})
	})
	st.backup.Provider = "elasticsearch"
	st.backup.Location = snapshotLocation(&st.storage, clusterBasePath(&st.storage, rc.Cluster, s), "kk-archive", "snapshot-one")
	cp := operationState{}
	op := &domain.Operation{ID: "delete-one"}
	for range 6 {
		result, err := p.runDeleteBackup(context.Background(), rc, s, op, cp, st.backup.Name)
		if err != nil {
			t.Fatal(err)
		}
		cp = opsCheckpoint(t, result)
		if result.Done {
			break
		}
	}
	if !st.deleted || deletes != 1 || cp.Stage != "done" {
		t.Fatalf("deleted=%v deletes=%d checkpoint=%+v", st.deleted, deletes, cp)
	}
	result, err := p.runDeleteBackup(context.Background(), rc, s, op, operationState{}, st.backup.Name)
	if err != nil || !result.Done {
		t.Fatalf("second delete: %+v %v", result, err)
	}
}

// Elasticsearch 9 reports a snapshot's index version as a release range;
// recording it verbatim made every restore fail as incompatible.
func TestSnapshotReleaseVersionRange(t *testing.T) {
	for in, want := range map[string]string{"9.5.2-9.5.4": "9.5.2", "9.5.5": "9.5.5", "9111000": "", "": ""} {
		if got := snapshotReleaseVersion(in); got != want {
			t.Errorf("snapshotReleaseVersion(%q)=%q want %q", in, got, want)
		}
	}
	if !restoreVersionCompatible("9.5.2-9.5.4", "9.5.4") || restoreVersionCompatible("9.5.2-9.5.4", "9.4.8") {
		t.Fatal("range compatibility")
	}
}
