// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package elastic

import (
	"errors"
	"net/http"
	"strings"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
)

func TestRestoreSnapshotAbsenceDistinguishesRepositoryFailure(t *testing.T) {
	for _, status := range []int{http.StatusNotFound, http.StatusForbidden, http.StatusServiceUnavailable} {
		p, rc, s, st := opsFixture(t, func(w http.ResponseWriter, r *http.Request) {
			if strings.HasSuffix(r.URL.Path, "/snapshot-one") {
				w.WriteHeader(status)
				opsJSON(w, map[string]any{"error": map[string]any{"type": "snapshot_missing_exception", "reason": "unavailable"}})
				return
			}
			opsJSON(w, map[string]any{"acknowledged": true, "nodes": map[string]any{}})
		})
		st.backup.Phase, st.backup.Provider, st.backup.SourceVersion = domain.PhaseSucceeded, "elasticsearch", "9.5.5"
		st.backup.Location = snapshotLocation(&st.storage, clusterBasePath(&st.storage, rc.Cluster, s), "kk-archive", st.backup.Name)
		st.restore = domain.Restore{Name: "restore", Source: domain.RestoreSource{Backup: st.backup.Name}}
		_, err := p.runRestore(t.Context(), rc, s, &domain.Operation{ID: "restore", Kind: domain.OperationRestore}, operationState{}, "restore")
		var field *engine.ValidationError
		if err == nil || errors.As(err, &field) != (status == http.StatusNotFound) {
			t.Fatalf("HTTP %d absence classification: %v", status, err)
		}
		if s.RestoreActive {
			t.Fatal("target fenced before source validation")
		}
	}
}
