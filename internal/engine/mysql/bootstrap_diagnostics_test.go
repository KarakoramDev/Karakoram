// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package mysql

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
)

func TestBootstrapNativeDiagnosticReachesReconcileError(t *testing.T) {
	p, rc, b := setup(t, "group")
	b.action = func(w http.ResponseWriter, r *http.Request, _ string) bool {
		if r.URL.Path != "/v1/mysql/bootstrap" {
			return false
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusServiceUnavailable)
		_ = json.NewEncoder(w).Encode(agentapi.ErrorResponse{Error: "MySQL bootstrap failed: MySQL 3092: server not configured; recent GR log (may predate request): [MY-011735] [GCS] local endpoint unavailable"})
		return true
	}
	for range 80 {
		_, err := p.Sync(t.Context(), rc)
		if err != nil {
			for _, want := range []string{"HTTP 503", "MySQL 3092", "MY-011735", "local endpoint unavailable"} {
				if !strings.Contains(err.Error(), want) {
					t.Fatalf("reconciliation lost native diagnostic %q", want)
				}
			}
			return
		}
		b.apply(t, p, rc)
	}
	t.Fatal("bootstrap dispatch checkpoint not reached")
}
