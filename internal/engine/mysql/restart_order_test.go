// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package mysql

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine/mysql/myspec"
)

func TestRestartSecondariesBeforePrimaryHandoff(t *testing.T) {
	p, rc, b := setup(t, "group")
	converge(t, p, rc, b)
	old := b.leader
	restarted := map[string]bool{}
	handoffs := 0
	b.action = func(w http.ResponseWriter, r *http.Request, name string) bool {
		if r.URL.Path == "/v1/mysql/switchover" {
			handoffs++
			if name != old || restarted[old] {
				t.Fatal("handoff must go to the current primary before its restart")
			}
			var req myspec.SwitchoverRequest
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				t.Fatal(err)
			}
			found := false
			for peer, status := range b.native {
				if peer != old {
					if !restarted[peer] {
						t.Fatal("handoff occurred before secondary restart", peer)
					}
					found = found || status.ServerUUID == req.TargetUUID && status.MemberRole == "SECONDARY"
				}
			}
			if !found {
				t.Fatal("handoff target is not an online secondary UUID")
			}
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusServiceUnavailable)
			_ = json.NewEncoder(w).Encode(agentapi.ErrorResponse{Error: "MySQL switchover failed: MySQL 3910: native election rejected"})
			return true
		}
		if strings.HasPrefix(r.URL.Path, "/v1/processes/") {
			restarted[name] = true
		}
		return false
	}
	op := &domain.Operation{ID: "restart-order", Kind: domain.OperationRestart}
	for range 30 {
		result, err := p.RunOperation(t.Context(), rc, op)
		if err != nil {
			if handoffs != 1 || restarted[old] || !strings.Contains(err.Error(), "MySQL 3910: native election rejected") {
				t.Fatal("handoff failure lost diagnostics or restarted primary", err)
			}
			return
		}
		if result.Done {
			t.Fatal("restart finished despite rejected handoff")
		}
		op.Checkpoint = result.Checkpoint
		p = New(Options{})
	}
	t.Fatal("restart never reached primary handoff")
}
