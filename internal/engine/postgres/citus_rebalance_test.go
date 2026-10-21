// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package postgres

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine/postgres/pgspec"
)

func TestCitusRebalanceLostResponseAndCancel(t *testing.T) {
	for _, cancel := range []bool{false, true} {
		t.Run(map[bool]string{false: "finish", true: "cancel"}[cancel], func(t *testing.T) {
			p, rc, _ := citusOpsFixture(t)
			starts, polls, stops := 0, 0, 0
			installBackupAgent(t, rc, func(w http.ResponseWriter, r *http.Request) {
				switch r.URL.Path {
				case "/v1/postgres/status":
					json.NewEncoder(w).Encode(pgspec.StatusResponse{Role: "primary", Running: true, Leader: strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")})
				case "/v1/postgres/citus-rebalance-start":
					var req pgspec.CitusRebalanceStartRequest
					json.NewDecoder(r.Body).Decode(&req)
					if req.Receipt != "rebalance-receipt" {
						t.Error("receipt changed")
					}
					starts++
					if starts == 1 {
						http.Error(w, "response lost after scheduling", http.StatusServiceUnavailable)
						return
					}
					json.NewEncoder(w).Encode(pgspec.CitusRebalanceResponse{JobID: 42, State: "running"})
				case "/v1/postgres/citus-rebalance-status", "/v1/postgres/citus-rebalance-stop":
					var req pgspec.CitusRebalanceRequest
					json.NewDecoder(r.Body).Decode(&req)
					if req.JobID != 42 {
						t.Error("job identity lost")
					}
					state := "finished"
					if r.URL.Path == "/v1/postgres/citus-rebalance-stop" {
						stops++
						state = "cancelled"
					} else {
						polls++
					}
					json.NewEncoder(w).Encode(pgspec.CitusRebalanceResponse{JobID: 42, State: state})
				default:
					t.Errorf("unexpected action %s", r.URL.Path)
					http.NotFound(w, r)
				}
			})
			op := &domain.Operation{ID: "rebalance-receipt", Kind: "rebalance", Payload: json.RawMessage(`{}`)}
			result, err := p.RunOperation(t.Context(), rc, op)
			if err != nil {
				t.Fatal(err)
			}
			op.Checkpoint = result.Checkpoint
			if allowed, err := p.CanClaimOperation(rc.Cluster); err != nil || allowed {
				t.Fatal("formation not fenced", err)
			}
			if _, err = p.RunOperation(t.Context(), rc, op); err == nil {
				t.Fatal("expected lost response")
			}
			result, err = p.RunOperation(t.Context(), rc, op)
			if err != nil {
				t.Fatal(err)
			}
			if polls != 0 {
				t.Fatal("polled before checkpoint")
			}
			op.Checkpoint = result.Checkpoint
			if cancel {
				result, err = p.CancelOperation(t.Context(), rc, op)
			} else {
				result, err = p.RunOperation(t.Context(), rc, op)
			}
			if err != nil || !result.Done {
				t.Fatal(result, err)
			}
			if starts != 2 || cancel && stops != 1 || !cancel && polls != 1 {
				t.Fatal(starts, polls, stops)
			}
			if allowed, err := p.CanClaimOperation(rc.Cluster); err != nil || !allowed {
				t.Fatal("formation fence leaked", err)
			}
		})
	}
}

func TestCitusRebalanceNoWork(t *testing.T) {
	p, rc, _ := citusOpsFixture(t)
	installBackupAgent(t, rc, func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/postgres/status":
			json.NewEncoder(w).Encode(pgspec.StatusResponse{Role: "primary", Running: true, Leader: strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")})
		case "/v1/postgres/citus-rebalance-start":
			json.NewEncoder(w).Encode(pgspec.CitusRebalanceResponse{State: "finished"})
		default:
			t.Errorf("unexpected action %s", r.URL.Path)
			http.NotFound(w, r)
		}
	})
	op := &domain.Operation{ID: "no-work", Kind: "rebalance", Payload: json.RawMessage(`{}`)}
	for range 3 {
		result, err := p.RunOperation(t.Context(), rc, op)
		if err != nil {
			t.Fatal(err)
		}
		op.Checkpoint = result.Checkpoint
		if result.Done {
			return
		}
	}
	t.Fatal("no-work job did not finish")
}
