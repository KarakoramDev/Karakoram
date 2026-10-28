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
)

func TestNativeFailurePreservesAgentTransportAndDiagnostic(t *testing.T) {
	p, rc, b := setupPXC(t)
	converge(t, p, rc, b)
	baseAction := b.action
	b.action = func(w http.ResponseWriter, r *http.Request, name string) bool {
		if r.URL.Path != "/v1/mysql/status" {
			return baseAction(w, r, name)
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusServiceUnavailable)
		json.NewEncoder(w).Encode(agentapi.ErrorResponse{Error: "MySQL native startup/status unavailable; MY-000000 failed to open gcomm backend"})
		return true
	}
	status, err := p.Status(t.Context(), rc)
	if err != nil {
		t.Fatal(err)
	}
	if status.Phase == domain.PhaseReady {
		t.Fatal("native failure became Ready")
	}
	for _, member := range status.Members {
		if member.Set == "proxy" {
			continue
		}
		if !member.Agent.Healthy || member.Health || member.State != "UNREACHABLE" {
			t.Fatalf("transport/native health conflated: %+v", member)
		}
	}
	diagnostics := 0
	for _, condition := range status.Conditions {
		if condition.Type == "NativeObservationFailed" {
			diagnostics++
			if !strings.Contains(condition.Message, "gcomm backend") {
				t.Fatal(condition)
			}
		}
	}
	if diagnostics != 1 {
		t.Fatalf("want one native diagnostic condition, got %d", diagnostics)
	}
	b.action = func(w http.ResponseWriter, r *http.Request, name string) bool {
		if r.URL.Path != "/v1/status" {
			return baseAction(w, r, name)
		}
		w.WriteHeader(http.StatusServiceUnavailable)
		return true
	}
	status, err = p.Status(t.Context(), rc)
	if err != nil {
		t.Fatal(err)
	}
	for _, member := range status.Members {
		if member.Agent.Healthy {
			t.Fatal("failed transport reported connected")
		}
	}
	for _, condition := range status.Conditions {
		if condition.Type == "NativeObservationFailed" {
			t.Fatal("stale native diagnostic retained")
		}
	}
}
