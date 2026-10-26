// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package postgres

import (
	"context"
	"encoding/json"
	"encoding/pem"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/postgres/pgspec"
)

func TestDirectReadinessGenericStatusBudget(t *testing.T) {
	for _, hang := range []bool{true, false} {
		name := "reload_retains_parent"
		if hang {
			name = "hung_status"
		}
		t.Run(name, func(t *testing.T) {
			_, rc, _ := setup(t, "single")
			var reloads atomic.Int32
			server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch r.URL.Path {
				case "/v1/postgres/status":
					_ = json.NewEncoder(w).Encode(pgspec.StatusResponse{Running: true, Role: "standalone"})
				case "/v1/status":
					if hang {
						<-r.Context().Done()
						return
					}
					_ = json.NewEncoder(w).Encode(agentapi.Status{})
				case "/v1/reload":
					// Administration may legitimately outlive an observation budget.
					if err := waitObservationLatency(r.Context(), engine.ObservationTimeout+250*time.Millisecond); err != nil {
						return
					}
					reloads.Add(1)
					_ = json.NewEncoder(w).Encode(pgspec.ActionResponse{OK: true})
				default:
					http.NotFound(w, r)
				}
			}))
			defer server.Close()
			rc.Agents = fakeAgents{url: server.URL, ca: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw})}
			ctx, cancel := context.WithTimeout(t.Context(), 7*time.Second)
			defer cancel()
			start := time.Now()
			ok, err := ready(ctx, rc, &member{Name: "demop0", Role: "data", Topology: "single", ConfigVersion: 1})
			elapsed := time.Since(start)
			if hang {
				if elapsed > engine.ObservationTimeout+time.Second {
					t.Errorf("status blocked readiness for %s", elapsed)
				}
				if ok || !errors.Is(err, context.DeadlineExceeded) {
					t.Fatalf("ready = %v, %v; want deadline failure", ok, err)
				}
				for _, text := range []string{"PostgreSQL generic status read", "4s budget", "inspect member demop0 responsiveness/connectivity"} {
					if !strings.Contains(err.Error(), text) {
						t.Errorf("missing %q in %v", text, err)
					}
				}
				if reloads.Load() != 0 {
					t.Fatal("mutated after missing status")
				}
			} else if ok || err != nil || reloads.Load() != 1 {
				t.Fatalf("reload with original context: ready=%v err=%v reloads=%d", ok, err, reloads.Load())
			}
		})
	}
}

func TestSharedMemoryGenericStatusBudget(t *testing.T) {
	p, rc, _ := setup(t, "ha")
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/status" {
			t.Errorf("unexpected request: %s", r.URL.Path)
			http.NotFound(w, r)
			return
		}
		<-r.Context().Done()
	}))
	defer server.Close()
	rc.Agents = fakeAgents{url: server.URL, ca: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw})}
	s := &state{
		Members: []*member{{Name: "demop0", Role: "data"}},
		Rolling: &rollingState{Version: 1, Phases: []rollingPhase{{Direction: "increase", Options: parameterOptions(map[string]string{"max_connections": "101"})}}},
	}
	ctx, cancel := context.WithTimeout(t.Context(), engine.ObservationTimeout+time.Second)
	defer cancel()
	start := time.Now()
	active, err := p.reconcileSharedMemory(ctx, rc, s)
	elapsed := time.Since(start)
	if !active || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("shared-memory acknowledgement = %v, %v; want deadline failure", active, err)
	}
	if elapsed < engine.ObservationTimeout-250*time.Millisecond || elapsed >= engine.ObservationTimeout+750*time.Millisecond {
		t.Errorf("generic status elapsed=%s; want observation budget %s", elapsed, engine.ObservationTimeout)
	}
	if !strings.Contains(err.Error(), "PostgreSQL generic status read exceeded 4s budget: inspect member demop0 responsiveness/connectivity") {
		t.Fatalf("missing member-named observation diagnostic: %v", err)
	}
}
