// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package elastic

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/engine"
)

func assertStatusBudget(t *testing.T, start time.Time, err error, member string) {
	t.Helper()
	if elapsed := time.Since(start); elapsed > engine.ObservationTimeout+time.Second {
		t.Errorf("status read took %s", elapsed)
	}
	if err == nil {
		t.Fatal("expected status observation diagnostic")
	}
	for _, text := range []string{"generic status read exceeded 4s budget", "inspect member " + member + " responsiveness/connectivity"} {
		if !strings.Contains(err.Error(), text) {
			t.Errorf("missing %q in %v", text, err)
		}
	}
}

func TestRotationStatusBudget(t *testing.T) {
	p, rc, s, _ := opsFixture(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/status" {
			t.Errorf("unexpected request %s", r.URL.Path)
			http.NotFound(w, r)
			return
		}
		<-r.Context().Done()
	})
	ctx, cancel := context.WithTimeout(t.Context(), 6*time.Second)
	defer cancel()
	start := time.Now()
	ready, err := p.rotationAcknowledged(ctx, rc, s, operationState{Names: []string{"example.com"}}, false)
	if ready {
		t.Fatal("rotation acknowledged without status")
	}
	assertStatusBudget(t, start, err, "example.com")
}
func TestComponentReadyStatusBudget(t *testing.T) {
	p, rc, s, _ := opsFixture(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/status" {
			t.Errorf("unexpected request %s", r.URL.Path)
			http.NotFound(w, r)
			return
		}
		<-r.Context().Done()
	})
	ctx, cancel := context.WithTimeout(t.Context(), 6*time.Second)
	defer cancel()
	start := time.Now()
	ready, err := p.componentReadyResult(ctx, rc, s.Members[0])
	if ready || err != nil {
		t.Fatalf("readiness = %v, %v; want retryable not-ready", ready, err)
	}
	if elapsed := time.Since(start); elapsed > engine.ObservationTimeout+time.Second {
		t.Fatalf("status read took %s", elapsed)
	}
}

func TestComponentNativeReadinessBudget(t *testing.T) {
	p, rc, s, _ := opsFixture(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/elasticsearch/readiness" {
			t.Errorf("unexpected request %s", r.URL.Path)
			http.NotFound(w, r)
			return
		}
		_, _ = io.Copy(io.Discard, r.Body)
		<-r.Context().Done()
	})
	ctx, cancel := context.WithTimeout(t.Context(), 6*time.Second)
	defer cancel()
	client, err := p.agentClient(ctx, rc, s.Members[0].Name)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	start := time.Now()
	ready, message, err := componentReadiness(ctx, client, s.Members[0])
	if ready || err != nil || message != "Component application readiness unavailable" {
		t.Fatalf("native readiness = %v, %q, %v; want retryable not-ready", ready, message, err)
	}
	if elapsed := time.Since(start); elapsed < engine.ObservationTimeout-250*time.Millisecond || elapsed > engine.ObservationTimeout+time.Second {
		t.Fatalf("native readiness took %s; want %s observation budget", elapsed, engine.ObservationTimeout)
	}
}
