// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package rabbitmq

import (
	"errors"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/engine"
)

func TestRollingStartupDiagnosticThresholdStartsAtDisruption(t *testing.T) {
	p, rc, _, agents := setup(t)
	ready(t, p, rc)
	s := state(t, rc)
	m := ordered(s)[0]
	beginChange(rc, s, m, "resources")
	m.ChangeStep = "stop"
	if err := s.save(rc.Cluster); err != nil {
		t.Fatal(err)
	}
	disrupted := rc.Clock.Now()
	if _, err := p.Sync(t.Context(), rc); err != nil {
		t.Fatal(err)
	}
	s = state(t, rc)
	m = s.find(m.Name)
	if !m.ChangeDisruptedAt.Equal(disrupted) || m.ChangeStep != "platform" {
		t.Fatalf("disruption not checkpointed: %+v", m)
	}
	m.ChangeStep = "reload"
	if err := s.save(rc.Cluster); err != nil {
		t.Fatal(err)
	}
	rc.Agents = rollingColdAgents{AgentDialer: agents, member: m.Name}
	rc.Clock.(*testClock).at = disrupted.Add(engine.DefaultReadinessTimeout - engine.ReadinessInterval)
	result, err := p.Sync(t.Context(), rc)
	var wait *engine.PlatformRetryWait
	if err != nil || !errors.As(result.ConvergenceError, &wait) || !errors.Is(result.ConvergenceError, syscall.ECONNREFUSED) {
		t.Fatalf("startup wait lost: %+v %v", result, err)
	}
	// Reconstruct the provider: a later refusal must retain the disruption
	// timestamp and checkpoint, but the diagnostic threshold is not a failure.
	p = New(p.options)
	rc.Clock.(*testClock).at = disrupted.Add(engine.DefaultReadinessTimeout)
	result, err = p.Sync(t.Context(), rc)
	if err != nil || !errors.As(result.ConvergenceError, &wait) || !strings.Contains(result.ConvergenceError.Error(), "startup diagnostic threshold") {
		t.Fatalf("slow startup did not remain a scheduled wait: %+v %v", result, err)
	}
	if got := state(t, rc).find(m.Name); got.ChangeStep != "reload" || !got.ChangeDisruptedAt.Equal(disrupted) {
		t.Fatalf("diagnostic threshold moved checkpoint: %+v", got)
	}
	// Repairing config must not reset elapsed disruption diagnostics.
	s = state(t, rc)
	m = s.find(m.Name)
	m.ChangeStep = "repair-stop"
	if err := s.save(rc.Cluster); err != nil {
		t.Fatal(err)
	}
	rc.Agents = agents
	rc.Clock.(*testClock).at = disrupted.Add(6 * time.Minute)
	if _, err := p.Sync(t.Context(), rc); err != nil {
		t.Fatal(err)
	}
	if !state(t, rc).find(m.Name).ChangeDisruptedAt.Equal(disrupted) {
		t.Fatal("repair reset disruption window")
	}
}

func TestResumeClearsAbandonedRollingStartupDiagnostic(t *testing.T) {
	p, rc, _, _ := setup(t)
	ready(t, p, rc)
	s := state(t, rc)
	m := ordered(s)[0]
	beginChange(rc, s, m, "resources")
	m.ChangeStep = "reload"
	m.ChangeDisruptedAt = rc.Clock.Now()
	m.ChangeStartupMessage = "waiting for rolling startup"
	s.Suspended = true
	if err := s.save(rc.Cluster); err != nil {
		t.Fatal(err)
	}
	if _, err := p.Sync(t.Context(), rc); err != nil {
		t.Fatal(err)
	}
	got := state(t, rc).find(m.Name)
	if got.ChangeStep != "" || !got.ChangeDisruptedAt.IsZero() || got.ChangeStartupMessage != "" {
		t.Fatalf("resume kept abandoned rolling startup state: %+v", got)
	}
}
