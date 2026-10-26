// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package engine

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/infra"
)

type placementBackend struct {
	infra.Backend
	failures []infra.PlacementFailure
	err      error
}

func (b *placementBackend) PlacementFailures(context.Context, []string) ([]infra.PlacementFailure, error) {
	return b.failures, b.err
}

func TestPlacementObservationSurfacesAndClearsCurrentFailure(t *testing.T) {
	at := time.Now().UTC()
	b := &placementBackend{failures: []infra.PlacementFailure{{Member: "m0", Node: "node-a", Message: "member m0 node node-a: dimension memory exhausted; constraint filtered"}}}
	events := &deploymentGuardEvents{}
	rc := &Context{Backend: b, Events: events, Clock: observationClock{at}, Cluster: &domain.Cluster{Name: "demo"}}
	p := observationProvider{observe: func(context.Context, *Context) (domain.ClusterStatus, error) {
		s := readyObservation(at)
		s.Conditions = append(s.Conditions, domain.Condition{Type: "Ready", Status: "True"})
		return s, nil
	}}
	status, err := ObserveStatus(t.Context(), p, rc)
	if err != nil || status.Phase == domain.PhaseReady || status.Members[0].Health || !strings.Contains(status.Message, "memory") {
		t.Fatalf("scheduler failure hidden or false Ready: %+v %v", status, err)
	}
	found := false
	for _, c := range status.Conditions {
		if c.Type == "Placement/m0" && c.Reason == "BackendPlacementFailed" {
			found = true
		}
		if c.Type == "Ready" && c.Status == "True" {
			t.Fatal("false Ready condition")
		}
	}
	if !found || len(events.events) != 1 || events.events[0].Reason != "BackendPlacementFailed" {
		t.Fatalf("missing condition/event: %+v %+v", status, events.events)
	}
	rc.Cluster.Status = status
	if _, err := ObserveStatus(t.Context(), p, rc); err != nil || len(events.events) != 1 {
		t.Fatal("unchanged placement failure repeated event", err)
	}
	b.failures = nil
	status, err = ObserveStatus(t.Context(), p, rc)
	if err != nil || status.Phase != domain.PhaseReady {
		t.Fatalf("placement recovery hidden: %+v %v", status, err)
	}
	for _, c := range status.Conditions {
		if c.Reason == "BackendPlacementFailed" {
			t.Fatal("stale placement failure retained")
		}
	}
}

func TestPlacementObservationReadFailureNeverRenewsReady(t *testing.T) {
	at := time.Now().UTC()
	rc := &Context{Backend: &placementBackend{err: errors.New("scheduler unavailable")}, Clock: observationClock{at}, Cluster: &domain.Cluster{Name: "demo"}}
	calls := 0
	p := observationProvider{observe: func(context.Context, *Context) (domain.ClusterStatus, error) {
		calls++
		return readyObservation(at), nil
	}}
	status, err := ObserveStatus(t.Context(), p, rc)
	if err == nil || calls != 2 || status.Phase == domain.PhaseReady || !status.ObservedAt.IsZero() || status.Members[0].Agent.Healthy {
		t.Fatalf("failed scheduler read renewed readiness: %+v %v", status, err)
	}
}
