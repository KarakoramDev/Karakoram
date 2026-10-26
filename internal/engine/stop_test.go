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

type stopDiagnosticBackend struct {
	infra.Backend
	failures []infra.RuntimeStopFailure
	err      error
	calls    int
}

func (b *stopDiagnosticBackend) RuntimeStopFailures(context.Context, []string) ([]infra.RuntimeStopFailure, error) {
	b.calls++
	return b.failures, b.err
}

func TestRuntimeStopObservationSurfacesAndClearsFailure(t *testing.T) {
	at := time.Now().UTC()
	b := &stopDiagnosticBackend{failures: []infra.RuntimeStopFailure{{Member: "m0", Allocation: "alloc-a", Node: "node-a", Message: "allocation alloc-a node node-a: close open nomad alloc exec stdin"}}}
	events := &deploymentGuardEvents{}
	rc := &Context{Backend: b, Events: events, Clock: observationClock{at}, Cluster: &domain.Cluster{Name: "demo"}}
	p := observationProvider{observe: func(context.Context, *Context) (domain.ClusterStatus, error) {
		s := readyObservation(at)
		s.Conditions = append(s.Conditions, domain.Condition{Type: "Ready", Status: "True"})
		return s, nil
	}}
	status, err := ObserveStatus(t.Context(), p, rc)
	if err != nil || status.Phase == domain.PhaseReady || status.Members[0].Health || !strings.Contains(status.Message, "alloc-a") {
		t.Fatalf("stalled stop hidden or false Ready: %+v %v", status, err)
	}
	found := false
	for _, c := range status.Conditions {
		if c.Type == "RuntimeStop/alloc-a" && c.Reason == "BackendStopStalled" {
			found = true
		}
		if c.Type == "Ready" && c.Status == "True" {
			t.Fatal("stalled live allocation allowed Ready")
		}
	}
	if !found || len(events.events) != 1 || events.events[0].Reason != "BackendStopStalled" {
		t.Fatalf("missing condition/event: %+v %+v", status, events.events)
	}
	rc.Cluster.Status = status
	if _, err := ObserveStatus(t.Context(), p, rc); err != nil || len(events.events) != 1 {
		t.Fatalf("unchanged diagnostic repeated events: %v %+v", err, events.events)
	}
	b.failures = nil
	status, err = ObserveStatus(t.Context(), p, rc)
	if err != nil || status.Phase != domain.PhaseReady {
		t.Fatalf("recovered stop remained stuck: %+v %v", status, err)
	}
	for _, c := range status.Conditions {
		if c.Type == "RuntimeStop/alloc-a" {
			t.Fatal("stale stop diagnostic retained")
		}
	}
}

func TestRuntimeStopObservationPreservesDeletingAndEventDeduplication(t *testing.T) {
	b := &stopDiagnosticBackend{failures: []infra.RuntimeStopFailure{{Member: "m0", Allocation: "alloc-a", Message: "close exec stdin"}}}
	events := &deploymentGuardEvents{}
	rc := &Context{Backend: b, Events: events, Cluster: &domain.Cluster{Name: "demo", Deleting: true, Status: domain.ClusterStatus{Phase: domain.PhaseDeleting, Members: []domain.MemberStatus{{Name: "m0"}}}}}
	for range 2 {
		if err := ObserveRuntimeStops(t.Context(), rc, &rc.Cluster.Status, true); err != nil {
			t.Fatal(err)
		}
	}
	if rc.Cluster.Status.Phase != domain.PhaseDeleting || len(events.events) != 1 {
		t.Fatalf("cleanup lost phase or repeated event: %+v %+v", rc.Cluster.Status, events.events)
	}
	b.err = errors.New("inventory unavailable")
	if err := ObserveRuntimeStops(t.Context(), rc, &rc.Cluster.Status, true); err == nil || len(rc.Cluster.Status.Conditions) != 1 {
		t.Fatalf("failed read cleared stop diagnostic: %+v %v", rc.Cluster.Status, err)
	}
	b.err = nil
	b.failures = nil
	if err := ObserveRuntimeStops(t.Context(), rc, &rc.Cluster.Status, true); err != nil || len(rc.Cluster.Status.Conditions) != 0 {
		t.Fatalf("successful empty sample did not clear diagnostic: %+v %v", rc.Cluster.Status, err)
	}
}

func TestRuntimeStopObservationPreservesPersistedDiagnosticOnFreshSnapshotFailure(t *testing.T) {
	at := time.Now().UTC()
	b := &stopDiagnosticBackend{failures: []infra.RuntimeStopFailure{{Member: "m0", Allocation: "alloc-a", Message: "close open exec stdin"}}}
	events := &deploymentGuardEvents{}
	rc := &Context{Backend: b, Events: events, Clock: observationClock{at}, Cluster: &domain.Cluster{Name: "demo"}}
	p := observationProvider{observe: func(context.Context, *Context) (domain.ClusterStatus, error) {
		s := readyObservation(at)
		s.Conditions = append(s.Conditions, domain.Condition{Type: "Ready", Status: "True"})
		return s, nil
	}}
	status, err := ObserveStatus(t.Context(), p, rc)
	if err != nil {
		t.Fatal(err)
	}
	rc.Cluster.Status = status
	var original domain.Condition
	for _, c := range status.Conditions {
		if c.Type == "RuntimeStop/alloc-a" {
			original = c
		}
	}
	if original.Type == "" || len(events.events) != 1 {
		t.Fatalf("initial diagnostic missing: %+v", status)
	}
	b.err = errors.New("inventory unavailable")
	before := b.calls
	status, err = ObserveStatus(t.Context(), p, rc)
	if err == nil || b.calls-before != 2 || status.Phase == domain.PhaseReady || !status.ObservedAt.IsZero() || status.Members[0].Health || status.Members[0].Agent.Healthy {
		t.Fatalf("failed inventory renewed readiness or skipped retry: %+v %v calls=%d", status, err, b.calls-before)
	}
	found := false
	for _, c := range status.Conditions {
		if c.Type == original.Type {
			found = true
			if c != original {
				t.Fatalf("failed observation changed persisted diagnostic: %+v != %+v", c, original)
			}
		}
	}
	if !found {
		t.Fatal("fresh failed snapshot dropped the persisted stop diagnostic")
	}
	rc.Cluster.Status = status
	b.err = nil
	status, err = ObserveStatus(t.Context(), p, rc)
	if err != nil || len(events.events) != 1 {
		t.Fatalf("same recovered inventory repeated warning: %v events=%+v", err, events.events)
	}
	rc.Cluster.Status = status
	b.failures = nil
	status, err = ObserveStatus(t.Context(), p, rc)
	if err != nil || status.Phase != domain.PhaseReady {
		t.Fatalf("confirmed empty inventory did not restore readiness: %+v %v", status, err)
	}
	for _, c := range status.Conditions {
		if c.Type == original.Type {
			t.Fatal("successful empty inventory retained stop diagnostic")
		}
	}
}
