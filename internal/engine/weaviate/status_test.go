// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package weaviate

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"strings"
	"testing"
)

type unavailableAgents struct{}

func (unavailableAgents) Dial(context.Context, domain.ClusterRef, string) (*agentapi.Client, error) {
	return nil, errors.New("agent unavailable")
}

func TestStatusFreshObservationWithdrawsReadiness(t *testing.T) {
	f := coreSetup(t, 1)
	coreConverge(t, f)
	status, err := engine.ObserveStatus(t.Context(), f.p, f.rc)
	if err != nil || status.Phase != domain.PhaseReady || len(status.Members) != 1 || !status.Members[0].Health || status.Members[0].Agent.LastSeen.IsZero() {
		t.Fatalf("ready status: %+v %v", status, err)
	}
	f.quorum = false
	status, err = engine.ObserveStatus(t.Context(), f.p, f.rc)
	if status.Phase == domain.PhaseReady || status.Members[0].Health {
		t.Fatalf("unsynchronized Raft retained readiness: %+v %v", status, err)
	}
	f.quorum = true
	status, err = engine.ObserveStatus(t.Context(), f.p, f.rc)
	if err != nil || status.Phase != domain.PhaseReady {
		t.Fatalf("fresh recovered quorum did not restore readiness: %+v %v", status, err)
	}
	details, err := f.p.Connection(t.Context(), f.rc)
	if err != nil || len(details.Credentials) != 1 || details.Credentials[0].Username != AdminUser || details.Credentials[0].Password == "" || details.CACertificate == "" {
		t.Fatalf("protected credentials missing: %v", err)
	}
	public, err := json.Marshal(status)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(public), details.Credentials[0].Password) {
		t.Fatal("status leaked admin key")
	}
	original := f.rc.Agents
	f.rc.Agents = unavailableAgents{}
	status, err = engine.ObserveStatus(t.Context(), f.p, f.rc)
	if status.Phase == domain.PhaseReady || status.Members[0].Health || status.Members[0].Agent.Healthy {
		t.Fatalf("failed agent retained readiness: %+v %v", status, err)
	}
	f.rc.Agents = original
	// A successful agent sample cannot hide failed fresh native observation.
	f.rc.Dialer = coreDialer{address: "127.0.0.1:1"}
	status, err = engine.ObserveStatus(t.Context(), f.p, f.rc)
	if status.Phase == domain.PhaseReady || status.Members[0].Health {
		t.Fatalf("failed native observation retained readiness: %+v %v", status, err)
	}
	found := false
	for _, condition := range status.Conditions {
		found = found || condition.Type == "ClusterHealth" && condition.Status != "True"
	}
	if !found {
		t.Fatal("missing failed native condition")
	}
}

func TestRelayPublicExposureReconciliation(t *testing.T) {
	f := coreSetup(t, 1)
	coreConverge(t, f)
	f.rc.Cluster.Spec.Exposure.Public = true
	f.rc.Cluster.Spec.Exposure.Allowlist = []string{"192.0.2.0/24"}
	s, _ := load(f.rc.Cluster)
	for range 4 {
		before := len(f.backend.Calls())
		busy, err := f.p.reconcileExposure(t.Context(), f.rc, s)
		if err != nil {
			t.Fatal(err)
		}
		mutations := 0
		for _, call := range f.backend.Calls()[before:] {
			if call == "EnsurePublicPort" || call == "DeletePublicPorts" {
				mutations++
			}
		}
		if mutations > 1 {
			t.Fatal("multiple public mutations")
		}
		if !busy {
			break
		}
	}
	routes, err := f.backend.ListPublicPorts(t.Context())
	if err != nil || len(routes) != 2 {
		t.Fatal(routes, err)
	}
	m := s.Members[0]
	if m.PublicHTTPPort == 0 || m.PublicGRPCPort == 0 || m.PublicHTTPPort == m.PublicGRPCPort {
		t.Fatal("listener allocation collision")
	}
	for _, route := range routes {
		if route.TargetPort != 8080 && route.TargetPort != 50051 {
			t.Fatal("internal port exposed", route)
		}
	}
	busy, err := f.p.reconcileExposure(t.Context(), f.rc, s)
	if err != nil || busy {
		t.Fatal("non-idempotent public routing", busy, err)
	}
	f.rc.Cluster.Spec.Exposure.Public = false
	if _, err := f.p.reconcileExposure(t.Context(), f.rc, s); err != nil {
		t.Fatal(err)
	}
	routes, err = f.backend.ListPublicPorts(t.Context())
	if err != nil || len(routes) != 0 || m.PublicHTTPPort != 0 || m.PublicGRPCPort != 0 {
		t.Fatal("public routes retained", routes, err)
	}
}
