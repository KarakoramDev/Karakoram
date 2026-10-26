// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package rabbitmq

import (
	"encoding/json"
	"encoding/pem"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/domain"
)

func TestAdminCredentialDriftWithdrawsReadyUntilVerified(t *testing.T) {
	p, rc, _, _ := setup(t)
	ready(t, p, rc)
	signal := ""
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/status":
			_ = json.NewEncoder(w).Encode(agentapi.Status{Processes: map[string]agentapi.ProcessStatus{"rabbitmq-server": {State: "running"}}})
		case "/v1/rabbitmq/observe":
			_ = json.NewEncoder(w).Encode(map[string]any{"version": rc.Cluster.Spec.Version, "runningNodes": []string{"rabbit@demom0", "rabbit@demom1", "rabbit@demom2"}, "adminCredentialStatus": signal})
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	rc.Agents = coldObservationAgents{options: agentapi.ClientOptions{BaseURL: server.URL, Token: "test-token", CAPEM: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw})}}
	for _, step := range []struct {
		signal string
		ready  bool
		drift  bool
	}{
		{"", true, false}, {"unknown", true, false}, {"rejected", false, true}, {"unknown", false, true}, {"accepted", true, false}, {"unknown", true, false},
	} {
		signal = step.signal
		status, err := p.Status(t.Context(), rc)
		if err != nil {
			t.Fatal(err)
		}
		if (status.Phase == domain.PhaseReady) != step.ready {
			t.Fatalf("signal %q: phase=%s, want Ready=%v", signal, status.Phase, step.ready)
		}
		drift := false
		for _, c := range status.Conditions {
			if c.Type == "ManagedAccessReady" && c.Status == "False" && c.Reason == "CredentialDrift" {
				drift = true
			}
		}
		if drift != step.drift {
			t.Fatalf("signal %q: actionable drift condition=%v", signal, drift)
		}
		rc.Cluster.Status = status
	}
}

func TestAdminRotationRejectionsAreNotCredentialDrift(t *testing.T) {
	p, rc, _, _ := setup(t)
	ready(t, p, rc)
	op := &domain.Operation{ID: "admin-rotation", Kind: domain.OperationRotateSystemUser, Payload: []byte(`{"user":"admin"}`)}
	for range 4 {
		result, err := p.RunOperation(t.Context(), rc, op)
		if err != nil {
			t.Fatal(err)
		}
		op.Checkpoint, op.Step = result.Checkpoint, result.Step
		if op.Step == "password-persisted" {
			break
		}
	}
	// Observe a staged-owner rejection first, then a committed password with
	// an old peer snapshot, then the same rejection at the applied epoch.
	s := state(t, rc)
	currentEpoch := s.ConfigVersion
	epoch := currentEpoch - 1
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/status":
			_ = json.NewEncoder(w).Encode(agentapi.Status{ConfigVersion: currentEpoch, Processes: map[string]agentapi.ProcessStatus{"rabbitmq-server": {State: "running"}}})
		case "/v1/rabbitmq/observe":
			_ = json.NewEncoder(w).Encode(map[string]any{"version": rc.Cluster.Spec.Version, "runningNodes": []string{"rabbit@demom0", "rabbit@demom1", "rabbit@demom2"}, "adminCredentialStatus": "rejected", "configVersion": epoch})
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	rc.Agents = coldObservationAgents{options: agentapi.ClientOptions{BaseURL: server.URL, Token: "test-token", CAPEM: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw})}}
	for _, staged := range []bool{true, false} {
		if staged {
			epoch = currentEpoch
		} else {
			epoch = currentEpoch - 1
		}
		s = state(t, rc)
		m := s.find("demom0")
		if staged {
			m.ChangeAction, m.TargetConfig = "password", "admin"
		} else {
			m.ChangeAction, m.TargetConfig = "", ""
		}
		if err := s.save(rc.Cluster); err != nil {
			t.Fatal(err)
		}
		status, err := p.Status(t.Context(), rc)
		if err != nil {
			t.Fatal(err)
		}
		for _, c := range status.Conditions {
			if c.Reason == "CredentialDrift" {
				t.Fatalf("expected rotation rejection classified as drift (staged=%v): %+v", staged, c)
			}
		}
		rc.Cluster.Status = status
	}
	epoch = currentEpoch
	status, err := p.Status(t.Context(), rc)
	if err != nil {
		t.Fatal(err)
	}
	drift := false
	for _, c := range status.Conditions {
		drift = drift || c.Reason == "CredentialDrift"
	}
	if !drift {
		t.Fatal("applied desired credential rejection failed to establish drift")
	}
}
