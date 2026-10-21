// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package openbao

import (
	"context"
	"encoding/json"
	"encoding/pem"
	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/openbao/baospec"
	"github.com/KarakoramDev/Karakoram/internal/infra"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
)

func (edgeBackend) Capabilities() infra.Capabilities { return infra.Capabilities{} }
func (edgeBackend) ListServices(context.Context, string) ([]infra.Service, error) {
	return []infra.Service{{Hostname: "baom0"}}, nil
}
func (edgeBackend) Containers(context.Context, string) ([]infra.Container, error) {
	return []infra.Container{{Status: infra.ContainerRunning}}, nil
}

type edgeAgents struct {
	url     string
	ca      []byte
	planned atomic.Int32
}

func (a *edgeAgents) Dial(_ context.Context, _ domain.ClusterRef, name string) (*agentapi.Client, error) {
	if name == "baom1" {
		a.planned.Add(1)
		return nil, engine.ErrUnavailable
	}
	return agentapi.NewClient(agentapi.ClientOptions{BaseURL: a.url, Token: "test-token", CAPEM: a.ca})
}
func TestEdgeStatusNativeMapping(t *testing.T) {
	for _, tc := range []struct {
		name                string
		native              baospec.StatusResponse
		role, state         string
		healthy, sealed     bool
		voter, voterHealthy bool
	}{{"active", baospec.StatusResponse{Initialized: true, Active: true}, "primary", "RUNNING", true, false, true, true}, {"standby", baospec.StatusResponse{Initialized: true, Standby: true}, "secondary", "RUNNING", true, false, true, true}, {"sealed", baospec.StatusResponse{Initialized: true, Sealed: true}, "unknown", "SEALED", false, true, true, false}, {"fresh", baospec.StatusResponse{}, "unknown", "UNINITIALIZED", false, false, false, false}, {"non-voter", baospec.StatusResponse{Initialized: true, Standby: true}, "secondary", "RUNNING", false, false, false, true}, {"unhealthy-voter", baospec.StatusResponse{Initialized: true, Standby: true}, "secondary", "RUNNING", false, false, true, false}} {
		t.Run(tc.name, func(t *testing.T) {
			p, rc, s := edgeFixture(t)
			s.Phase = "ready"
			s.Members[0].Voter = tc.voter
			s.Members[0].Healthy = tc.voterHealthy
			s.QuorumBlocked = "A live majority is required"
			s.InitializationOutcomeLost = "receipt missing"
			if err := s.save(rc.Cluster); err != nil {
				t.Fatal(err)
			}
			tc.native.Version = "2.7.1"
			server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				switch r.URL.Path {
				case "/v1/status":
					json.NewEncoder(w).Encode(agentapi.Status{ConfigVersion: 1, HostFingerprint: "host-a", Processes: map[string]agentapi.ProcessStatus{baospec.Process: {State: "running"}}})
				case "/v1/openbao/status":
					json.NewEncoder(w).Encode(tc.native)
				default:
					http.NotFound(w, r)
				}
			}))
			defer server.Close()
			dialer := &edgeAgents{url: server.URL, ca: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw})}
			rc.Agents = dialer
			v, err := p.Status(t.Context(), rc)
			if err != nil {
				t.Fatal(err)
			}
			if len(v.Members) != 2 {
				t.Fatal(v.Members)
			}
			m := v.Members[0]
			if m.Role != tc.role || m.State != tc.state || m.Health != tc.healthy || m.Version != "2.7.1" {
				t.Fatalf("member=%+v", m)
			}
			if dialer.planned.Load() != 0 {
				t.Fatal("probed a planned member")
			}
			conditions := map[string]string{}
			for _, c := range v.Conditions {
				conditions[c.Type] = c.Status
			}
			wantVoter := "False"
			if tc.voter {
				wantVoter = "True"
			}
			if conditions["RaftVoter/baom0"] != wantVoter {
				t.Fatalf("voter condition: %+v", v.Conditions)
			}
			wantSealed := "False"
			if tc.sealed {
				wantSealed = "True"
			}
			if conditions["Sealed"] != wantSealed || conditions["QuorumBlocked"] != "True" || conditions["InitializationOutcomeLost"] != "True" {
				t.Fatal(v.Conditions)
			}
		})
	}
}

func TestEdgePhaseMapping(t *testing.T) {
	for _, tc := range []struct {
		phase string
		want  domain.Phase
	}{{"initializing", domain.PhaseInitializing}, {"initialization-outcome-lost", domain.PhaseDegraded}} {
		if got := phase(&internalState{Phase: tc.phase}); got != tc.want {
			t.Fatalf("phase %q = %q, want %q", tc.phase, got, tc.want)
		}
	}
}
