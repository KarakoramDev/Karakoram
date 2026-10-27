// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package keycloak

import (
	"context"
	"encoding/json"
	"encoding/pem"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine/enginetest"
	"github.com/KarakoramDev/Karakoram/internal/infra"
)

type idleAgents struct{ server *httptest.Server }

func (a idleAgents) Dial(context.Context, domain.ClusterRef, string) (*agentapi.Client, error) {
	u, err := url.Parse(a.server.URL)
	if err != nil {
		return nil, err
	}
	return agentapi.NewClient(agentapi.ClientOptions{BaseURL: a.server.URL, Token: "test-token", CAPEM: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: a.server.Certificate().Raw}), DialInstanceContext: func(ctx context.Context, _, _, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, "tcp", u.Host)
	}})
}

type idleBackend struct{ *backendFake }

func (b idleBackend) Containers(_ context.Context, name string) ([]infra.Container, error) {
	if b.service == nil || b.service.Status != infra.StatusRunning {
		return nil, nil
	}
	return []infra.Container{{ID: name + "-instance", Hostname: name + "-instance", Status: infra.ContainerRunning, IPv6: "2001:db8::1"}}, nil
}
func (b idleBackend) Deploy(ctx context.Context, name string, d infra.Deployment) (infra.Op, error) {
	op, err := b.backendFake.Deploy(ctx, name, d)
	if err == nil {
		b.service.Status = infra.StatusRunning
	}
	return op, err
}

func TestIdleReconcileZeroProviderMutations(t *testing.T) {
	t.Run("single", func(t *testing.T) {
		p, rc, _, _ := setup(t)
		rc.Backend = idleBackend{rc.Backend.(*backendFake)}
		server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			s, _ := load(rc.Cluster)
			if r.URL.Path == "/v1/status" {
				json.NewEncoder(w).Encode(agentapi.Status{ConfigVersion: s.ConfigVersion})
				return
			}
			if r.URL.Path == "/v1/keycloak/status" {
				json.NewEncoder(w).Encode(map[string]bool{"ready": true})
				return
			}
			json.NewEncoder(w).Encode(agentapi.ActionResponse{Success: true})
		}))
		t.Cleanup(server.Close)
		rc.Agents = idleAgents{server}
		enginetest.AssertIdle(t, p, rc, nil)
	})
}
