// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package postgres

import (
	"context"
	"encoding/json"
	"encoding/pem"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/postgres/pgspec"
)

type coldObservationAgents struct{ options agentapi.ClientOptions }

func (d coldObservationAgents) Dial(_ context.Context, _ domain.ClusterRef, name string) (*agentapi.Client, error) {
	o := d.options
	o.Token = name
	return agentapi.NewClient(o)
}

func waitObservationLatency(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func TestObservationColdTLSAndSlowNativeFitsSweep(t *testing.T) {
	p, rc, _ := setup(t, "ha")
	s := converge(t, p, rc)
	statuses := make(map[string]pgspec.StatusResponse, len(s.Members))
	for _, m := range s.Members {
		st, err := live(t.Context(), rc, m.Name)
		if err != nil {
			t.Fatal(err)
		}
		statuses[m.Name] = st
	}
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		delay := time.Second
		if r.URL.Path == "/v1/postgres/status" {
			delay = 1500 * time.Millisecond
		}
		if err := waitObservationLatency(r.Context(), delay); err != nil {
			return
		}
		switch r.URL.Path {
		case "/v1/status":
			_ = json.NewEncoder(w).Encode(agentapi.Status{ConfigVersion: s.ConfigVersion})
		case "/v1/postgres/status":
			_ = json.NewEncoder(w).Encode(statuses[strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")])
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	rc.Clock = engine.RealClock{}
	rc.Agents = coldObservationAgents{options: agentapi.ClientOptions{
		BaseURL: server.URL, CAPEM: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw}),
		DialContext: func(ctx context.Context, network, address string) (net.Conn, error) {
			if err := waitObservationLatency(ctx, 1500*time.Millisecond); err != nil {
				return nil, err
			}
			return (&net.Dialer{}).DialContext(ctx, network, address)
		},
	}}
	// The data-member observation is the shared per-member budget boundary;
	// the two HTTP clients have independent cold TLS connections.
	ctx, cancel := context.WithTimeout(t.Context(), engine.ObservationSweepTimeout)
	defer cancel()
	observed := observeStatusMembers(ctx, rc, s.data())
	for i, sample := range observed {
		if sample.err != nil || !sample.agentHealthy || !sample.status.Running {
			t.Fatalf("cold healthy member %s lost fresh availability: %+v", s.data()[i].Name, sample)
		}
	}
}
