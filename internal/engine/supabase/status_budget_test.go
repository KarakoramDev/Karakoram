// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package supabase

import (
	"context"
	"encoding/json"
	"encoding/pem"
	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/supabase/sbspec"
	"github.com/KarakoramDev/Karakoram/internal/infra"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
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

type budgetAgents struct{ options agentapi.ClientOptions }

func (a budgetAgents) Dial(context.Context, domain.ClusterRef, string) (*agentapi.Client, error) {
	return agentapi.NewClient(a.options)
}
func TestComponentGenericStatusBudget(t *testing.T) {
	for _, path := range []string{"restart", "terminal-verification", "shared-component", "shared-observed-component", "native-restart", "native-terminal-verification"} {
		t.Run(path, func(t *testing.T) {
			p, rc, backend := setupProvider(t)
			convergeProvider(t, p, rc)
			s, err := loadState(rc.Cluster)
			if err != nil {
				t.Fatal(err)
			}
			component := sbspec.Components[0]
			m := s.Members[component]
			if m == nil {
				t.Fatalf("missing %s", component)
			}
			for name, spec := range rc.Cluster.Spec.Components.Extra {
				spec.Replicas = 0
				if name == component {
					spec.Replicas = 1
				}
				rc.Cluster.Spec.Components.Extra[name] = spec
			}
			containers := []infra.Container{{ID: "instance0", Hostname: "instance0", Status: infra.ContainerRunning}}
			rc.Backend = restartInventory{testBackend: backend, instances: map[string][]infra.Container{m.Name: containers}}
			native := strings.HasPrefix(path, "native-")
			server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				_, _ = io.Copy(io.Discard, r.Body)
				if native && r.URL.Path == "/v1/status" {
					_ = json.NewEncoder(w).Encode(agentapi.Status{ConfigVersion: m.ConfigVersion, Processes: map[string]agentapi.ProcessStatus{component: {State: "running", PID: 100}}})
					return
				}
				if native && r.URL.Path == "/v1/supabase/status" {
					<-r.Context().Done()
					return
				}
				if r.URL.Path != "/v1/status" {
					t.Errorf("unexpected request %s", r.URL.Path)
					http.NotFound(w, r)
					return
				}
				<-r.Context().Done()
			}))
			defer server.Close()
			rc.Agents = budgetAgents{options: agentapi.ClientOptions{BaseURL: server.URL, Token: "test", CAPEM: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw}), DialContext: func(ctx context.Context, network, address string) (net.Conn, error) {
				return (&net.Dialer{}).DialContext(ctx, network, server.Listener.Addr().String())
			}}}
			ctx, cancel := context.WithTimeout(t.Context(), 6*time.Second)
			defer cancel()
			start := time.Now()
			if strings.HasPrefix(path, "shared") {
				probe := engine.ComponentProbe{Ready: func(*agentapi.Client) (bool, error) { t.Error("native probe called without status"); return true, nil }}
				var ready bool
				if path == "shared-component" {
					ready, err = engine.ComponentInstancesHealthy(ctx, rc, m.Name, 1, m.ConfigVersion, probe)
				} else {
					ready, err = engine.ComponentObservedInstancesHealthy(ctx, rc, m.Name, 1, m.ConfigVersion, engine.InstanceInventory{Containers: containers}, probe)
				}
				if ready {
					t.Fatal("ready without status")
				}
				assertStatusBudget(t, start, err, m.Name)
				return
			}
			cp := restartCheckpoint{Stack: true, CoreDone: true}
			if native {
				cp.Instances = map[string]restartInstance{"instance0": {Done: true}}
			}
			if strings.HasSuffix(path, "terminal-verification") {
				cp.Component = len(sbspec.Components)
				cp.Completed = map[string]map[string]restartInstance{component: {"instance0": {Done: true}}}
			}
			raw, err := json.Marshal(cp)
			if err != nil {
				t.Fatal(err)
			}
			out, err := p.restartStack(ctx, rc, &domain.Operation{ID: "restart", Checkpoint: raw})
			if out.Done {
				t.Fatal("restart done without status")
			}
			if native {
				if elapsed := time.Since(start); elapsed > engine.ObservationTimeout+time.Second {
					t.Errorf("native status read took %s", elapsed)
				}
				if err == nil || !strings.Contains(err.Error(), "native status observation failed") || !strings.Contains(err.Error(), component) {
					t.Fatalf("missing component native status diagnostic: %v", err)
				}
			} else {
				assertStatusBudget(t, start, err, m.Name)
			}
		})
	}
}
