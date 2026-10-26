// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package elastic

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine/elastic/agentspec"
	"github.com/KarakoramDev/Karakoram/internal/infra"
)

func TestKibanaHeapExhaustionConditions(t *testing.T) {
	p, rc, f, _ := coreSetup(t, 1)
	kibana := rc.Cluster.Spec.Components.Extra[kindKibana]
	kibana.Replicas = 1
	rc.Cluster.Spec.Components.Extra[kindKibana] = kibana
	s := coreConverge(t, p, rc, f)
	var m *member
	for _, candidate := range s.componentMembers() {
		if candidate.Kind == kindKibana {
			m = candidate
		}
	}
	if m == nil {
		t.Fatal("missing Kibana")
	}
	var mode atomic.Int32
	message := "Kibana heap exhausted at the old-space cap (614 MiB); increase spec.components.extra.kibana.resources minRam and maxRam together to at least 1.5 GiB for Kibana 9.x"
	handler := func(bad bool) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			version := int64(1 << 30)
			if bad && mode.Load() == 2 {
				version = 0
			}
			switch r.URL.Path {
			case "/v1/status":
				process := agentapi.ProcessStatus{State: "running"}
				if bad && mode.Load() == 0 {
					process.State = "failed"
					process.Error = "exit status 1"
				}
				if bad && mode.Load() == 4 {
					process.State = "paused"
				}
				_ = json.NewEncoder(w).Encode(agentapi.Status{ConfigVersion: version, Maintenance: bad && mode.Load() == 5, Processes: map[string]agentapi.ProcessStatus{product(m): process}})
			case "/v1/elasticsearch/readiness":
				var heap *agentspec.HeapExhaustion
				if bad && mode.Load() != 6 {
					heap = &agentspec.HeapExhaustion{SupervisorSession: "session", Launch: 1, OldSpaceMiB: 614}
				}
				_ = json.NewEncoder(w).Encode(agentspec.Readiness{Component: "kibana", ConfigVersion: version, Ready: !bad && mode.Load() != 7 || mode.Load() == 3, HeapExhaustion: heap, MemoryPressure: !bad && mode.Load() == 7, Message: message})
			default:
				http.NotFound(w, r)
			}
		})
	}
	a := httptest.NewTLSServer(handler(false))
	defer a.Close()
	b := httptest.NewUnstartedServer(handler(true))
	b.TLS = a.TLS.Clone()
	b.StartTLS()
	defer b.Close()
	m.Containers = 2
	kibana.Replicas = 2
	rc.Cluster.Spec.Components.Extra[kindKibana] = kibana
	if err := save(rc.Cluster, s); err != nil {
		t.Fatal(err)
	}
	rc.Backend = &componentInstanceBackend{coreInfra: f, member: m.Name, instances: []infra.Container{{ID: "allocation-a", Hostname: "instance-a", Status: infra.ContainerRunning}, {ID: "allocation-b", Hostname: "instance-b", Status: infra.ContainerRunning}}}
	rc.Agents = componentInstanceDialer{AgentDialer: rc.Agents, member: m.Name, first: a, second: b}
	for _, tc := range []struct {
		name        string
		mode        int32
		heap, ready bool
	}{{"failed child", 0, true, false}, {"running retry", 1, true, false}, {"stale configuration", 2, false, false}, {"healthy native wins", 3, false, true}, {"paused child", 4, false, false}, {"maintenance", 5, false, false}, {"legacy agent", 6, false, false}, {"pressured sibling and heap-failed sibling", 7, true, false}} {
		t.Run(tc.name, func(t *testing.T) {
			mode.Store(tc.mode)
			ready, err := p.componentReadyResult(t.Context(), rc, m)
			if err != nil || ready != tc.ready || m.ConfigurationWarning != "" {
				t.Fatalf("heap failure became invalid intent: ready=%v err=%v warning=%q", ready, err, m.ConfigurationWarning)
			}
			status, err := p.Status(t.Context(), rc)
			if err != nil {
				t.Fatal(err)
			}
			found, processFailure, pressure := false, false, false
			for _, c := range status.Conditions {
				if c.Type == "KibanaHeapExhausted" {
					found = c.Status == "True" && c.Reason == "HeapExhausted" && strings.Contains(c.Message, m.Name) && strings.Contains(c.Message, "allocation-b") && strings.Contains(c.Message, "1.5 GiB")
				}
				if c.Reason == "ProcessError" {
					processFailure = true
				}
				if c.Reason == "CgroupMemoryPressure" {
					pressure = true
				}
			}
			if found != tc.heap || (!tc.ready && status.Phase == domain.PhaseReady) || tc.mode == 0 && !processFailure {
				t.Fatalf("incorrect heap/process readiness composition: %+v", status)
			}
			if tc.mode == 7 && !pressure {
				t.Fatal("heap diagnosis hid independent cgroup pressure")
			}
		})
	}
}

func TestKibanaLegacyMinimumRAMConditionMatrix(t *testing.T) {
	p, rc, f, _ := coreSetup(t, 1)
	coreConverge(t, p, rc, f)
	for _, tc := range []struct {
		version  string
		min      float64
		replicas int
		want     bool
	}{{"9.5.5", 1, 1, true}, {"9.5.4", 1.25, 1, true}, {"9.4.8", 1.49, 2, true}, {"9.5.5", 1.5, 1, false}, {"9.5.5", 2, 1, false}, {"8.19.23", 1, 1, false}, {"9.5.5", 1, 0, false}} {
		t.Run(tc.version+"/"+strings.Repeat("x", tc.replicas), func(t *testing.T) {
			rc.Cluster.Spec.Version = tc.version
			kibana := rc.Cluster.Spec.Components.Extra[kindKibana]
			kibana.Replicas = tc.replicas
			kibana.Resources.MinRAM = tc.min
			kibana.Resources.MaxRAM = tc.min
			rc.Cluster.Spec.Components.Extra[kindKibana] = kibana
			status, err := p.Status(t.Context(), rc)
			if err != nil {
				t.Fatal(err)
			}
			found := false
			for _, c := range status.Conditions {
				if c.Type == "KibanaResources" {
					found = c.Status == "False" && c.Reason == "BelowMinimumRAM" && strings.Contains(c.Message, "1.5-GiB minimum for 9.x") && strings.Contains(c.Message, "minRam and maxRam together to at least 1.5 GiB")
				}
			}
			if found != tc.want {
				t.Fatalf("legacy minimum condition mismatch: %+v", status.Conditions)
			}
			if rc.Cluster.Spec.Components.Extra[kindKibana].Resources.MinRAM != tc.min {
				t.Fatal("status mutated resources")
			}
		})
	}
}
