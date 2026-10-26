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

func TestKibanaMemoryPressureIsActionableNonterminalAndFresh(t *testing.T) {
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
		t.Fatal("Kibana missing")
	}
	var mode atomic.Int32
	message := "Kibana memory pressure observed; increase minRam and maxRam"
	handler := func(bad bool) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			version := int64(1 << 30)
			if bad && (mode.Load() == 1 || mode.Load() == 6) {
				version = 0
			}
			switch r.URL.Path {
			case "/v1/status":
				_ = json.NewEncoder(w).Encode(agentapi.Status{ConfigVersion: version, Processes: map[string]agentapi.ProcessStatus{product(m): {State: "running"}}})
			case "/v1/elasticsearch/readiness":
				unavailable := bad && mode.Load() >= 4 && mode.Load() <= 6 || !bad && mode.Load() == 7
				_ = json.NewEncoder(w).Encode(agentspec.Readiness{Component: "kibana", Ready: !bad && mode.Load() != 7 || mode.Load() == 2 || mode.Load() == 5, ConfigVersion: version, MemoryPressure: bad && mode.Load() != 3 && !unavailable, MemoryPressureUnavailable: unavailable, Message: message})
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
	desired := rc.Cluster.Spec.Components.Extra[kindKibana]
	desired.Replicas = 2
	rc.Cluster.Spec.Components.Extra[kindKibana] = desired
	if err := save(rc.Cluster, s); err != nil {
		t.Fatal(err)
	}
	rc.Backend = &componentInstanceBackend{coreInfra: f, member: m.Name, instances: []infra.Container{{ID: "allocation-a", Hostname: "instance-a", Status: infra.ContainerRunning}, {ID: "allocation-b", Hostname: "instance-b", Status: infra.ContainerRunning}}}
	rc.Agents = componentInstanceDialer{AgentDialer: rc.Agents, member: m.Name, first: a, second: b}
	for _, tc := range []struct {
		name            string
		mode            int32
		pressure, ready bool
		unknown         bool
	}{
		{"pressured sibling", 0, true, false, false},
		{"stale observation", 1, false, false, false},
		{"native recovery even with pressure flag", 2, false, true, false},
		{"legacy unready agent without pressure signal", 3, false, false, false},
		{"runtime evidence unavailable", 4, false, false, true},
		{"native recovery even with unavailable evidence", 5, false, true, false},
		{"stale unavailable observation", 6, false, false, false},
		{"measured pressure wins over unknown sibling", 7, true, false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mode.Store(tc.mode)
			ready, err := p.componentReadyResult(t.Context(), rc, m)
			if err != nil || ready != tc.ready || m.ConfigurationWarning != "" {
				t.Fatalf("pressure became a configuration error or readiness override: ready=%v err=%v warning=%q", ready, err, m.ConfigurationWarning)
			}
			status, err := p.Status(t.Context(), rc)
			if err != nil {
				t.Fatal(err)
			}
			found := false
			unknown := false
			for _, condition := range status.Conditions {
				if condition.Type == "MemoryPressure" {
					found = condition.Status == "True" && condition.Reason == "CgroupMemoryPressure" && strings.Contains(condition.Message, m.Name) && strings.Contains(condition.Message, "allocation-b") && strings.Contains(condition.Message, "minRam and maxRam")
					unknown = condition.Status == "Unknown" && condition.Reason == "CgroupMemoryEvidenceUnavailable" && strings.Contains(condition.Message, m.Name) && strings.Contains(condition.Message, "allocation-b")
				}
			}
			if found != tc.pressure || unknown != tc.unknown || (!tc.ready && status.Phase == domain.PhaseReady) {
				t.Fatalf("incorrect fresh pressure/native readiness projection: %+v", status)
			}
		})
	}
}
