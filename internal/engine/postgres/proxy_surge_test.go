// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package postgres

import (
	"encoding/json"
	"fmt"
	"slices"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/infra"
)

type proxySurgeBackend struct {
	infra.Backend
	surge int
}

func (b proxySurgeBackend) Capabilities() infra.Capabilities {
	c := b.Backend.Capabilities()
	c.StatelessRolloutSurge = b.surge
	return c
}

func TestProxyPoolReservesEveryPermittedRolloutInstance(t *testing.T) {
	p, rc, _ := setup(t, "ha")
	s := converge(t, p, rc)
	original := rc.Backend
	proxy := s.find("demopx")
	for _, surge := range []int{0, 1} {
		for _, counts := range [][2]int{{1, 1}, {2, 2}, {1, 3}, {3, 1}} {
			for _, limits := range [][2]int{{100, 100}, {100, 200}, {200, 100}} {
				t.Run(fmt.Sprintf("surge%d/%d-to-%d/%d-to-%d", surge, counts[0], counts[1], limits[0], limits[1]), func(t *testing.T) {
					rc.Backend = proxySurgeBackend{original, surge}
					proxy.Replicas = counts[0]
					c := rc.Cluster.Spec.Components.Extra["proxy"]
					c.Replicas = counts[1]
					rc.Cluster.Spec.Components.Extra["proxy"] = c
					rc.Cluster.Spec.EngineOptions = parameterOptions(map[string]string{"max_connections": fmt.Sprint(limits[1])})
					for _, m := range s.data() {
						m.Options = parameterOptions(map[string]string{"max_connections": fmt.Sprint(limits[0])})
					}
					limit, err := p.proxyConnectionLimit(rc, s, proxy)
					if err != nil {
						t.Fatal(err)
					}
					instances := counts[0] + counts[1]
					if surge > 0 {
						instances = max(counts[0], counts[1]) + surge
					}
					if used := instances*(limit+3) + 8; used > min(limits[0], limits[1]) {
						t.Fatalf("permitted serving set overbooks PostgreSQL: %d > %d", used, min(limits[0], limits[1]))
					}
				})
			}
		}
	}
}

func TestProxyResizeWaitsForSurgeBudgetAcknowledgement(t *testing.T) {
	p, rc, b, _, first, late := replicatedProxyFixture(t)
	component := rc.Cluster.Spec.Components.Extra["proxy"]
	component.Resources.MaxCPU++
	rc.Cluster.Spec.Components.Extra["proxy"] = component
	rc.Cluster.Generation++
	b.actions = nil
	if _, err := p.Sync(t.Context(), rc); err != nil {
		t.Fatal(err)
	}
	s, err := load(rc.Cluster)
	if err != nil {
		t.Fatal(err)
	}
	version := s.ConfigVersion
	first.applied(version)
	for range 4 {
		if _, err := p.Sync(t.Context(), rc); err != nil {
			t.Fatal(err)
		}
	}
	if slices.Contains(b.actions, "resize:demopx") {
		t.Fatal("resource rollout started before late existing instance acknowledged surge budget")
	}
	config, err := p.MemberConfig(t.Context(), rc, "demopx")
	if err != nil {
		t.Fatal(err)
	}
	var spec struct {
		Proxy struct {
			MaxConnections int `json:"maxConnections"`
		} `json:"proxy"`
	}
	if err := json.Unmarshal(config.EngineSpec, &spec); err != nil {
		t.Fatal(err)
	}
	if spec.Proxy.MaxConnections > 43 {
		t.Fatalf("singleton rollout can serve two overcommitted pools: %d", spec.Proxy.MaxConnections)
	}
	late.applied(version)
	for range 8 {
		if _, err := p.Sync(t.Context(), rc); err != nil {
			t.Fatal(err)
		}
		if slices.Contains(b.actions, "resize:demopx") {
			return
		}
	}
	t.Fatal("acknowledged surge-safe budget did not release resource rollout")
}
