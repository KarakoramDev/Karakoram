// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package rabbitmq

import (
	"reflect"
	"strings"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/engine/enginetest"
)

func TestCPUpgradeRetainsExistingRabbitMQRuntime(t *testing.T) {
	p, rc, backend, _ := setup(t)
	f := enginetest.RestoreCPUpgrade(t, rc, "testdata/cp_upgrade_ed9541a6.json")
	for i := range f.Services {
		service := f.Services[i]
		backend.services[service.Hostname] = &service
	}
	s := state(t, rc)
	before := make(map[string]any)
	for _, m := range s.Members {
		cfg, err := p.MemberConfig(t.Context(), rc, m.Name)
		if err != nil {
			t.Fatal(err)
		}
		before[m.Name] = cfg
	}
	old := strings.Join(f.Runtime.Prepare, "\n")
	current, err := PrepareCommands(rc.Cluster.Spec.Version)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(old, "4.3.6") || !strings.Contains(old, "27.3.4.17") || reflect.DeepEqual(f.Runtime.Prepare, current) {
		t.Fatal("fixture does not cross broker/Erlang preparation change")
	}
	for range 12 {
		p.ApplyDefaults(&rc.Cluster.Spec)
		if _, err := p.Sync(t.Context(), rc); err != nil {
			t.Fatal(err)
		}
		for _, m := range s.Members {
			cfg, err := p.MemberConfig(t.Context(), rc, m.Name)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(cfg, before[m.Name]) {
				t.Fatalf("CP-only upgrade activated new config for %s", m.Name)
			}
		}
	}
	if rc.Cluster.Spec.Version != "4.3.6" || len(backend.deployments) != 0 {
		t.Fatalf("CP-only upgrade replaced existing broker/Erlang: version=%s deployments=%d", rc.Cluster.Spec.Version, len(backend.deployments))
	}
}
