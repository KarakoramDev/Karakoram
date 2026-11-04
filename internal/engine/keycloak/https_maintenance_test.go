// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package keycloak

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine/keycloak/kcspec"
)

func TestMaintenanceHTTPSPatchDoesNotMutateRuntime(t *testing.T) {
	for _, release := range []string{"open", "override"} {
		t.Run(release, func(t *testing.T) {
			p, rc, bindings, secrets := setup(t)
			converge(t, p, rc)
			rc.Cluster.Status.ObservedGeneration = 1
			rc.Cluster.Generation = 2
			rc.Cluster.Spec.Maintenance = &domain.MaintenancePolicy{Windows: []domain.MaintenanceWindow{{Schedule: fmt.Sprintf("0 0 1 %d *", int(time.Now().Month())%12+1), Duration: "1h"}}}
			before, _ := load(rc.Cluster)
			token := secrets.token.Token
			deploys, resizes := bindings.backend.deploys, bindings.backend.resizes
			rc.Cluster.Spec.EngineOptions = json.RawMessage(`{"database":{"cluster":"pg"},"hostname":"https://identity.example.test","proxyHeaders":"xforwarded","publicHttps":true}`)
			for range 4 {
				if _, err := p.Sync(t.Context(), rc); err != nil {
					t.Fatal(err)
				}
			}
			s, _ := load(rc.Cluster)
			if bindings.backend.public || s.Public || s.Pending != nil || s.ConfigVersion != before.ConfigVersion || bindings.backend.deploys != deploys || bindings.backend.resizes != resizes || secrets.token.Token != token {
				t.Fatal("closed window mutated runtime, configuration or credentials")
			}
			if rc.Cluster.Status.ObservedGeneration != 1 {
				t.Fatal("deferred generation acknowledged")
			}
			if release == "open" {
				rc.Cluster.Spec.Maintenance.Windows[0].Schedule = "* * * * *"
			} else {
				rc.Cluster.MaintenanceOverrideGeneration = 2
			}
			converge(t, p, rc)
			s, _ = load(rc.Cluster)
			config, err := p.MemberConfig(t.Context(), rc, s.Member)
			if err != nil {
				t.Fatal(err)
			}
			var spec kcspec.Spec
			if err := json.Unmarshal(config.EngineSpec, &spec); err != nil {
				t.Fatal(err)
			}
			if !s.Public || !bindings.backend.public || spec.Hostname != "https://identity.example.test" || spec.ProxyHeaders != "xforwarded" {
				t.Fatal("released PATCH did not apply routing and settings")
			}
			rc.Cluster.Spec.Maintenance.Windows[0].Schedule = fmt.Sprintf("0 0 1 %d *", int(time.Now().Month())%12+1)
			rc.Cluster.MaintenanceOverrideGeneration = 0
			rc.Cluster.Generation = 3
			rc.Cluster.Status.ObservedGeneration = 2
			rc.Cluster.Spec.EngineOptions = json.RawMessage(`{"database":{"cluster":"pg"},"hostname":"https://identity.example.test","proxyHeaders":"xforwarded"}`)
			converge(t, p, rc)
			if !bindings.backend.public {
				t.Fatal("closed window revoked route through a runtime update")
			}
		})
	}
}
