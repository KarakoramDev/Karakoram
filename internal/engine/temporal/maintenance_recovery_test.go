// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package temporal

import (
	"encoding/json"
	"fmt"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine/temporal/tspec"
	"testing"
	"time"
)

func TestMaintenanceDeferredChangeDoesNotBlockOtherRoleCredentials(t *testing.T) {
	for _, change := range []string{"resources", "version"} {
		t.Run(change, func(t *testing.T) {
			p, rc, bindings, _ := fixture(t, "split")
			converge(t, p, rc)
			before, _ := loadState(rc.Cluster)
			resources := before.Members[tspec.History].Resources
			tokens := map[string]string{}
			for name, m := range before.Members {
				tokens[name] = m.ConfigHash
			}
			rc.Cluster.Spec.Maintenance = &domain.MaintenancePolicy{Windows: []domain.MaintenanceWindow{{Schedule: fmt.Sprintf("0 0 1 %d *", int(time.Now().Month())%12+1), Duration: "1h"}}}
			rc.Cluster.Generation++
			if change == "resources" {
				d := rc.Cluster.Spec.Components.Extra[tspec.History]
				d.Resources.MinRAM++
				d.Resources.MaxRAM++
				rc.Cluster.Spec.Components.Extra[tspec.History] = d
			} else {
				rc.Cluster.Spec.Version = "1.32.1"
			}
			bindings.conn.CredentialGeneration++
			bindings.conn.Username = "rotated_owner"
			settled := false
			for range 100 {
				if _, err := p.Sync(t.Context(), rc); err != nil {
					t.Fatal(err)
				}
				s, _ := loadState(rc.Cluster)
				if s.CredentialGeneration == bindings.conn.CredentialGeneration {
					settled = true
					break
				}
			}
			s, _ := loadState(rc.Cluster)
			if !settled || s.Members[tspec.History].Resources != resources || s.Version != "1.31.4" {
				t.Fatal("deferred desired change blocked credential settlement")
			}
			for name, m := range s.Members {
				cfg, err := p.MemberConfig(t.Context(), rc, m.Name)
				if err != nil {
					t.Fatal(err)
				}
				var spec tspec.Spec
				if err = json.Unmarshal(cfg.EngineSpec, &spec); err != nil {
					t.Fatal(err)
				}
				if m.ConfigHash == tokens[name] || spec.Persistence != nil && spec.Persistence.Username != "rotated_owner" {
					t.Fatal("role retained obsolete credentials", name)
				}
			}
		})
	}
}
