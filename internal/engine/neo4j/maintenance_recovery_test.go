// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package neo4j

import (
	"encoding/json"
	"fmt"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine/neo4j/neo4jspec"
	"testing"
	"time"
)

func TestMaintenanceDeferredChangeDoesNotBlockRuntimeRecovery(t *testing.T) {
	for _, change := range []string{"resources", "version"} {
		t.Run(change, func(t *testing.T) {
			p, rc, b, _ := setup(t)
			rc.Cluster.Spec.Version = "2026.09.0"
			converge(t, p, rc)
			before, _ := load(rc.Cluster)
			old := before.Member.Resources
			volume := before.Member.VolumeID
			rc.Cluster.Spec.Maintenance = &domain.MaintenancePolicy{Windows: []domain.MaintenanceWindow{{Schedule: fmt.Sprintf("0 0 1 %d *", int(time.Now().Month())%12+1), Duration: "1h"}}}
			rc.Cluster.Generation++
			if change == "resources" {
				rc.Cluster.Spec.Components.Engine.Resources.MinRAM++
				rc.Cluster.Spec.Components.Engine.Resources.MaxRAM++
			} else {
				rc.Cluster.Spec.Version = defaultVersion
			}
			if _, err := b.DeleteService(t.Context(), before.Member.Name); err != nil {
				t.Fatal(err)
			}
			recovered := false
			for range 50 {
				if _, err := p.Sync(t.Context(), rc); err != nil {
					t.Fatal(err)
				}
				s, _ := load(rc.Cluster)
				if s.Member.Stage == "deployed" && s.Member.Pending == nil {
					recovered = true
					break
				}
			}
			s, _ := load(rc.Cluster)
			if !recovered || s.Member.VolumeID != volume || s.Member.Resources != old || s.Member.Version != "2026.09.0" {
				t.Fatal("deferred desired change blocked accepted-runtime recovery")
			}
			cfg, err := p.MemberConfig(t.Context(), rc, s.Member.Name)
			if err != nil {
				t.Fatal(err)
			}
			var spec neo4jspec.Spec
			if err = json.Unmarshal(cfg.EngineSpec, &spec); err != nil || spec.Version != "2026.09.0" {
				t.Fatal("recovery installed deferred version", err)
			}
		})
	}
}
