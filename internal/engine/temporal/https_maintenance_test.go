// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package temporal

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/temporal/tspec"
)

func TestMaintenanceHTTPSPatchDoesNotMutateRuntime(t *testing.T) {
	for _, public := range []bool{false, true} {
		t.Run(fmt.Sprint(public), func(t *testing.T) {
			p, rc, _, _ := fixture(t, "combined")
			converge(t, p, rc)
			s, err := loadState(rc.Cluster)
			if err != nil {
				t.Fatal(err)
			}
			m := s.Members[tspec.UI]
			// Exercise enable and disable at the provider's ingress boundary. A nil
			// backend makes any accidental platform call fail instead of hiding a restart.
			m.Public = !public
			o, err := optionsFor(rc.Cluster)
			if err != nil {
				t.Fatal(err)
			}
			o.UI.PublicHTTPS = public
			rc.Cluster.Spec.EngineOptions, _ = json.Marshal(o)
			rc.Cluster.Generation = 2
			rc.Cluster.Status.ObservedGeneration = 1
			rc.Cluster.Spec.Maintenance = &domain.MaintenancePolicy{Windows: []domain.MaintenanceWindow{{Schedule: fmt.Sprintf("0 0 1 %d *", int(time.Now().Month())%12+1), Duration: "1h"}}}
			rc.Backend = nil
			for _, stage := range []engine.ComponentStage{engine.ComponentJoined, engine.ComponentImported} {
				m.Stage = stage
				busy, err := syncUIExposure(t.Context(), rc, s, m)
				if err != nil || !busy || s.Pending != nil || m.Public == public {
					t.Fatal("closed window crossed ingress boundary", busy, err)
				}
			}
		})
	}
}
