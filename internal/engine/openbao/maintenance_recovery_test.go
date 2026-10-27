// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package openbao

import (
	"fmt"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"testing"
	"time"
)

func TestMaintenanceScaleInDoesNotBlockUnseal(t *testing.T) {
	p, rc, _, agents := setup(t)
	ready(t, p, rc)
	rc.Cluster.Spec.Maintenance = &domain.MaintenancePolicy{Windows: []domain.MaintenanceWindow{{Schedule: fmt.Sprintf("0 0 1 %d *", int(time.Now().Month())%12+1), Duration: "1h"}}}
	rc.Cluster.Spec.Components.Engine.Replicas = 1
	rc.Cluster.Generation++
	agents.member("demom1").sealed = true
	agents.member("demom1").healthy = false
	recovered := false
	for range 60 {
		if _, err := p.Sync(t.Context(), rc); err != nil {
			t.Fatal(err)
		}
		s := state(t, rc)
		for _, m := range s.Members {
			if m.Removing {
				t.Fatal("closed scale-in retired a member")
			}
		}
		if !agents.member("demom1").sealed && s.find("demom1").Healthy {
			recovered = true
			break
		}
	}
	if !recovered {
		t.Fatal("deferred scale-in blocked exempt unseal")
	}
	if len(state(t, rc).Members) != 3 {
		t.Fatal("scale-in was not deferred")
	}
}
