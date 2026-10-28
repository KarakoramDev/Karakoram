// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package supabase

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
)

func TestMaintenanceHTTPSPatchDoesNotMutateRuntime(t *testing.T) {
	for _, initialPublic := range []bool{false, true} {
		t.Run(fmt.Sprint(initialPublic), func(t *testing.T) {
			p, rc, backend := setupProvider(t)
			public := &publicBackend{testBackend: backend}
			rc.Backend = public
			if initialPublic {
				rc.Cluster.Spec.EngineOptions = json.RawMessage(`{"gateway":{"publicHttps":true}}`)
			}
			convergeProvider(t, p, rc)
			rc.Cluster.Generation = 2
			rc.Cluster.Status.ObservedGeneration = 1
			rc.Cluster.Spec.Maintenance = &domain.MaintenancePolicy{Windows: []domain.MaintenanceWindow{{Schedule: fmt.Sprintf("0 0 1 %d *", int(time.Now().Month())%12+1), Duration: "1h"}}}
			if initialPublic {
				rc.Cluster.Spec.EngineOptions = json.RawMessage(`{}`)
			} else {
				rc.Cluster.Spec.EngineOptions = json.RawMessage(`{"gateway":{"publicHttps":true}}`)
			}
			for range 4 {
				if _, err := p.Sync(t.Context(), rc); err != nil {
					t.Fatal(err)
				}
			}
			s, err := loadState(rc.Cluster)
			if err != nil {
				t.Fatal(err)
			}
			if public.enabled != initialPublic || s.Members["gateway"].Public != initialPublic || s.Pending != nil {
				t.Fatal("closed window changed ingress/runtime")
			}
			rc.Cluster.MaintenanceOverrideGeneration = 2
			convergeProvider(t, p, rc)
			if public.enabled == initialPublic {
				t.Fatal("override did not apply routing")
			}
		})
	}
}
