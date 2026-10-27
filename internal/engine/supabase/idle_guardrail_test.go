// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package supabase

import (
	"fmt"
	"github.com/KarakoramDev/Karakoram/internal/engine/enginetest"
	"github.com/KarakoramDev/Karakoram/internal/engine/postgres/pgspec"
	"testing"
)

func TestIdleReconcileZeroProviderMutations(t *testing.T) {
	for _, topology := range []string{"single", "ha"} {
		t.Run(topology, func(t *testing.T) {
			p, rc, b := setupProvider(t)
			rc.Cluster.Spec.Topology.Type = topology
			p.ApplyDefaults(&rc.Cluster.Spec)
			if topology == "ha" {
				rc.Cluster.Spec.Components.Engine.Replicas = 3
				b.nativeStatus = map[string]pgspec.StatusResponse{}
				for i := range 3 {
					role := "replica"
					if i == 0 {
						role = "primary"
					}
					b.nativeStatus[fmt.Sprintf("demop%d", i)] = pgspec.StatusResponse{Running: true, Role: role, Leader: "demop0", Streaming: i != 0, EtcdHealthy: true, Version: rc.Cluster.Spec.Version, Timeline: 1, SystemID: "123456789", WALPosition: "1/100", SlotStatusKnown: true, WALStatusKnown: true, ArchiveStatusKnown: true}
				}
			}
			enginetest.AssertIdle(t, p, rc, nil)
		})
	}
}
