// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package postgres

import (
	"maps"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/engine/postgres/pgspec"
)

func TestPrimaryFirstRestartRequiresPureObservedDecrease(t *testing.T) {
	base := testSpec()
	base.Topology = pgspec.TopologyHA
	base.Postgres.Parameters = map[string]string{"max_connections": "100"}
	observed := pgspec.StatusResponse{
		Role: "primary", Running: true, Leader: base.Member, EtcdHealthy: true,
		SharedMemoryParameters: map[string]int64{
			"max_connections": 200, "max_prepared_transactions": 0,
			"max_locks_per_transaction": 64, "max_wal_senders": 10, "max_worker_processes": 8,
		},
	}
	if !primaryDecreaseAllowed(base, observed) {
		t.Fatal("verified shared-memory decrease cannot restart its primary")
	}
	for _, name := range []string{"replica", "stopped", "unknown-leader", "no-quorum", "missing-values", "increase", "mixed", "unchanged", "invalid"} {
		t.Run(name, func(t *testing.T) {
			s := base
			pg := *base.Postgres
			pg.Parameters = maps.Clone(base.Postgres.Parameters)
			s.Postgres = &pg
			st := observed
			st.SharedMemoryParameters = maps.Clone(observed.SharedMemoryParameters)
			switch name {
			case "replica":
				st.Role = "replica"
			case "stopped":
				st.Running = false
			case "unknown-leader":
				st.Leader = ""
			case "no-quorum":
				st.EtcdHealthy = false
			case "missing-values":
				delete(st.SharedMemoryParameters, "max_wal_senders")
			case "increase":
				s.Postgres.Parameters["max_connections"] = "300"
			case "mixed":
				s.Postgres.Parameters["max_worker_processes"] = "16"
			case "unchanged":
				s.Postgres.Parameters["max_connections"] = "200"
			case "invalid":
				s.Postgres.Parameters["max_connections"] = "invalid"
			}
			if primaryDecreaseAllowed(s, st) {
				t.Fatal("unsafe primary-first restart accepted", name)
			}
		})
	}
}
