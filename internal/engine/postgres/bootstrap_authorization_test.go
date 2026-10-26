// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package postgres

import (
	"encoding/json"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/engine/postgres/pgspec"
)

func TestFormedClusterRevokesEmptyBootstrapAuthorization(t *testing.T) {
	for _, topology := range []string{"single", "ha"} {
		t.Run(topology, func(t *testing.T) {
			p, rc, _ := setup(t, topology)
			s := converge(t, p, rc)
			for _, m := range s.data() {
				config, err := p.MemberConfig(t.Context(), rc, m.Name)
				if err != nil {
					t.Fatal(err)
				}
				var spec pgspec.Spec
				if err := json.Unmarshal(config.EngineSpec, &spec); err != nil {
					t.Fatal(err)
				}
				if spec.Bootstrap == nil || spec.Bootstrap.AllowInitDB {
					t.Fatal("formed cluster retained permission to initialize empty data")
				}
				if config.Volume == nil || !config.Volume.Initialized {
					t.Fatal("formed data member lost durable volume identity")
				}
			}
		})
	}
}

func TestAllHAMembersMissingDataRequireBackupRestore(t *testing.T) {
	for _, scenario := range []struct {
		name        string
		missing     int
		unreachable bool
		restore     bool
		want        bool
	}{
		{"all-data-lost", 3, false, false, true},
		{"replica-can-reclone", 1, false, false, false},
		{"unknown-is-not-missing", 2, true, false, false},
		{"authorized-restore-wipe", 3, false, true, false},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			p, rc, b := setup(t, "ha")
			s := converge(t, p, rc)
			b.statusOverrides = map[string]pgspec.StatusResponse{}
			for i, m := range s.data() {
				b.statusOverrides[m.Name] = pgspec.StatusResponse{Role: "starting", Running: false, DataMissing: i < scenario.missing, EtcdHealthy: true}
			}
			if scenario.unreachable {
				rc.Agents = unavailableResizeAgent{AgentDialer: rc.Agents, member: s.data()[2].Name}
			}
			if scenario.restore {
				bs, _ := loadBackupState(rc.Cluster)
				bs.ManagedRolesHeld = true
				if err := saveBackupState(rc.Cluster, bs); err != nil {
					t.Fatal(err)
				}
			}
			status, err := p.Status(t.Context(), rc)
			if err != nil {
				t.Fatal(err)
			}
			found := false
			for _, condition := range status.Conditions {
				if condition.Type == "DataRecoveryRequired" && condition.Status == "True" {
					found = true
				}
			}
			if found != scenario.want {
				t.Fatalf("all-data-loss diagnosis=%v want=%v: %+v", found, scenario.want, status.Conditions)
			}
		})
	}
}
