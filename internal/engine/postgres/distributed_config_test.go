// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package postgres

import (
	"encoding/json"
	"slices"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/engine/postgres/pgspec"
)

func TestDistributedMemberConfig(t *testing.T) {
	p, rc, _ := setup(t, "ha")
	s := converge(t, p, rc)
	s.Topology, rc.Cluster.Spec.Topology.Type = "distributed", "distributed"
	rc.Cluster.Spec.EngineOptions = json.RawMessage(`{"citus":{"workerGroups":1,"database":"tenant"}}`)
	// An unformed formation reserves every member of every group together.
	s.Formed = false
	s.allocate(rc.Cluster)
	for _, m := range s.Members {
		m.Topology = "distributed"
		m.Options = rc.Cluster.Spec.EngineOptions
	}
	if err := p.ensureSecrets(t.Context(), rc, s); err != nil {
		t.Fatal(err)
	}
	if err := save(rc.Cluster, s); err != nil {
		t.Fatal(err)
	}
	var scope string
	for _, m := range s.data() {
		config, err := p.MemberConfig(t.Context(), rc, m.Name)
		if err != nil {
			t.Fatal(err)
		}
		var spec pgspec.Spec
		if err := json.Unmarshal(config.EngineSpec, &spec); err != nil {
			t.Fatal(err)
		}
		if spec.Citus == nil || spec.Citus.Group != m.Group || spec.Citus.Database != "tenant" {
			t.Fatalf("Citus identity: %+v", spec.Citus)
		}
		if scope != "" && spec.Scope != scope {
			t.Fatal("scope is not formation-wide")
		}
		scope = spec.Scope
		if len(spec.Peers) != 3 || len(spec.Etcd.Endpoints) != 3 {
			t.Fatalf("peers/endpoints: %+v %+v", spec.Peers, spec.Etcd)
		}
		for _, peer := range spec.Peers {
			if s.find(peer.Name).Group != m.Group {
				t.Fatal("cross-group replication peer", peer)
			}
		}
		if spec.Postgres.Parameters["wal_level"] != "logical" || len(spec.Postgres.SharedPreloadLibraries) == 0 || spec.Postgres.SharedPreloadLibraries[0] != "citus" {
			t.Fatal("missing forced Citus parameters")
		}
		if m.Group > 0 && (slices.Contains(config.DesiredProcesses, "etcd") || spec.Etcd.InitialCluster != "") {
			t.Fatal("worker started an etcd voter")
		}
		if m.Group == 0 && !slices.Contains(config.DesiredProcesses, "etcd") {
			t.Fatal("coordinator lacks etcd")
		}
	}
	config, err := p.MemberConfig(t.Context(), rc, "demopx")
	if err != nil {
		t.Fatal(err)
	}
	var spec pgspec.Spec
	if err := json.Unmarshal(config.EngineSpec, &spec); err != nil {
		t.Fatal(err)
	}
	for _, peer := range spec.Peers {
		if s.find(peer.Name).Group != 0 {
			t.Fatal("proxy published a worker")
		}
	}
}
