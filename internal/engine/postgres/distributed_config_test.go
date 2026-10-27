// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package postgres

import (
	"encoding/json"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"slices"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/agent"
	agentpostgres "github.com/KarakoramDev/Karakoram/internal/agent/postgres"
	"github.com/KarakoramDev/Karakoram/internal/engine/postgres/pgspec"
)

func TestDistributedMemberConfig(t *testing.T) {
	for _, replicas := range []int{1, 3} {
		for _, ipv6 := range []bool{false, true} {
			t.Run(fmt.Sprintf("replicas%d/ipv6%t", replicas, ipv6), func(t *testing.T) {
				testDistributedMemberConfig(t, replicas, ipv6)
			})
		}
	}
}

func testDistributedMemberConfig(t *testing.T, replicas int, ipv6 bool) {
	t.Helper()
	p, rc, b, _ := setupDistributed(t, replicas)
	if ipv6 {
		rc.Backend = ipv6Backend{b}
	}
	rc.Cluster.Spec.EngineOptions = json.RawMessage(`{"citus":{"workerGroups":1,"database":"tenant"}}`)
	s := converge(t, p, rc)
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
		// Readiness decodes and validates the real CP wire payload before
		// honoring maintenance. Avoid native process probes, not validation.
		config.Maintenance = true
		dir := t.TempDir()
		if err := os.MkdirAll(filepath.Join(dir, "karakoram"), 0700); err != nil {
			t.Fatal(err)
		}
		wire, err := json.Marshal(config)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, "karakoram", "config.json"), wire, 0600); err != nil {
			t.Fatal(err)
		}
		if err := agentpostgres.New().Readiness(t.Context(), agent.Options{DataDir: dir}, nil); err != nil {
			t.Fatalf("agent rejected group %d CP payload: %v", m.Group, err)
		}
		if spec.Citus == nil || spec.Citus.Group != m.Group || spec.Citus.Database != "tenant" {
			t.Fatalf("Citus identity: %+v", spec.Citus)
		}
		if scope != "" && spec.Scope != scope {
			t.Fatal("scope is not formation-wide")
		}
		scope = spec.Scope
		endpoints := 0
		if m.Group != 0 {
			endpoints = replicas
		}
		if len(spec.Peers) != replicas || len(spec.Etcd.Endpoints) != endpoints {
			t.Fatalf("wrong peer/endpoint count for group %d", m.Group)
		}
		for i, endpoint := range spec.Etcd.Endpoints {
			if want := net.JoinHostPort(s.group(0).data()[i].Name, "2379"); endpoint != want {
				t.Fatalf("worker endpoint = %q, want %q", endpoint, want)
			}
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
		if m.Group == 0 && (!slices.Contains(config.DesiredProcesses, "etcd") || spec.Etcd.InitialCluster == "" || len(spec.Etcd.Endpoints) != 0) {
			t.Fatal("coordinator must retain local etcd voter mode")
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
