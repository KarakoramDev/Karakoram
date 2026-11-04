// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package postgres

import (
	"github.com/KarakoramDev/Karakoram/internal/engine/postgres/pgspec"
	"testing"
)

func TestSingletonCitusRestart(t *testing.T) {
	s := testSpec()
	s.Topology = pgspec.TopologyDistributed
	s.Citus = &pgspec.Citus{Group: 0, Database: "postgres"}
	s.Peers = []pgspec.Peer{{Name: s.Member, Host: "localhost"}}
	st := pgspec.StatusResponse{Role: "primary", Running: true, Leader: s.Member, EtcdHealthy: true}
	if !singletonCitusRestartAllowed(s, st) {
		t.Fatal("singleton Citus primary cannot restart")
	}
	s.Peers = append(s.Peers, pgspec.Peer{Name: "replica", Host: "replica.example"})
	if singletonCitusRestartAllowed(s, st) {
		t.Fatal("multi-member group may bypass switchover")
	}
	s.Peers = s.Peers[:1]
	for _, topology := range []string{pgspec.TopologySingle, pgspec.TopologyHA} {
		legacy := s
		legacy.Topology = topology
		legacy.Citus = nil
		if singletonCitusRestartAllowed(legacy, st) {
			t.Fatalf("changed %s restart behavior", topology)
		}
	}
	for _, bad := range []pgspec.StatusResponse{{Role: "replica", Running: true, Leader: s.Member, EtcdHealthy: true}, {Role: "primary", Leader: s.Member, EtcdHealthy: true}, {Role: "primary", Running: true, Leader: "other", EtcdHealthy: true}, {Role: "primary", Running: true, Leader: s.Member}} {
		if singletonCitusRestartAllowed(s, bad) {
			t.Fatalf("accepted unsafe restart %+v", bad)
		}
	}
}

func TestDistributedProxyNeedsNoLocalCitusGroup(t *testing.T) {
	s := testSpec()
	s.Topology = pgspec.TopologyDistributed
	s.Role = pgspec.RoleProxy
	s.Proxy = &pgspec.Proxy{Admin: s.Superuser, Monitor: s.Monitor, Servers: []string{"coordinator.example"}}
	if err := validate(s); err != nil {
		t.Fatalf("distributed coordinator proxy rejected: %v", err)
	}
}
