//go:build integration

// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package mysql

import (
	"context"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/engine/mysql/myspec"
)

func nativeGaleraAdditionProof(t *testing.T, ctx context.Context, nodes []*groupRunnerAgent, proxy *groupRunnerAgent, dir string) []*groupRunnerAgent {
	t.Helper()
	began := time.Now()
	additions := nativeGaleraConfigs(t, "added-members.json")
	byName := map[string]*groupRunnerAgent{}
	pids := map[string]int{}
	peers := append([]myspec.Peer(nil), nodes[0].spec.Peers...)
	seeds := append([]myspec.Peer(nil), nodes[0].spec.Galera.Peers...)
	for _, a := range nodes {
		byName[a.spec.Member] = a
		status, err := a.client.Status(ctx)
		if err != nil {
			t.Fatal(err)
		}
		pids[a.spec.Member] = status.Processes["mysqld"].PID
	}
	type newcomer struct {
		fixture          nativeGaleraConfig
		galera, ist, sst int
	}
	var fresh []newcomer
	current := nodes[0].spec.Galera
	for _, fixture := range additions {
		if fixture.spec.Role != myspec.RoleData {
			continue
		}
		if fixture.spec.Galera.TrafficCAPEM != current.TrafficCAPEM || fixture.spec.Galera.TrafficCert.KeyPEM != current.TrafficCert.KeyPEM {
			t.Fatal("membership addition changed shared traffic trust or key")
		}
		if old := byName[fixture.spec.Member]; old != nil {
			if fixture.spec.ServerCert.CertPEM != old.spec.ServerCert.CertPEM {
				t.Fatal("membership addition replaced an existing SQL leaf")
			}
			continue
		}
		ports := newcomer{fixture: fixture, galera: nativeTestPort(t), ist: nativeTestPort(t), sst: nativeTestPort(t)}
		fresh = append(fresh, ports)
		peers = append(peers, myspec.Peer{Name: fixture.spec.Member, Host: "127.0.0.1", Port: nativeTestPort(t)})
		seeds = append(seeds, myspec.Peer{Name: fixture.spec.Member, Host: "127.0.0.1", Port: ports.galera})
	}
	if len(fresh) != 2 {
		t.Fatalf("expected two CP-authorized additions, got%d", len(fresh))
	}
	for _, a := range nodes {
		a.spec.Peers = peers
		a.spec.Galera.Peers = seeds
		a.publish(ctx, []string{"mysqld"})
		status, err := a.client.Status(ctx)
		if err != nil || status.Processes["mysqld"].PID != pids[a.spec.Member] {
			t.Fatalf("peer addition interrupted existing member%s: %v", a.spec.Member, err)
		}
	}
	all := append([]*groupRunnerAgent(nil), nodes...)
	for _, entry := range fresh {
		s := entry.fixture.spec
		if !s.AllowInitialize {
			t.Fatal("CP addition export lacks new-member initialization authorization")
		}
		s.Peers = peers
		s.Galera.Peers = seeds
		s.Galera.GaleraPort, s.Galera.ISTPort, s.Galera.SSTPort = entry.galera, entry.ist, entry.sst
		// The live proof restored into a new history; addresses and recovery history
		// are the only differences from the CP-issued membership checkpoint.
		s.Galera.ClusterName = current.ClusterName
		s.Galera.FormationEpoch = current.FormationEpoch
		s.Galera.Bootstrap = false
		s.Galera.StartPosition = ""
		all = append(all, startNativeGaleraAgent(t, ctx, entry.fixture, s, dir))
	}
	nativeGroupPoll(t, 90*time.Second, "five CP-authorized Primary/Synced members", func() bool {
		uuid := ""
		for _, a := range all {
			var status myspec.GaleraStatusResponse
			if a.client.Engine(ctx, "mysql", myspec.ActionGaleraStatus, struct{}{}, &status) != nil || !status.Eligible() || status.ClusterSize != 5 {
				return false
			}
			if uuid == "" {
				uuid = status.StateUUID
			}
			if uuid != status.StateUUID {
				return false
			}
		}
		return true
	})
	db, err := all[4].runner.LocalAccessDB(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = db.ExecContext(ctx, "INSERT INTO galera_runner.writes VALUES(3000,@@port)"); err != nil {
		t.Fatal(err)
	}
	nativeGroupPoll(t, 30*time.Second, "new-member write across all five nodes", func() bool {
		for _, a := range all {
			db, err := a.runner.LocalAccessDB(ctx)
			if err != nil {
				return false
			}
			var count int
			if db.QueryRowContext(ctx, "SELECT COUNT(*) FROM galera_runner.writes WHERE id=3000").Scan(&count) != nil || count != 1 {
				return false
			}
		}
		return true
	})
	nativeGaleraTLSProof(t, ctx, all, dir, *current.TrafficCert)
	nativeGaleraRejectUnauthorizedPeers(t, ctx, all[4])
	proxy.spec.Peers = peers
	proxy.spec.Proxy.Backends = peers
	proxy.publish(ctx, []string{"proxysql"})
	admin, err := proxyAdmin(ctx, proxy.spec)
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	nativeGroupPoll(t, 30*time.Second, "five-node proxy retains exactly one online writer", func() bool {
		var count int
		return admin.QueryRowContext(ctx, "SELECT COUNT(*) FROM runtime_mysql_servers WHERE hostgroup_id=10 AND status='ONLINE'").Scan(&count) == nil && count == 1
	})
	nativeGaleraGTIDQuorumProof(t, ctx, all, "membership addition")
	t.Logf("CP-shaped3->5 membership addition without existing-member restart: %s", time.Since(began))
	return all
}
