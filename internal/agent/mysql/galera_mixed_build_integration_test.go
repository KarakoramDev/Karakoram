//go:build integration

// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/engine/mysql/myspec"
)

// Keep CP security/volume authorization and the production Runner/supervisor;
// only extracted artifact locations, listener ports and the rolled build change.
func TestNativeGaleraMixedBuild(t *testing.T) {
	previousBin, currentBin := os.Getenv("KARAKORAM_PXC_PREVIOUS_BIN_DIR"), os.Getenv("KARAKORAM_MYSQL_BIN_DIR")
	if previousBin == "" || currentBin == "" || os.Getenv("KK_PXC_NATIVE_CONFIG_DIR") == "" {
		t.Skip("set KARAKORAM_PXC_PREVIOUS_BIN_DIR, KARAKORAM_MYSQL_BIN_DIR and KK_PXC_NATIVE_CONFIG_DIR")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	defer cancel()
	buildVersion := func(bin string) string {
		cmd := exec.CommandContext(ctx, filepath.Join(bin, "mysqld"), "--version")
		cmd.Env = nativeEnv()
		raw, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("mysqld --version: %v: %s", err, raw)
		}
		match := regexp.MustCompile(`\bVer\s+([0-9]+\.[0-9]+\.[0-9]+-[0-9]+\.[0-9]+)\b`).FindSubmatch(raw)
		if match == nil {
			t.Fatalf("cannot identify PXC build from %s", raw)
		}
		return string(match[1])
	}
	previous, current := buildVersion(previousBin), buildVersion(currentBin)
	if previous == current {
		t.Fatalf("mixed-build proof requires distinct binaries, both report %s", current)
	}
	xtrabackup := func(variable, bin string) string {
		if value := os.Getenv(variable); value != "" {
			return value
		}
		return filepath.Join(bin, "pxc_extra", "pxb-8.4")
	}
	previousPXB := xtrabackup("KARAKORAM_PXC_PREVIOUS_XTRABACKUP_DIR", previousBin)
	currentPXB := xtrabackup("KARAKORAM_MYSQL_XTRABACKUP_DIR", currentBin)
	fixtures := nativeGaleraConfigs(t, "members.json")
	formed := nativeGaleraConfigs(t, "formed-members.json")
	dir := nativeTestDir(t)
	nodes := make([]*groupRunnerAgent, 3)
	roots := []string{dir, dir, dir}
	versions := []string{previous, previous, previous}
	peers, seeds := make([]myspec.Peer, 3), make([]myspec.Peer, 3)
	var istPorts, sstPorts [3]int
	for i := range nodes {
		member := fixtures[i].spec.Member
		peers[i] = myspec.Peer{Name: member, Host: "127.0.0.1", Port: nativeTestPort(t)}
		seeds[i] = myspec.Peer{Name: member, Host: "127.0.0.1", Port: nativeTestPort(t)}
		istPorts[i], sstPorts[i] = nativeTestPort(t), nativeTestPort(t)
		t.Setenv("KARAKORAM_MYSQL_BIN_DIR_"+member, previousBin)
		t.Setenv("KARAKORAM_MYSQL_XTRABACKUP_DIR_"+member, previousPXB)
	}
	for i := range nodes {
		s := fixtures[i].spec
		s.Version = previous
		s.Peers, s.Galera.Peers = peers, seeds
		s.Galera.GaleraPort, s.Galera.ISTPort, s.Galera.SSTPort = seeds[i].Port, istPorts[i], sstPorts[i]
		nodes[i] = startNativeGaleraAgent(t, ctx, fixtures[i], s, dir)
	}
	// Observe membership independently of SQL to avoid writing through a view
	// change after a stop (wsrep can temporarily reject application requests).
	awaitMembers := func(size int, active []int) {
		nativeGroupPoll(t, 90*time.Second, fmt.Sprintf("%d Primary/Synced mixed-build members", size), func() bool {
			for _, i := range active {
				var status myspec.GaleraStatusResponse
				if nodes[i].client.Engine(ctx, "mysql", myspec.ActionGaleraStatus, struct{}{}, &status) != nil || !status.Eligible() || status.ClusterSize != size {
					return false
				}
			}
			return true
		})
	}
	all := []int{0, 1, 2}
	awaitMembers(3, all)
	// Retire initial formation/bootstrap intent exactly as the existing runner.
	for _, a := range nodes {
		matched := false
		for _, fixture := range formed {
			if fixture.spec.Member == a.spec.Member {
				a.spec = nativeGaleraLocalConfig(fixture.spec, a.spec)
				a.spec.Version = previous
				a.publish(ctx, []string{"mysqld"})
				matched = true
				break
			}
		}
		if !matched {
			t.Fatalf("missing formed config for %s", a.spec.Member)
		}
	}
	execSQL := func(i int, query string, args ...any) {
		db, err := nodes[i].runner.LocalAccessDB(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if err = nativeGaleraExec(ctx, db, query, args...); err != nil {
			t.Fatal(err)
		}
	}
	execSQL(0, "CREATE DATABASE galera_mixed")
	execSQL(0, "CREATE TABLE galera_mixed.writes(id INT PRIMARY KEY,payload VARCHAR(64) NOT NULL) ENGINE=InnoDB")
	execSQL(0, "INSERT INTO galera_mixed.writes VALUES(1,'before-repin'),(2,'known-row'),(3,'full-sst-row')")
	assertPhase := func(phase string, want int) {
		awaitMembers(3, all)
		var last string
		nativeGroupPoll(t, 90*time.Second, phase+" equal rows/checksums, builds and providers", func() bool {
			var checksum int64
			var provider string
			for i, a := range nodes {
				db, err := a.runner.LocalAccessDB(ctx)
				if err != nil {
					last = err.Error()
					return false
				}
				var version, name, table, providerVersion string
				var count int
				var sum sql.NullInt64
				err = db.QueryRowContext(ctx, "SELECT @@version, (SELECT COUNT(*) FROM galera_mixed.writes)").Scan(&version, &count)
				if err == nil {
					err = db.QueryRowContext(ctx, "SHOW GLOBAL STATUS LIKE 'wsrep_provider_version'").Scan(&name, &providerVersion)
				}
				if err == nil {
					err = db.QueryRowContext(ctx, "CHECKSUM TABLE galera_mixed.writes").Scan(&table, &sum)
				}
				last = fmt.Sprintf("%s version=%s want=%s rows=%d want=%d checksum=%v provider=%s err=%v", a.spec.Member, version, versions[i], count, want, sum, providerVersion, err)
				if err != nil || version != versions[i] || count != want || !sum.Valid || providerVersion == "" {
					return false
				}
				if i == 0 {
					checksum, provider = sum.Int64, providerVersion
				} else if checksum != sum.Int64 || provider != providerVersion {
					return false
				}
			}
			return true
		})
		t.Logf("%s: %s", phase, last)
		for i, a := range nodes {
			nativeGaleraTLSProof(t, ctx, []*groupRunnerAgent{a}, roots[i], *a.spec.Galera.TrafficCert)
		}
	}
	assertPhase("three previous-build members", 3)

	// A separate owned empty volume preserves the old data for cleanup but
	// makes it impossible for the replacement to satisfy this join via IST.
	nodes[2].publish(ctx, []string{})
	nodes[2].close()
	awaitMembers(2, []int{0, 1})
	replacementSpec := nativeGaleraLocalConfig(fixtures[2].spec, nodes[2].spec)
	replacementSpec.Version = current
	t.Setenv("KARAKORAM_MYSQL_BIN_DIR_"+replacementSpec.Member, currentBin)
	t.Setenv("KARAKORAM_MYSQL_XTRABACKUP_DIR_"+replacementSpec.Member, currentPXB)
	roots[2] = nativeTestDir(t)
	nodes[2] = startNativeGaleraAgent(t, ctx, fixtures[2], replacementSpec, roots[2])
	versions[2] = current
	assertPhase("current-build replacement SST from previous-build donor", 3)
	errorLog := func(i int) string {
		return filepath.Join(dataPaths(filepath.Join(roots[i], nodes[i].spec.Member)).root, "mysql-error.log")
	}
	raw, err := os.ReadFile(errorLog(2))
	if err != nil || !strings.Contains(string(raw), "Proceeding with SST") || !strings.Contains(string(raw), "Moving the backup to") {
		t.Fatalf("fresh replacement lacks physical xtrabackup SST evidence: %v", err)
	}
	roll := func(i int) {
		a := nodes[i]
		t.Setenv("KARAKORAM_MYSQL_BIN_DIR_"+a.spec.Member, currentBin)
		t.Setenv("KARAKORAM_MYSQL_XTRABACKUP_DIR_"+a.spec.Member, currentPXB)
		a.spec.Version = current
		a.publish(ctx, []string{"mysqld"})
		versions[i] = current
	}
	nodes[1].publish(ctx, []string{})
	awaitMembers(2, []int{0, 2})
	before, err := os.Stat(errorLog(1))
	if err != nil {
		t.Fatal(err)
	}
	execSQL(0, "INSERT INTO galera_mixed.writes VALUES(4,'missing-during-roll'),(5,'ist-required')")
	roll(1)
	assertPhase("in-place current-build member catches up", 5)
	raw, err = os.ReadFile(errorLog(1))
	if err != nil || int64(len(raw)) < before.Size() || !containsCompletedIST(raw[before.Size():]) {
		t.Fatalf("in-place build roll lacks completed nonempty writeset IST or used SST: %v", err)
	}
	nodes[0].publish(ctx, []string{})
	awaitMembers(2, []int{1, 2})
	roll(0)
	assertPhase("all members rolled to current build", 5)
	for i := range nodes {
		execSQL(i, "INSERT INTO galera_mixed.writes VALUES(?,?)", 6+i, fmt.Sprintf("current-build-writer-%d", i+1))
		assertPhase(fmt.Sprintf("write from rolled member %d replicated", i+1), 6+i)
	}
}
