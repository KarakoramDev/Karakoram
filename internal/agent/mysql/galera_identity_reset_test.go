// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package mysql

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"os"
	"path/filepath"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/engine/mysql/myspec"
)

const resetTestStateUUID = "826914cc-cd20-11f1-b4c4-6ac2787ad4af"
const resetTestServerUUID = "11111111-1111-1111-1111-111111111111"
const resetTestState = "GALERA saved state\nversion: 2.1\nuuid: 00000000-0000-0000-0000-000000000000\nseqno: -1\nsafe_to_bootstrap: 0\n"

func galeraResetFixture(t *testing.T) (myspec.Spec, paths, galeraIdentity) {
	t.Helper()
	s := testGaleraSpec()
	s.Galera.Bootstrap = false
	s.AllowInitialize = false
	p := dataPaths(t.TempDir())
	for _, dir := range []string{p.root, p.data, filepath.Join(p.data, "mysql")} {
		if err := privateDir(dir); err != nil {
			t.Fatal(err)
		}
	}
	id := myspec.DataIdentity{Initialized: true, ServerUUID: resetTestServerUUID, FormationEpoch: s.Galera.FormationEpoch}
	if err := durableJSON(filepath.Join(p.root, "initialized.json"), id); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(p.data, "auto.cnf"), []byte("[auto]\nserver-uuid="+resetTestServerUUID+"\n"), 0600); err != nil {
		t.Fatal(err)
	}
	receipt := galeraIdentity{ClusterName: s.Galera.ClusterName, Member: s.Member, FormationEpoch: s.Galera.FormationEpoch, StateUUID: resetTestStateUUID}
	if err := durableJSON(filepath.Join(p.root, "sst-authorized.json"), receipt); err != nil {
		t.Fatal(err)
	}
	// The last launch never reached Synced: an interrupted join.
	if err := os.WriteFile(filepath.Join(p.root, galeraUnsyncedMarker), nil, 0600); err != nil {
		t.Fatal(err)
	}
	return s, p, receipt
}
func TestGaleraInterruptedTransferResetAdmission(t *testing.T) {
	for _, test := range []struct {
		name, state string
		change      func(*myspec.Spec, *galeraIdentity)
		synced      bool
		allowed     bool
	}{
		{name: "exact upstream reset", state: resetTestState, allowed: true},
		{name: "absent state file"},
		{name: "foreign UUID", state: "uuid: 33333333-3333-3333-3333-333333333333\nseqno: -1\nsafe_to_bootstrap: 0\n"},
		{name: "zero with committed position", state: "uuid: 00000000-0000-0000-0000-000000000000\nseqno: 18\nsafe_to_bootstrap: 0\n"},
		{name: "zero with bootstrap permission", state: "uuid: 00000000-0000-0000-0000-000000000000\nseqno: -1\nsafe_to_bootstrap: 1\n"},
		{name: "reset missing sequence", state: "uuid: 00000000-0000-0000-0000-000000000000\nsafe_to_bootstrap: 0\n"},
		{name: "reset missing bootstrap flag", state: "uuid: 00000000-0000-0000-0000-000000000000\nseqno: -1\n"},
		{name: "duplicate reset sequence", state: resetTestState + "seqno: -1\n"},
		{name: "different member", state: resetTestState, change: func(s *myspec.Spec, _ *galeraIdentity) { s.Member = "foreign" }},
		{name: "different cluster", state: resetTestState, change: func(s *myspec.Spec, _ *galeraIdentity) { s.Galera.ClusterName = "foreign" }},
		// A CP reformation of the same pinned history raises the epoch while a
		// member may still be retrying its interrupted join.
		{name: "same-history reformation epoch", state: resetTestState, change: func(s *myspec.Spec, _ *galeraIdentity) { s.Galera.FormationEpoch++ }, allowed: true},
		{name: "receipt newer than configured epoch", state: resetTestState, change: func(_ *myspec.Spec, r *galeraIdentity) { r.FormationEpoch++ }},
		{name: "invalid durable state UUID", state: resetTestState, change: func(_ *myspec.Spec, r *galeraIdentity) { r.StateUUID = "invalid" }},
		{name: "reset of a Synced member (inconsistency eviction)", state: resetTestState, synced: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			s, p, r := galeraResetFixture(t)
			if test.change != nil {
				test.change(&s, &r)
			}
			if test.synced {
				if err := os.Remove(filepath.Join(p.root, galeraUnsyncedMarker)); err != nil {
					t.Fatal(err)
				}
			}
			if err := durableJSON(filepath.Join(p.root, "sst-authorized.json"), r); err != nil {
				t.Fatal(err)
			}
			if test.state != "" {
				if err := os.WriteFile(filepath.Join(p.data, "grastate.dat"), []byte(test.state), 0600); err != nil {
					t.Fatal(err)
				}
			}
			fresh, err := guardInitialization(s, p)
			if (err == nil) != test.allowed || fresh {
				t.Fatalf("reset admission fresh=%v err=%v allowed=%v", fresh, err, test.allowed)
			}
			if test.allowed {
				admitted, err := guardGaleraIdentity(s, p)
				if err != nil || admitted.Pending || admitted.StateUUID != resetTestStateUUID {
					t.Fatalf("reset must retain own identity without granting identity change: %+v %v", admitted, err)
				}
			}
		})
	}
}
func TestGaleraInterruptedTransferResetSeedBootstrapReplay(t *testing.T) {
	s, p, _ := galeraResetFixture(t)
	if err := os.WriteFile(filepath.Join(p.data, "grastate.dat"), []byte(resetTestState), 0600); err != nil {
		t.Fatal(err)
	}
	// A same-history reformation selects this member as recovered seed.
	s.Galera.FormationEpoch++
	s.Galera.Bootstrap = true
	s.Galera.StartPosition = resetTestStateUUID + ":9"
	if _, err := guardInitialization(s, p); err != nil {
		t.Fatal("reset seed rejected before bootstrap preparation", err)
	}
	if err := setSafeToBootstrap(p); err != nil {
		t.Fatal(err)
	}
	// Startup interrupted after preparation: the replay must pass the guard.
	if _, err := guardInitialization(s, p); err != nil {
		t.Fatal("prepared reset seed blocked on replay", err)
	}
	joiner := s
	joiner.Galera = new(myspec.Galera)
	*joiner.Galera = *s.Galera
	joiner.Galera.Bootstrap, joiner.Galera.StartPosition = false, ""
	if _, err := guardGaleraIdentity(joiner, p); err == nil {
		t.Fatal("bootstrap permission admitted without a bootstrap intent")
	}
	if err := os.Mkdir(galeraClaimPath(s, p), 0700); err != nil {
		t.Fatal(err)
	}
	if _, err := guardGaleraIdentity(s, p); err == nil {
		t.Fatal("bootstrap permission admitted after its claim was consumed")
	}
}
func TestGaleraInterruptedTransferResetGrantsNoServerIdentityChange(t *testing.T) {
	s, p, _ := galeraResetFixture(t)
	if err := os.WriteFile(filepath.Join(p.data, "grastate.dat"), []byte(resetTestState), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(p.data, "auto.cnf"), []byte("[auto]\nserver-uuid=22222222-2222-2222-2222-222222222222\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := guardInitialization(s, p); err == nil {
		t.Fatal("reset alone admitted a changed server identity")
	}
	// The actual supervised launch records its own transfer authorization.
	if err := os.Mkdir(filepath.Join(p.root, "galera-sst-start"), 0700); err != nil {
		t.Fatal(err)
	}
	if _, err := guardInitialization(s, p); err != nil {
		t.Fatal("launch-authorized transfer after reset rejected", err)
	}
	if err := os.Remove(filepath.Join(p.root, "galera-sst-start")); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(filepath.Join(p.data, "grastate.dat")); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(filepath.Join(p.root, "elsewhere"), filepath.Join(p.data, "grastate.dat")); err != nil {
		t.Fatal(err)
	}
	if _, err := guardGaleraIdentity(s, p); err == nil {
		t.Fatal("symlinked Galera state admitted")
	}
}
func TestGaleraInterruptedTransferIdentityRefreshRequiresSyncedOwnHistory(t *testing.T) {
	s, p, _ := galeraResetFixture(t)
	if err := os.WriteFile(filepath.Join(p.data, "grastate.dat"), []byte(resetTestState), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := guardInitialization(s, p); err != nil {
		t.Fatal(err)
	}
	id, err := readIdentity(p)
	if err != nil || id.ServerUUID != resetTestServerUUID {
		t.Fatal("reset prematurely adopted a server UUID", err)
	}
	if err = authorizeGaleraStart(s, p); err != nil {
		t.Fatal(err)
	}
	const renewedUUID = "22222222-2222-2222-2222-222222222222"
	if err = os.WriteFile(filepath.Join(p.data, "auto.cnf"), []byte("[auto]\nserver-uuid="+renewedUUID+"\n"), 0600); err != nil {
		t.Fatal(err)
	}
	connector := &groupTestConnector{t: t, queries: []groupQuery{{contains: "SELECT @@server_uuid", values: []driver.Value{renewedUUID}}}}
	db := sql.OpenDB(connector)
	defer db.Close()
	status := myspec.GaleraStatusResponse{ClusterStatus: "Primary", LocalStateComment: "Joining", Connected: true, Ready: true, StateUUID: resetTestStateUUID}
	if err = refreshGaleraIdentity(context.Background(), db, s, p, status); err == nil {
		t.Fatal("joining member refreshed durable identity")
	}
	status.LocalStateComment = "Synced"
	status.StateUUID = "33333333-3333-3333-3333-333333333333"
	if err = refreshGaleraIdentity(context.Background(), db, s, p, status); err == nil {
		t.Fatal("foreign Synced cluster refreshed durable identity")
	}
	id, err = readIdentity(p)
	if err != nil || id.ServerUUID != resetTestServerUUID {
		t.Fatal("unverified transfer changed durable server UUID", err)
	}
	status.StateUUID = resetTestStateUUID
	if err = refreshGaleraIdentity(context.Background(), db, s, p, status); err != nil {
		t.Fatal(err)
	}
	id, err = readIdentity(p)
	if err != nil || id.ServerUUID != renewedUUID || !id.Initialized {
		t.Fatalf("Synced own history did not refresh SST identity: %+v %v", id, err)
	}
	receipt, err := readGaleraIdentity(s, p)
	if err != nil || receipt.Pending || receipt.StateUUID != resetTestStateUUID {
		t.Fatalf("wrong recovered receipt: %+v %v", receipt, err)
	}
	if _, err = os.Lstat(filepath.Join(p.root, galeraUnsyncedMarker)); !os.IsNotExist(err) {
		t.Fatal("Synced own history kept the unfinished-join marker", err)
	}
	if _, err = guardGaleraIdentity(s, p); err == nil {
		t.Fatal("reset tuple admitted after the member finished joining")
	}
	if err = os.WriteFile(filepath.Join(p.data, "grastate.dat"), []byte("uuid: "+resetTestStateUUID+"\nseqno: -1\nsafe_to_bootstrap: 0\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err = guardInitialization(s, p); err != nil {
		t.Fatal("normal recovered member rejected", err)
	}
}
