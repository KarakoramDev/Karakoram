// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package mysql

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/engine/mysql/myspec"
)

func TestGaleraOwnReceiptFencesForeignData(t *testing.T) {
	s := testGaleraSpec()
	p := dataPaths(t.TempDir())
	for _, d := range []string{p.root, p.data, filepath.Join(p.data, "mysql")} {
		if err := privateDir(d); err != nil {
			t.Fatal(err)
		}
	}
	uuid := "826914cc-cd20-11f1-b4c4-6ac2787ad4af"
	id := myspec.DataIdentity{Initialized: true, ServerUUID: "11111111-1111-1111-1111-111111111111"}
	if err := durableJSON(filepath.Join(p.root, "initialized.json"), id); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(p.data, "grastate.dat"), []byte("uuid: "+uuid+"\nseqno: -1\nsafe_to_bootstrap: 0\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := guardInitialization(s, p); err == nil {
		t.Fatal("foreign data accepted without own receipt")
	}
	r := galeraIdentity{ClusterName: s.Galera.ClusterName, Member: s.Member, FormationEpoch: 1, StateUUID: uuid, Pending: true}
	save := func() {
		t.Helper()
		if err := durableJSON(filepath.Join(p.root, "sst-authorized.json"), r); err != nil {
			t.Fatal(err)
		}
	}
	save()
	if _, err := guardInitialization(s, p); err != nil {
		t.Fatalf("authorized SST missing auto.cnf rejected: %v", err)
	}
	if err := os.WriteFile(filepath.Join(p.data, "auto.cnf"), []byte("[auto]\nserver-uuid=22222222-2222-2222-2222-222222222222\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := guardInitialization(s, p); err != nil {
		t.Fatalf("authorized SST UUID replacement rejected: %v", err)
	}
	r.Pending = false
	save()
	if _, err := guardInitialization(s, p); err == nil {
		t.Fatal("unapproved UUID replacement accepted")
	}
	r.Pending = true
	r.StateUUID = "33333333-3333-3333-3333-333333333333"
	save()
	if _, err := guardInitialization(s, p); err == nil {
		t.Fatal("foreign cluster UUID accepted with pending SST")
	}
	r.StateUUID = uuid
	r.Member = "foreign"
	save()
	if _, err := guardInitialization(s, p); err == nil {
		t.Fatal("foreign own-member receipt accepted")
	}
}

func TestGaleraEpochAdmissionRejectsDelayedConfigurations(t *testing.T) {
	s := testGaleraSpec()
	p := dataPaths(t.TempDir())
	if err := privateDir(p.root); err != nil {
		t.Fatal(err)
	}
	s.Galera.FormationEpoch = 7
	if err := admitGaleraEpoch(s, p); err != nil {
		t.Fatal(err)
	}
	s.Galera.FormationEpoch = 6
	if err := admitGaleraEpoch(s, p); err == nil {
		t.Fatal("stale nonbootstrap epoch accepted")
	}
}

func TestGaleraPrepareJoinResetsOwnDataOnce(t *testing.T) {
	s := testGaleraSpec()
	s.Galera.Bootstrap = false
	p := dataPaths(t.TempDir())
	for _, dir := range []string{p.root, p.data, filepath.Join(p.data, "mysql")} {
		if err := privateDir(dir); err != nil {
			t.Fatal(err)
		}
	}
	uuid := "826914cc-cd20-11f1-b4c4-6ac2787ad4af"
	if err := durableJSON(filepath.Join(p.root, "initialized.json"), myspec.DataIdentity{Initialized: true}); err != nil {
		t.Fatal(err)
	}
	if err := durableJSON(filepath.Join(p.root, "sst-authorized.json"), galeraIdentity{Member: s.Member, ClusterName: s.Galera.ClusterName, FormationEpoch: 1, StateUUID: uuid}); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(p.data, "grastate.dat"), []byte("uuid: "+uuid+"\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := prepareGaleraJoin(s, p); err == nil {
		t.Fatal("same epoch reset admitted")
	}
	s.Galera.FormationEpoch = 2
	s.Galera.ClusterName = "restored-cluster"
	if err := prepareGaleraJoin(s, p); err != nil {
		t.Fatal(err)
	}
	s.AllowInitialize = false
	if fresh, err := guardInitialization(s, p); err != nil || fresh {
		t.Fatalf("prepared empty SST rejected: %v %v", fresh, err)
	}
	marker := filepath.Join(p.data, "joined-data")
	if err := os.WriteFile(marker, []byte("preserve"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := prepareGaleraJoin(s, p); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(marker); err != nil {
		t.Fatal("repeat reset erased new data", err)
	}
}
