// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package mysql

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

func TestGaleraPreparePristineRestoreFollowers(t *testing.T) {
	for _, member := range []string{"restoredm1", "restoredm2"} {
		t.Run(member, func(t *testing.T) {
			s := testGaleraSpec()
			s.Member = member
			s.AllowInitialize = false // Restore/reformation is not ordinary initialization.
			s.Galera.Bootstrap = false
			s.Galera.FormationEpoch++
			p := dataPaths(t.TempDir())
			for _, dir := range []string{p.root, p.data} {
				if err := privateDir(dir); err != nil {
					t.Fatal(err)
				}
			}
			if err := prepareGaleraJoin(s, p); err != nil {
				t.Fatal(err)
			}
			receipt, err := readGaleraIdentity(s, p)
			if err != nil || !receipt.Pending || receipt.Member != member || receipt.FormationEpoch != s.Galera.FormationEpoch {
				t.Fatalf("receipt=%+v err=%v", receipt, err)
			}
			// Once complete, replay must not erase data supplied by a subsequent SST.
			marker := filepath.Join(p.data, "transferred")
			if err := os.WriteFile(marker, []byte("keep"), 0600); err != nil {
				t.Fatal(err)
			}
			if err := prepareGaleraJoin(s, p); err != nil {
				t.Fatal(err)
			}
			if _, err := os.Stat(marker); err != nil {
				t.Fatal("replay erased transferred data", err)
			}
		})
	}
}

func TestGaleraPreparePristineRefusesAmbiguousData(t *testing.T) {
	for _, name := range []string{"nonempty", "identity", "start", "launch", "bootstrap", "previous join", "malformed receipt", "symlink"} {
		t.Run(name, func(t *testing.T) {
			s := testGaleraSpec()
			s.Galera.Bootstrap = false
			s.Galera.FormationEpoch++
			p := dataPaths(t.TempDir())
			for _, dir := range []string{p.root, p.data} {
				if err := privateDir(dir); err != nil {
					t.Fatal(err)
				}
			}
			file := ""
			switch name {
			case "nonempty":
				file = filepath.Join(p.data, "foreign")
			case "identity":
				file = filepath.Join(p.root, "initialized.json")
			case "start":
				file = filepath.Join(p.root, "galera-sst-start")
			case "launch":
				file = filepath.Join(p.root, galeraUnsyncedMarker)
			case "bootstrap":
				file = filepath.Join(p.root, "galera-bootstrap-1")
			case "previous join":
				file = filepath.Join(p.root, "galera-join-1.json")
			case "malformed receipt":
				file = filepath.Join(p.root, "sst-authorized.json")
			case "symlink":
				if err := os.Remove(p.data); err != nil {
					t.Fatal(err)
				}
				if err := os.Symlink(t.TempDir(), p.data); err != nil {
					t.Fatal(err)
				}
			}
			if file != "" {
				if err := os.WriteFile(file, []byte("foreign"), 0600); err != nil {
					t.Fatal(err)
				}
			}
			if err := prepareGaleraJoin(s, p); err == nil {
				t.Fatal("ambiguous data admitted")
			}
			if file != "" {
				raw, err := os.ReadFile(file)
				if err != nil || string(raw) != "foreign" {
					t.Fatalf("foreign evidence changed: %q %v", raw, err)
				}
			}
		})
	}
}

func TestGaleraPreparePristineInterruptedAdmission(t *testing.T) {
	for _, authorized := range []bool{false, true} {
		t.Run(fmt.Sprint(authorized), func(t *testing.T) {
			s := testGaleraSpec()
			s.Galera.Bootstrap = false
			s.Galera.FormationEpoch++
			p := dataPaths(t.TempDir())
			for _, dir := range []string{p.root, p.data} {
				if err := privateDir(dir); err != nil {
					t.Fatal(err)
				}
			}
			intent := struct {
				ClusterName, Member string
				Done, Pristine      bool
			}{ClusterName: s.Galera.ClusterName, Member: s.Member, Pristine: true}
			if err := durableJSON(filepath.Join(p.root, fmt.Sprintf("galera-join-%d.json", s.Galera.FormationEpoch)), intent); err != nil {
				t.Fatal(err)
			}
			if authorized {
				receipt := galeraIdentity{ClusterName: s.Galera.ClusterName, Member: s.Member, FormationEpoch: s.Galera.FormationEpoch, Pending: true}
				if err := durableJSON(filepath.Join(p.root, "sst-authorized.json"), receipt); err != nil {
					t.Fatal(err)
				}
			}
			if err := prepareGaleraJoin(s, p); err != nil {
				t.Fatal(err)
			}
			if err := prepareGaleraJoin(s, p); err != nil {
				t.Fatal(err)
			}
		})
	}
}
