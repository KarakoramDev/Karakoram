// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package postgres

import (
	"encoding/json"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/engine/postgres/pgspec"
)

func upgradeRequest() pgspec.UpgradeRequest {
	return pgspec.UpgradeRequest{Receipt: "upgrade-operation", OldMajor: 16, NewMajor: 17, Locale: "C.UTF-8"}
}
func TestUpgradeAdmission(t *testing.T) {
	request := upgradeRequest()
	if err := validateUpgradeRequest(request); err != nil {
		t.Fatal(err)
	}
	for name, change := range map[string]func(*pgspec.UpgradeRequest){"empty receipt": func(r *pgspec.UpgradeRequest) { r.Receipt = "" }, "nonadjacent": func(r *pgspec.UpgradeRequest) { r.NewMajor = 18 }, "downgrade": func(r *pgspec.UpgradeRequest) { r.NewMajor = 15 }, "missing locale": func(r *pgspec.UpgradeRequest) { r.Locale = "" }, "locale injection": func(r *pgspec.UpgradeRequest) { r.Locale = "C\n.UTF-8" }} {
		t.Run(name, func(t *testing.T) {
			r := request
			change(&r)
			if validateUpgradeRequest(r) == nil {
				t.Fatal("invalid upgrade request accepted")
			}
		})
	}
	request.Replica = true
	request.Locale = ""
	if err := validateUpgradeRequest(request); err != nil {
		t.Fatal("replica rebuilding must not require a locale")
	}
}
func TestUpgradeCommandArguments(t *testing.T) {
	request := upgradeRequest()
	request.Receipt = "../../not-a-path"
	p := dataPaths("/volume with spaces")
	u := pathsForUpgrade(p, request)
	s := testSpec()
	if filepath.Dir(u.target) != filepath.Dir(p.pgdata) || strings.Contains(u.target, "not-a-path") {
		t.Fatal("receipt escaped data directory")
	}
	args := upgradeArgs(request, s, p, u, true)
	for _, arg := range []string{"--old-bindir=/usr/lib/postgresql/16/bin", "--new-bindir=/usr/lib/postgresql/17/bin", "--link", "--check", "--old-port=55432", "--new-port=55433"} {
		if !slices.Contains(args, arg) {
			t.Error("missing argument", arg)
		}
	}
	for _, arg := range args {
		if strings.Contains(arg, s.Superuser.Password) {
			t.Fatal("superuser secret leaked into command arguments")
		}
	}
	run := upgradeArgs(request, s, p, u, false)
	if slices.Contains(run, "--check") {
		t.Fatal("upgrade-run only performed a check")
	}
}
func makeVersion(t *testing.T, path string, major int) {
	t.Helper()
	if err := os.MkdirAll(path, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(path, "PG_VERSION"), []byte(strconv.Itoa(major)), 0600); err != nil {
		t.Fatal(err)
	}
}
func TestUpgradeActivationResumesRenames(t *testing.T) {
	for _, crash := range []string{"before rename", "after old rename", "after new rename"} {
		t.Run(crash, func(t *testing.T) {
			p := dataPaths(t.TempDir())
			request := upgradeRequest()
			u := pathsForUpgrade(p, request)
			name, err := receiptPath(p, "upgrade", request.Receipt)
			if err != nil {
				t.Fatal(err)
			}
			makeVersion(t, p.pgdata, 16)
			makeVersion(t, u.target, 17)
			receipt := upgradeReceipt{Request: request, Phase: "linked", Checked: true}
			if crash != "before rename" {
				if err = os.Rename(p.pgdata, u.retired); err != nil {
					t.Fatal(err)
				}
			}
			if crash == "after new rename" {
				receipt.Phase = "old-retired"
				if err = os.Rename(u.target, p.pgdata); err != nil {
					t.Fatal(err)
				}
			}
			if err = activateUpgrade(p, u, &receipt, name); err != nil {
				t.Fatal(err)
			}
			if !receipt.Done || receipt.Phase != "done" {
				t.Fatal("activation receipt incomplete")
			}
			if major, err := pgMajorAt(p.pgdata); err != nil || major != 17 {
				t.Fatal("new data was not activated")
			}
			if major, err := pgMajorAt(u.retired); err != nil || major != 16 {
				t.Fatal("old linked data was not retained offline")
			}
			data, err := os.ReadFile(name)
			if err != nil {
				t.Fatal(err)
			}
			var saved upgradeReceipt
			if err = json.Unmarshal(data, &saved); err != nil || !saved.Done {
				t.Fatal("completion was not durable")
			}
		})
	}
}
func TestReplicaUpgradeRetiresOldData(t *testing.T) {
	p := dataPaths(t.TempDir())
	request := upgradeRequest()
	request.Replica = true
	u := pathsForUpgrade(p, request)
	name, err := receiptPath(p, "upgrade", request.Receipt)
	if err != nil {
		t.Fatal(err)
	}
	makeVersion(t, p.pgdata, 16)
	if err = os.WriteFile(filepath.Join(p.pgdata, "old-table"), []byte("obsolete replica"), 0600); err != nil {
		t.Fatal(err)
	}
	receipt := upgradeReceipt{Request: request, Phase: "planned"}
	if err = rebuildUpgradedReplica(p, u, &receipt, name); err != nil {
		t.Fatal(err)
	}
	entries, err := os.ReadDir(p.pgdata)
	if err != nil || len(entries) != 0 {
		t.Fatal("new replica data directory is not empty")
	}
	info, err := os.Stat(p.pgdata)
	if err != nil || info.Mode().Perm() != 0700 {
		t.Fatal("new replica data mode")
	}
	if !receipt.Done || !receipt.Checked {
		t.Fatal("replica checkpoint not completed")
	}
	if _, err = os.Stat(u.retired); !os.IsNotExist(err) {
		t.Fatal("retired replica still consumes basebackup capacity")
	}
}
func TestUpgradeRejectsRunningPostmaster(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "postmaster.pid"), []byte(strconv.Itoa(os.Getpid())+"\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if assertOfflineData(dir) == nil {
		t.Fatal("running process accepted as offline")
	}
}
