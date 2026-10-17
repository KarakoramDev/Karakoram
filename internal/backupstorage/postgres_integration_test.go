//go:build integration

// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package backupstorage

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// Run as a non-root user in an isolated native PostgreSQL environment. This
// checks our exact info-file checksum against pgBackRest itself, then restores
// a remaining backup after Go has expired an older full and its incrementals.
func TestPostgresInfoNativeCompatibility(t *testing.T) {
	bin := os.Getenv("KARAKORAM_PG_BIN_DIR")
	if bin == "" {
		t.Skip("KARAKORAM_PG_BIN_DIR is required")
	}
	if os.Geteuid() == 0 {
		t.Fatal("native PostgreSQL integration must run as a non-root user")
	}
	root := t.TempDir()
	pgdata := filepath.Join(root, "pgdata")
	repo := filepath.Join(root, "repo")
	socket := filepath.Join(root, "socket")
	if err := os.Mkdir(socket, 0700); err != nil {
		t.Fatal(err)
	}
	run := func(binary string, args ...string) []byte {
		t.Helper()
		command := exec.Command(binary, args...)
		output, err := command.CombinedOutput()
		if err != nil {
			t.Fatalf("%s failed: %v\n%s", filepath.Base(binary), err, output)
		}
		return output
	}
	run(filepath.Join(bin, "initdb"), "-D", pgdata, "--auth=trust", "--data-checksums", "--no-locale")
	config := filepath.Join(root, "pgbackrest.conf")
	text := "[global]\nrepo1-type=posix\nrepo1-path=" + repo + "\nrepo1-retention-full=9999999\nexpire-auto=n\nlog-level-console=warn\nlog-level-file=off\nlock-path=" + filepath.Join(root, "lock") + "\n[kk]\npg1-path=" + pgdata + "\npg1-socket-path=" + socket + "\npg1-port=55432\n"
	if err := os.WriteFile(config, []byte(text), 0600); err != nil {
		t.Fatal(err)
	}
	pgconf := "\nport=55432\nlisten_addresses=''\nunix_socket_directories='" + socket + "'\narchive_mode=on\narchive_command='/usr/bin/pgbackrest --config=" + config + " --stanza=kk archive-push %p'\n"
	f, err := os.OpenFile(filepath.Join(pgdata, "postgresql.conf"), os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		t.Fatal(err)
	}
	_, err = f.WriteString(pgconf)
	closeErr := f.Close()
	if err != nil || closeErr != nil {
		t.Fatal(err, closeErr)
	}
	run(filepath.Join(bin, "pg_ctl"), "-D", pgdata, "-l", filepath.Join(root, "postgres.log"), "-w", "start")
	defer func() {
		_ = exec.Command(filepath.Join(bin, "pg_ctl"), "-D", pgdata, "-m", "immediate", "-w", "stop").Run()
	}()
	backrest := func(args ...string) []byte {
		return run("/usr/bin/pgbackrest", append([]string{"--config=" + config, "--stanza=kk"}, args...)...)
	}
	backrest("stanza-create")
	backrest("--type=full", "backup")
	time.Sleep(time.Second)
	backrest("--type=incr", "backup")
	time.Sleep(time.Second)
	backrest("--type=full", "backup")
	var inventory []struct {
		Backup []struct {
			Label string `json:"label"`
		} `json:"backup"`
	}
	if err = json.Unmarshal(backrest("--output=json", "info"), &inventory); err != nil || len(inventory) != 1 || len(inventory[0].Backup) != 3 {
		t.Fatal("native backup inventory", err)
	}
	first := inventory[0].Backup[0].Label
	remaining := inventory[0].Backup[2].Label
	infoFile := filepath.Join(repo, "backup", "kk", "backup.info")
	original, err := os.ReadFile(infoFile)
	if err != nil {
		t.Fatal(err)
	}
	plan, err := planPostgresExpire(original, first)
	if err != nil {
		t.Fatal("cannot parse native info checksum", err)
	}
	if len(plan.Removed) != 2 || plan.Empty {
		t.Fatal("incorrect native dependency closure", plan.Removed)
	}
	for _, name := range []string{"backup.info", "backup.info.copy"} {
		if err = os.WriteFile(filepath.Join(repo, "backup", "kk", name), plan.Info, 0600); err != nil {
			t.Fatal(err)
		}
	}
	for _, label := range plan.Removed {
		if err = os.RemoveAll(filepath.Join(repo, "backup", "kk", label)); err != nil {
			t.Fatal(err)
		}
	}
	output := backrest("--output=json", "info")
	if strings.Contains(string(output), first) || !strings.Contains(string(output), remaining) {
		t.Fatal("pgBackRest rejected rewritten inventory", string(output))
	}
	restore := filepath.Join(root, "restored")
	backrest("--pg1-path="+restore, "--set="+remaining, "--type=immediate", "--archive-mode=off", "restore")
	if _, err = os.Stat(filepath.Join(restore, "PG_VERSION")); err != nil {
		t.Fatal("native restore of remaining set failed", err)
	}
	t.Log("native pgBackRest accepted Go-rewritten info and restored remaining full set")
}
