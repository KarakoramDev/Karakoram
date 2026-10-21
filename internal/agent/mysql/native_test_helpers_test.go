//go:build integration

// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

// Keep paths short enough for MySQL's Unix sockets. A caller can select a local
// filesystem with KARAKORAM_MYSQL_TEST_TMPDIR without a checkout-relative lab.
func nativeTestDir(t *testing.T) string {
	t.Helper()
	root := os.Getenv("KARAKORAM_MYSQL_TEST_TMPDIR")
	if root == "" {
		root = os.TempDir()
	}
	if err := os.MkdirAll(root, 0700); err != nil {
		t.Fatal(err)
	}
	dir, err := os.MkdirTemp(root, "kkmy-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	return dir
}

var nativePorts = struct {
	sync.Mutex
	used map[int]bool
}{used: make(map[int]bool)}

// Native executables cannot inherit a Go net.Listener. Allocate via the OS and
// reject ports already allocated by this suite; check UDP too for Galera. There
// remains the usual close-to-bind race with unrelated processes, not a fixed
// listener collision between independent checkouts or test cases.
func nativeTestPort(t *testing.T) int {
	t.Helper()
	nativePorts.Lock()
	defer nativePorts.Unlock()
	for range 100 {
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		port := listener.Addr().(*net.TCPAddr).Port
		if nativePorts.used[port] {
			_ = listener.Close()
			continue
		}
		udp, err := net.ListenPacket("udp", fmt.Sprintf("127.0.0.1:%d", port))
		_ = listener.Close()
		if err != nil {
			continue
		}
		_ = udp.Close()
		nativePorts.used[port] = true
		return port
	}
	t.Fatal("could not allocate a distinct native test port")
	return 0
}

func nativeTestAddress(t *testing.T) string {
	t.Helper()
	return fmt.Sprintf("127.0.0.1:%d", nativeTestPort(t))
}

func assertNativeMySQLOnlyProxy(t *testing.T, db *sql.DB) {
	t.Helper()
	for _, name := range []string{"admin-pgsql_ifaces", "pgsql-interfaces"} {
		var value string
		if err := db.QueryRow("SELECT variable_value FROM runtime_global_variables WHERE variable_name=" + proxyQuote(name)).Scan(&value); err != nil {
			t.Fatalf("read unused protocol listener %s: %v", name, err)
		}
		if !filepath.IsAbs(value) || strings.Contains(value, ";") || !strings.HasSuffix(value, ".sock") {
			t.Fatalf("unused PostgreSQL listener %s is not a private Unix socket: %q", name, value)
		}
		info, err := os.Stat(value)
		if err != nil || info.Mode()&os.ModeSocket == 0 {
			t.Fatalf("unused protocol Unix socket %s unavailable: %v", name, err)
		}
		dir, err := os.Stat(filepath.Dir(value))
		if err != nil || dir.Mode().Perm()&0077 != 0 {
			t.Fatalf("unused protocol socket directory is not owner-only: %v", err)
		}
	}
}

// Exercise the real Fatal/Cleanup path without starting a native database.
func TestNativeAgentCleanupAfterFatal(t *testing.T) {
	const markerEnv = "KARAKORAM_NATIVE_CLEANUP_MARKER"
	if marker := os.Getenv(markerEnv); marker != "" {
		dir := nativeTestDir(t)
		ctx, cancel := context.WithCancel(context.Background())
		a := &groupRunnerAgent{t: t, cancel: cancel, done: make(chan error, 1)}
		t.Cleanup(a.close)
		go func() {
			<-ctx.Done()
			// Model a server that takes time to finish its shutdown. The
			// datadir must remain intact until agent.Run reports completion.
			time.Sleep(25 * time.Millisecond)
			_, err := os.Stat(dir)
			if err == nil {
				err = os.WriteFile(marker, []byte(dir), 0600)
			}
			a.done <- err
		}()
		t.Fatal("intentional assertion failure exercising native cleanup")
	}
	marker := filepath.Join(t.TempDir(), "reaped-before-delete")
	cmd := exec.Command(os.Args[0], "-test.run=^TestNativeAgentCleanupAfterFatal$")
	cmd.Env = append(os.Environ(), markerEnv+"="+marker)
	raw, err := cmd.CombinedOutput()
	if err == nil || !strings.Contains(string(raw), "intentional assertion failure") {
		t.Fatalf("cleanup helper did not take the expected Fatal path: %v %s", err, raw)
	}
	dir, err := os.ReadFile(marker)
	if err != nil {
		t.Fatal("native cleanup removed data before owned agent exit", err)
	}
	if _, err := os.Stat(string(dir)); !os.IsNotExist(err) {
		t.Fatal("native datadir was not removed after agent exit", err)
	}
}
