// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package tombstone

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestFromEnv(t *testing.T) {
	for _, tc := range []struct {
		value, kind string
		bad         bool
	}{
		{"", "", false}, {"nomad-variable:", "nomad-variable", false}, {"hosts-helper:/usr/local/libexec/helper", "hosts-helper", false},
		{"nomad-variable:/file", "", true}, {"nomad-file:/file", "", true}, {"hosts-helper:relative", "", true}, {"hosts-helper:/tmp/../helper", "", true}, {"hosts-helper:/helper\n", "", true}, {"unknown:/file", "", true},
	} {
		t.Run(tc.value, func(t *testing.T) {
			writer, err := FromEnv(func(key string) string {
				if key != "KARAKORAM_RESOLVER_TOMBSTONES" {
					t.Fatal(key)
				}
				return tc.value
			})
			if (err != nil) != tc.bad {
				t.Fatalf("error = %v", err)
			}
			if tc.kind != "" && writer.Kind() != tc.kind {
				t.Fatalf("writer = %#v", writer)
			}
			if tc.value == "" && writer != nil {
				t.Fatal("unsupported must be nil")
			}
		})
	}
}
func TestHostsHelperInvocation(t *testing.T) {
	var argv []string
	var stdin string
	w := &hostsHelper{path: "/usr/local/libexec/helper", run: func(_ context.Context, command string, args []string, input string) error {
		argv = append([]string{command}, args...)
		stdin = input
		return nil
	}}
	if err := w.Install(context.Background(), []string{"retired-a", "retired-b"}); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(argv, []string{"/usr/bin/sudo", "-n", w.path}) || stdin != "retired-a\nretired-b\n" {
		t.Fatalf("argv=%v stdin=%q", argv, stdin)
	}
	for _, hosts := range [][]string{{"bad.name"}, {""}, make([]string, 65)} {
		if err := w.Install(context.Background(), hosts); err == nil {
			t.Fatal("accepted invalid input")
		}
	}
	w.run = func(context.Context, string, []string, string) error { return errors.New("denied") }
	if w.Install(context.Background(), nil) == nil {
		t.Fatal("helper error lost")
	}
}
func TestHostsHelperPreflightRejectsUnsafeFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "helper")
	if err := os.WriteFile(path, []byte("#!/bin/sh\n"), 0777); err != nil {
		t.Fatal(err)
	}
	// Do not let the test runner's umask make this fixture safe under root.
	if err := os.Chmod(path, 0777); err != nil {
		t.Fatal(err)
	}
	w := &hostsHelper{path: path, run: func(context.Context, string, []string, string) error { t.Fatal("unsafe helper executed"); return nil }}
	if w.Preflight(context.Background()) == nil {
		t.Fatal("unsafe helper accepted")
	}
}
func TestNomadVariableAcknowledgement(t *testing.T) {
	path := filepath.Join(t.TempDir(), "hosts")
	write := func(data string) {
		t.Helper()
		if err := os.WriteFile(path, []byte(data), 0600); err != nil {
			t.Fatal(err)
		}
	}
	w := &nomadVariable{hostsPath: path}
	write("127.0.0.1 localhost\n")
	if w.Preflight(context.Background()) == nil {
		t.Fatal("old template accepted")
	}
	data := nomadMarker + "\n239.192.0.1 retired\nff18::4b4b:1 retired\n"
	write(data)
	if err := w.Install(context.Background(), []string{"retired"}); err != nil {
		t.Fatal(err)
	}
	actual, _ := os.ReadFile(path)
	if string(actual) != data {
		t.Fatal("acknowledger mutated hosts")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := w.Install(ctx, nil); err == nil {
		t.Fatal("stale managed entries acknowledged")
	}
	write(nomadMarker + "\n")
	if err := w.Install(context.Background(), nil); err != nil {
		t.Fatal(err)
	}
}

func TestNomadVariableInstallAndReleaseWaitForProjection(t *testing.T) {
	path := filepath.Join(t.TempDir(), "hosts")
	project := func(data string) {
		t.Helper()
		// A watched projection replaces the complete file; never expose a
		// truncated file that could look like an acknowledged empty set.
		next := path + ".next"
		if err := os.WriteFile(next, []byte(data), 0600); err != nil {
			t.Fatal(err)
		}
		if err := os.Rename(next, path); err != nil {
			t.Fatal(err)
		}
	}
	empty := nomadMarker + "\n127.0.0.1 localhost\n"
	ipv4 := "239.192.0.1 retired\n"
	ipv6 := "ff18::4b4b:1 retired\n"
	project(empty)
	w := &nomadVariable{hostsPath: path}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	const pollers = 4
	start := func(hosts []string) <-chan error {
		started := make(chan struct{}, pollers)
		results := make(chan error, pollers)
		for range pollers {
			go func() {
				started <- struct{}{}
				results <- w.Install(ctx, hosts)
			}()
		}
		for range pollers {
			select {
			case <-started:
			case <-ctx.Done():
				t.Fatalf("pollers did not start: %v", ctx.Err())
			}
		}
		return results
	}
	assertPending := func(stage string, hosts []string, results <-chan error) {
		t.Helper()
		// An independent bounded poll proves the current projection is still
		// rejected before allowing the next watched update, without sleeps.
		probe, stop := context.WithTimeout(ctx, 100*time.Millisecond)
		defer stop()
		if err := w.Install(probe, hosts); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("%s: unprojected set acknowledged or unexpected error: %v", stage, err)
		}
		select {
		case err := <-results:
			t.Fatalf("%s: concurrent acknowledgement completed early: %v", stage, err)
		default:
		}
	}
	assertAcknowledged := func(stage string, results <-chan error, projection string) {
		t.Helper()
		for range pollers {
			select {
			case err := <-results:
				if err != nil {
					t.Fatalf("%s: acknowledgement failed: %v", stage, err)
				}
			case <-ctx.Done():
				t.Fatalf("%s: acknowledgement did not complete: %v", stage, ctx.Err())
			}
		}
		actual, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		if string(actual) != projection {
			t.Fatalf("%s: acknowledger mutated the watched projection: %q", stage, actual)
		}
	}

	installed := start([]string{"retired"})
	assertPending("install before watched update", []string{"retired"}, installed)
	project(empty + ipv4)
	assertPending("install missing IPv6", []string{"retired"}, installed)
	project(empty + ipv4 + ipv6)
	assertAcknowledged("install", installed, empty+ipv4+ipv6)

	released := start(nil)
	assertPending("release with stale dual-stack sentinels", nil, released)
	project(empty + ipv6)
	assertPending("release with stale IPv6 sentinel", nil, released)
	project(empty + ipv4)
	assertPending("release with stale IPv4 sentinel", nil, released)
	project(empty)
	assertAcknowledged("release", released, empty)
}
func TestHostsMatch(t *testing.T) {
	valid := "239.192.0.1 retired\nff18::4b4b:1 retired\n"
	for _, data := range []string{"", "239.192.0.1 retired\n", valid + "192.0.2.1 retired\n", valid + "239.192.0.1 other\n", strings.ReplaceAll(valid, "retired", "other")} {
		if hostsMatch(data, []string{"retired"}) {
			t.Fatalf("accepted %q", data)
		}
	}
	if !hostsMatch("127.0.0.1 localhost\n"+valid, []string{"retired"}) {
		t.Fatal("valid dual stack rejected")
	}
}

func TestHostsHelperPreflightInvocation(t *testing.T) {
	var argv []string
	w := &hostsHelper{path: "/usr/bin/true", run: func(_ context.Context, name string, args []string, input string) error {
		argv = append([]string{name}, args...)
		if input != "" {
			t.Fatal("preflight must not submit hostnames")
		}
		return nil
	}}
	if err := w.Preflight(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(argv, []string{"/usr/bin/sudo", "-n", "-l", "/usr/bin/true"}) {
		t.Fatal(argv)
	}
}

func TestHostsHelperHostilePATH(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("PATH", dir)
	t.Setenv("LC_ALL", "hostile")
	t.Setenv("HOSTILE_INHERITED", "must-not-leak")
	// Never run sudo on the test host: the injected runner checks the command
	// selected by both callers and executes an absolute, unprivileged fixture.
	fixture := filepath.Join(dir, "fixture")
	capture := filepath.Join(dir, "environment")
	if err := os.WriteFile(fixture, []byte("#!/bin/sh\n/usr/bin/env > '"+capture+"'\n"), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "sudo"), []byte("#!/bin/sh\nexit 99\n"), 0700); err != nil {
		t.Fatal(err)
	}
	calls := 0
	w := &hostsHelper{path: "/usr/bin/true", run: func(ctx context.Context, name string, args []string, input string) error {
		calls++
		if name != "/usr/bin/sudo" {
			t.Fatalf("PATH-dependent command: %q", name)
		}
		return runHelper(ctx, fixture, nil, input)
	}}
	if err := w.Preflight(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := w.Install(context.Background(), []string{"retired"}); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(capture)
	if err != nil {
		t.Fatal(err)
	}
	got := string(data)
	if calls != 2 || !strings.Contains(got, "PATH=/usr/sbin:/usr/bin:/sbin:/bin\n") || !strings.Contains(got, "LC_ALL=C\n") || strings.Contains(got, "HOSTILE_INHERITED") || strings.Contains(got, dir) {
		t.Fatalf("calls=%d environment=%q", calls, got)
	}
}

func TestHostsHelperZeropsFactory(t *testing.T) {
	t.Setenv("USER", "zerops")
	t.Setenv("LOGNAME", "zerops")
	w, err := FromEnv(func(key string) string {
		if key == "KARAKORAM_RESOLVER_TOMBSTONES" {
			return "hosts-helper:/usr/local/libexec/karakoram-hosts-tombstones"
		}
		t.Fatalf("factory must not depend on runtime user: %q", key)
		return ""
	})
	if err != nil {
		t.Fatal(err)
	}
	helper, ok := w.(*hostsHelper)
	if !ok || helper.path != "/usr/local/libexec/karakoram-hosts-tombstones" {
		t.Fatalf("Zerops writer = %#v", w)
	}
	called := false
	helper.run = func(_ context.Context, command string, args []string, input string) error {
		called = true
		if command != "/usr/bin/sudo" || !reflect.DeepEqual(args, []string{"-n", helper.path}) || input != "retired\n" {
			t.Fatalf("Zerops helper invocation: %q %v %q", command, args, input)
		}
		return nil
	}
	if err := helper.Install(context.Background(), []string{"retired"}); err != nil || !called {
		t.Fatalf("Zerops install: %v; called=%v", err, called)
	}
}
