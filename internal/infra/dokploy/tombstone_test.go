// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package dokploy

import (
	"archive/zip"
	"bytes"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func helperFixture(t *testing.T, initial string) (string, func(string) error) {
	return helperFixtureWithHook(t, initial, func(script string) string { return script })
}

func helperFixtureWithHook(t *testing.T, initial string, hook func(string) string) (string, func(string) error) {
	t.Helper()
	dir := t.TempDir()
	hosts := filepath.Join(dir, "hosts")
	if err := os.WriteFile(hosts, []byte(initial), 0644); err != nil {
		t.Fatal(err)
	}
	// Production has no path override or environment-controlled destination. Only
	// this private test copy substitutes the two literal paths, without mounts.
	script := strings.Replace(hostsTombstonesHelper, "hosts=/etc/hosts", "hosts='"+hosts+"'", 1)
	script = strings.Replace(script, "lock=/etc/.karakoram-hosts-tombstones.lock", "lock='"+filepath.Join(dir, "lock")+"'", 1)
	script = hook(script)
	path := filepath.Join(dir, "helper.sh")
	if err := os.WriteFile(path, []byte(script), 0600); err != nil {
		t.Fatal(err)
	}
	return hosts, func(input string) error {
		cmd := exec.Command("sh", path)
		cmd.Stdin = strings.NewReader(input)
		output, err := cmd.CombinedOutput()
		if err != nil && len(output) == 0 {
			t.Fatal("failure without diagnostic")
		}
		return err
	}
}
func readHosts(t *testing.T, path string) string {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return string(data)
}
func TestHostsTombstonesTransform(t *testing.T) {
	foreign := "127.0.0.1 localhost\n# operator comment\n192.0.2.20 live live-alias\n"
	shared := "192.0.2.2 alias # duplicate\n"
	path, run := helperFixture(t, foreign+"192.0.2.1 retired\n192.0.2.2 alias retired # duplicate\n")
	before, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := run("retired\nretired\n"); err != nil {
		t.Fatal(err)
	}
	want := foreign + shared + "# BEGIN karakoram resolution tombstones\n239.192.0.1 retired\nff18::4b4b:1 retired\n# END karakoram resolution tombstones\n"
	if got := readHosts(t, path); got != want {
		t.Fatalf("hosts = %q, want %q", got, want)
	}
	after, _ := os.Stat(path)
	if !os.SameFile(before, after) || after.Mode().Perm() != 0644 {
		t.Fatal("hosts inode or permissions changed")
	}
	if err := run("retired\n"); err != nil {
		t.Fatal(err)
	}
	if readHosts(t, path) != want {
		t.Fatal("not idempotent")
	}
	if err := run("new-retired\n"); err != nil {
		t.Fatal(err)
	}
	got := readHosts(t, path)
	if strings.Contains(got, " retired\n") || !strings.Contains(got, "239.192.0.1 new-retired\n") {
		t.Fatal(got)
	}
	if err := run(""); err != nil {
		t.Fatal(err)
	}
	if got := readHosts(t, path); got != foreign+shared {
		t.Fatalf("remove = %q", got)
	}
}
func TestHostsTombstonesRejectInput(t *testing.T) {
	own, err := exec.Command("uname", "-n").Output()
	if err != nil {
		t.Fatal(err)
	}
	for _, input := range []string{"bad.name\n", "Upper\n", "bad name\n", "\n", "a\r\n", "-start\n", "a;exit\n", strings.Repeat("a", 64) + "\n", strings.Repeat("retired\n", 65), strings.TrimSpace(string(own)) + "\n", "valid\nbad.name\n"} {
		t.Run(strings.TrimSpace(input), func(t *testing.T) {
			path, run := helperFixture(t, "127.0.0.1 localhost\n")
			if run(input) == nil {
				t.Fatal("invalid input accepted")
			}
			if readHosts(t, path) != "127.0.0.1 localhost\n" {
				t.Fatal("rejected input mutated hosts")
			}
		})
	}
	path, run := helperFixture(t, "# BEGIN karakoram resolution tombstones\n")
	if run("retired\n") == nil {
		t.Fatal("malformed block accepted")
	}
	if readHosts(t, path) != "# BEGIN karakoram resolution tombstones\n" {
		t.Fatal("malformed block mutated")
	}
}
func TestRenderMySQLTombstoneHelper(t *testing.T) {
	d := renderTestDeployment(t)
	d.Runtime.Env["KARAKORAM_ENGINE"] = "mysql"
	files, _ := renderTestZip(t, d)
	if string(files["karakoram-hosts-tombstones"]) != hostsTombstonesHelper {
		t.Fatal("helper asset missing")
	}
	df := string(files["Dockerfile"])
	rule := "karakoram ALL=(root) NOPASSWD: NOSETENV: /usr/local/libexec/karakoram-hosts-tombstones \"\""
	_, runtime, ok := strings.Cut(df, "USER root\nRUN rm /etc/sudoers.d/karakoram\n")
	if !ok || strings.Contains(runtime, "NOPASSWD:ALL") || strings.Count(runtime, "karakoram ALL=") != 2 {
		t.Fatal("runtime must contain exactly volume and helper narrow rules:\n" + df)
	}
	if strings.Count(runtime, rule) != 1 || !strings.Contains(runtime, "'Defaults:karakoram !setenv' '"+rule+"'") ||
		!strings.Contains(runtime, "COPY --chown=root:root --chmod=0755 [\"karakoram-hosts-tombstones\"") ||
		!strings.Contains(runtime, "chmod 0440 /etc/sudoers.d/karakoram-hosts-tombstones && visudo -cf") ||
		!strings.Contains(runtime, "chmod 0440 /etc/sudoers.d/karakoram-volume && visudo -cf") ||
		strings.LastIndex(df, "USER ") != strings.LastIndex(df, "USER karakoram\n") {
		t.Fatal(df)
	}
	for _, copy := range []string{
		`COPY --chown=root:root --chmod=0644 ["karakoram.env","/opt/karakoram/karakoram.env"]`,
		`COPY --chown=root:root --chmod=0755 ["karakoram-entrypoint","/usr/local/bin/karakoram-entrypoint"]`,
	} {
		if !strings.Contains(runtime, copy) {
			t.Fatal("runtime artifact not protected: " + copy)
		}
	}
	d.Runtime.Volume = ""
	stateless := renderDockerfile(d)
	_, runtime, _ = strings.Cut(stateless, "USER root\nRUN rm /etc/sudoers.d/karakoram\n")
	if strings.Count(runtime, "karakoram ALL=") != 1 || !strings.Contains(runtime, "'Defaults:karakoram !setenv' '"+rule+"'") || strings.Contains(runtime, "NOEXEC:") {
		t.Fatal("stateless MySQL must retain only the executable NOSETENV helper rule:\n" + stateless)
	}
	if !strings.Contains(string(files["karakoram.env"]), "hosts-helper:/usr/local/libexec/karakoram-hosts-tombstones") {
		t.Fatal("writer environment missing")
	}
	d = renderTestDeployment(t)
	files, _ = renderTestZip(t, d)
	if _, ok := files["karakoram-hosts-tombstones"]; ok || strings.Contains(string(files["karakoram.env"]), "RESOLVER_TOMBSTONES") {
		t.Fatal("writer configured for non-MySQL runtime")
	}
}

func TestDeployTombstoneWriterRuntime(t *testing.T) {
	for _, engine := range []string{"mysql", "elasticsearch"} {
		t.Run(engine, func(t *testing.T) {
			f := newFakeAPI(t)
			b := fakeBackend(t, f, nil)
			imported := importFake(t, b, volumeSpec("volume"), runtimeSpec("member", "volume"))
			d := testDeployment(t, "volume")
			d.Runtime.Env = map[string]string{"KARAKORAM_ENGINE": engine}
			op, err := b.Deploy(t.Context(), "member", d)
			if err != nil {
				t.Fatal(err)
			}
			awaitFake(t, b, op)
			if fakeCalls(f, "application.dropDeployment") != 1 {
				t.Fatal("deployment must upload its rendered runtime")
			}
			f.mu.Lock()
			uploaded := bytes.Clone(f.apps[imported.Services["member"]].upload)
			f.mu.Unlock()
			zr, err := zip.NewReader(bytes.NewReader(uploaded), int64(len(uploaded)))
			if err != nil {
				t.Fatal(err)
			}
			files := map[string]string{}
			for _, file := range zr.File {
				r, err := file.Open()
				if err != nil {
					t.Fatal(err)
				}
				data, err := io.ReadAll(r)
				r.Close()
				if err != nil {
					t.Fatal(err)
				}
				files[file.Name] = string(data)
			}
			env := files["karakoram.env"]
			if !strings.Contains(env, "export KARAKORAM_ENGINE='"+engine+"'\n") {
				t.Fatal("deployment lost runtime engine environment")
			}
			if !strings.Contains(files["karakoram-entrypoint"], ". /opt/karakoram/karakoram.env\nexec \"$@\"\n") {
				t.Fatal("deployed entrypoint does not load runtime environment")
			}
			df := files["Dockerfile"]
			for _, instruction := range []string{
				`COPY --chown=root:root --chmod=0644 ["karakoram.env","/opt/karakoram/karakoram.env"]`,
				`COPY --chown=root:root --chmod=0755 ["karakoram-entrypoint","/usr/local/bin/karakoram-entrypoint"]`,
				`ENTRYPOINT ["/usr/local/bin/karakoram-entrypoint"]`,
			} {
				if !strings.Contains(df, instruction) {
					t.Fatalf("deployed runtime missing %s", instruction)
				}
			}
			if engine != "mysql" {
				if _, ok := files["karakoram-hosts-tombstones"]; ok ||
					strings.Contains(env, "KARAKORAM_RESOLVER_TOMBSTONES") ||
					strings.Contains(df, "karakoram-hosts-tombstones") {
					t.Fatal("non-MySQL deployment received tombstone writer setup")
				}
				return
			}
			if files["karakoram-hosts-tombstones"] != hostsTombstonesHelper {
				t.Fatal("deployment lost tombstone helper asset")
			}
			if !strings.Contains(env, "export KARAKORAM_RESOLVER_TOMBSTONES='hosts-helper:/usr/local/libexec/karakoram-hosts-tombstones'\n") {
				t.Fatal("deployment lost tombstone writer environment")
			}
			for _, instruction := range []string{
				`COPY --chown=root:root --chmod=0755 ["karakoram-hosts-tombstones","/usr/local/libexec/karakoram-hosts-tombstones"]`,
				`karakoram ALL=(root) NOPASSWD: NOSETENV: /usr/local/libexec/karakoram-hosts-tombstones ""`,
				"chmod 0440 /etc/sudoers.d/karakoram-hosts-tombstones && visudo -cf /etc/sudoers.d/karakoram-hosts-tombstones",
			} {
				if !strings.Contains(df, instruction) {
					t.Fatalf("deployed writer missing %s", instruction)
				}
			}
			if strings.LastIndex(df, "USER ") != strings.LastIndex(df, "USER karakoram\n") {
				t.Fatal("deployed writer runtime must remain unprivileged")
			}
		})
	}
}

func TestHostsTombstonesConcurrentChange(t *testing.T) {
	const initial = "127.0.0.1 localhost\n"
	const added = "192.0.2.9 docker-added\n"
	path, run := helperFixtureWithHook(t, initial, func(script string) string {
		return strings.Replace(script, `cmp -s "$tmp/backup" "$hosts" || continue`,
			`if [ "$attempt" -eq 1 ]; then printf '%s\n' '192.0.2.9 docker-added' >> "$hosts"; fi
cmp -s "$tmp/backup" "$hosts" || continue`, 1)
	})
	if err := run("retired\n"); err != nil {
		t.Fatal(err)
	}
	if got := readHosts(t, path); !strings.HasPrefix(got, initial+added) || !strings.Contains(got, "239.192.0.1 retired\n") {
		t.Fatal("concurrent Docker update lost: " + got)
	}
}

func TestHostsTombstonesConcurrentChangeExhausted(t *testing.T) {
	path, run := helperFixtureWithHook(t, "127.0.0.1 localhost\n", func(script string) string {
		return strings.Replace(script, `cmp -s "$tmp/backup" "$hosts" || continue`,
			`printf '%s\n' '# Docker update' >> "$hosts"
cmp -s "$tmp/backup" "$hosts" || continue`, 1)
	})
	if run("retired\n") == nil {
		t.Fatal("continually changing hosts accepted")
	}
	if got := readHosts(t, path); strings.Count(got, "# Docker update\n") != 3 || strings.Contains(got, "tombstones") {
		t.Fatal(got)
	}
}

func TestHostsTombstonesFailedWriteRestoresOriginal(t *testing.T) {
	const initial = "127.0.0.1 localhost\n192.0.2.1 live\n"
	for _, failure := range []string{`{ printf 'partial' > "$hosts"; false; }`, `printf 'corrupt' > "$hosts"`} {
		path, run := helperFixtureWithHook(t, initial, func(script string) string {
			return strings.Replace(script, `printf '%s' "$replacement" > "$hosts"`, failure, 1)
		})
		if run("retired\n") == nil {
			t.Fatal("failed or corrupt write accepted")
		}
		if got := readHosts(t, path); got != initial {
			t.Fatalf("original not restored: %q", got)
		}
	}
}
