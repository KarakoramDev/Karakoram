// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package zerops

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/infra"
	"github.com/KarakoramDev/Karakoram/internal/infra/hoststombstones"
	"gopkg.in/yaml.v3"
)

type renderFixture struct {
	Hostname string            `json:"hostname"`
	Binary   string            `json:"binary"`
	Runtime  infra.RuntimeSpec `json:"runtime"`
}

func TestRenderYAMLGolden(t *testing.T) {
	paths, err := filepath.Glob("testdata/render/*.json")
	if err != nil {
		t.Fatal(err)
	}
	if len(paths) == 0 {
		t.Fatal("no render fixtures")
	}
	for _, filename := range paths {
		t.Run(strings.TrimSuffix(filepath.Base(filename), ".json"), func(t *testing.T) {
			data, err := os.ReadFile(filename)
			if err != nil {
				t.Fatal(err)
			}
			var fixture renderFixture
			if err = json.Unmarshal(data, &fixture); err != nil {
				t.Fatal(err)
			}
			want, err := os.ReadFile(strings.TrimSuffix(filename, ".json") + ".yaml")
			if err != nil {
				t.Fatal(err)
			}
			got, err := RenderYAML(fixture.Hostname, zeropsLayout, fixture.Binary, fixture.Runtime)
			if err != nil {
				t.Fatal(err)
			}
			if got != string(want) {
				t.Fatalf("rendered YAML differs from baseline\nwant:\n%s\ngot:\n%s", want, got)
			}
		})
	}
}

func TestRenderYAMLOptionalFields(t *testing.T) {
	got, err := RenderYAML("member", zeropsLayout, "karakoram", infra.RuntimeSpec{Base: infra.BaseUbuntu2404, Args: []string{"agent"}, Ports: []infra.Port{{Port: 8080, Protocol: "tcp", HTTP: true}, {Port: 9090, Protocol: "tcp"}}})
	if err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"prepareCommands:", "readinessCheck:", "volume:"} {
		if strings.Contains(got, key) {
			t.Errorf("unexpected %s", key)
		}
	}
	if strings.Count(got, "httpSupport: true") != 1 || strings.Contains(got, "httpSupport: false") {
		t.Fatalf("incorrect HTTP flags:\n%s", got)
	}
}

func TestRenderShutdownOnlyForVolumeWriters(t *testing.T) {
	for _, volume := range []string{"", "memberv"} {
		t.Run("volume="+volume, func(t *testing.T) {
			spec := infra.RuntimeSpec{Base: infra.BaseUbuntu2404, Args: []string{"agent"}, Volume: volume, StopBeforeStart: true, Readiness: &infra.Readiness{Args: []string{"agent", "readiness"}}}
			got, err := RenderYAML("member", zeropsLayout, "karakoram", spec)
			if err != nil {
				t.Fatal(err)
			}
			want := "temporaryShutdown: false"
			if volume != "" {
				want = "temporaryShutdown: true"
			}
			if !strings.Contains(got, want) || !strings.Contains(got, "readinessCheck:") {
				t.Fatalf("unsafe shutdown/readiness policy:\n%s", got)
			}
		})
	}
}

func TestRenderRelayRemainsSingleton(t *testing.T) {
	got, err := RenderYAML("relay", zeropsLayout, "karakoram", infra.RuntimeSpec{Base: infra.BaseUbuntu2404, Args: []string{"relay"}, StopBeforeStart: true})
	if err != nil || !strings.Contains(got, "temporaryShutdown: true") {
		t.Fatalf("overlapping relay sessions permitted:\n%s\n%v", got, err)
	}
}

func TestRenderMySQLTombstonePrepare(t *testing.T) {
	for _, engine := range []string{"mysql", "postgres", ""} {
		t.Run(engine, func(t *testing.T) {
			spec := infra.RuntimeSpec{
				Base:    infra.BaseUbuntu2404,
				Args:    []string{"agent"},
				Env:     map[string]string{"KARAKORAM_ENGINE": engine, "UNCHANGED": "value"},
				Prepare: []string{"prepare-engine-one", "prepare-engine-two"},
			}
			got, err := RenderYAML("member", zeropsLayout, "karakoram", spec)
			if err != nil {
				t.Fatal(err)
			}
			var doc struct {
				Zerops []struct {
					Run struct {
						Prepare []string          `yaml:"prepareCommands"`
						Env     map[string]string `yaml:"envVariables"`
						Start   string            `yaml:"start"`
					} `yaml:"run"`
				} `yaml:"zerops"`
			}
			if err := yaml.Unmarshal([]byte(got), &doc); err != nil {
				t.Fatal(err)
			}
			run := doc.Zerops[0].Run
			if run.Start != "./karakoram agent" || run.Env["UNCHANGED"] != "value" ||
				run.Prepare[0] != spec.Prepare[0] || run.Prepare[1] != spec.Prepare[1] {
				t.Fatal("engine preparation/start changed")
			}
			if _, changed := spec.Env["KARAKORAM_RESOLVER_TOMBSTONES"]; changed || len(spec.Prepare) != 2 {
				t.Fatal("caller runtime mutated")
			}
			if engine != "mysql" {
				if len(run.Prepare) != 2 || run.Env["KARAKORAM_RESOLVER_TOMBSTONES"] != "" || strings.Contains(got, "karakoram-hosts-tombstones") {
					t.Fatal("helper configured for non-MySQL")
				}
				return
			}
			if len(run.Prepare) != 3 || run.Env["KARAKORAM_RESOLVER_TOMBSTONES"] != "hosts-helper:/usr/local/libexec/karakoram-hosts-tombstones" {
				t.Fatal("MySQL writer missing")
			}
			prepare := run.Prepare[2]
			ordered := []string{
				"helperStage=$(/usr/bin/mktemp /usr/local/libexec/.karakoram-hosts-tombstones.XXXXXXXX)\n",
				"policyStage=$(/usr/bin/mktemp /etc/sudoers.d/.karakoram-hosts-tombstones.XXXXXXXX)\n",
				"/usr/bin/cat > \"$helperStage\" <<'KARAKORAM_HOSTS_HELPER'\n" + hoststombstones.Script + "KARAKORAM_HOSTS_HELPER\n",
				"/usr/bin/chown root:root \"$helperStage\"\n",
				"/usr/bin/chmod 0755 \"$helperStage\"\n",
				"/usr/bin/cat > \"$policyStage\" <<'KARAKORAM_HOSTS_POLICY'\nDefaults:zerops !setenv\nzerops ALL=(root) NOPASSWD: NOSETENV: /usr/local/libexec/karakoram-hosts-tombstones \"\"\n",
				"/usr/bin/chown root:root \"$policyStage\"\n",
				"/usr/bin/chmod 0440 \"$policyStage\"\n",
				"/usr/sbin/visudo -cf \"$policyStage\"\n",
				"/usr/bin/mv -f -- \"$helperStage\" \"$helper\"\n",
				"/usr/bin/mv -f -- \"$policyStage\" \"$policy\"\n",
			}
			remaining := prepare
			for _, part := range ordered {
				_, after, ok := strings.Cut(remaining, part)
				if !ok {
					t.Fatalf("missing/out-of-order prepare fragment %q", part)
				}
				remaining = after
			}
			if strings.Contains(prepare, "NOEXEC:") || strings.Contains(prepare, "NOPASSWD:ALL") {
				t.Fatal("helper policy blocks utilities or adds a blanket grant")
			}
			// Parse the complete generated command without executing sudo.
			cmd := exec.Command("/bin/sh", "-n")
			cmd.Stdin = strings.NewReader(prepare)
			if output, err := cmd.CombinedOutput(); err != nil {
				t.Fatalf("prepare syntax: %v: %s", err, output)
			}
		})
	}
}

func TestRenderMySQLTombstoneWithoutEnginePrepare(t *testing.T) {
	spec := infra.RuntimeSpec{Base: infra.BaseUbuntu2404, Env: map[string]string{"KARAKORAM_ENGINE": "mysql"}}
	got, err := RenderYAML("member", zeropsLayout, "karakoram", spec)
	if err != nil || !strings.Contains(got, "prepareCommands:") || !strings.Contains(got, "Defaults:zerops !setenv") {
		t.Fatalf("helper preparation requires no prior engine preparation: %v\n%s", err, got)
	}
}

func TestPrepareTombstoneStagingFailureAndReplay(t *testing.T) {
	for _, failure := range []string{"validation", "helper-move", "policy-move", ""} {
		t.Run(failure, func(t *testing.T) {
			dir := t.TempDir()
			helperDir := filepath.Join(dir, "libexec")
			policyDir := filepath.Join(dir, "sudoers.d")
			for _, path := range []string{helperDir, policyDir} {
				if err := os.Mkdir(path, 0755); err != nil {
					t.Fatal(err)
				}
			}
			helper := filepath.Join(helperDir, "karakoram-hosts-tombstones")
			policy := filepath.Join(policyDir, "karakoram-hosts-tombstones")
			for _, path := range []string{helper, policy} {
				if err := os.WriteFile(path, []byte("previous-"+filepath.Base(filepath.Dir(path))), 0600); err != nil {
					t.Fatal(err)
				}
			}
			// Only this private fixture substitutes privileged paths/tools. It
			// executes no sudo and cannot reach the production destinations.
			script := strings.Replace(prepareHostsTombstones(), "/usr/bin/sudo -n /bin/sh", "/bin/sh", 1)
			script = strings.ReplaceAll(script, "/usr/local/libexec", helperDir)
			script = strings.ReplaceAll(script, "/etc/sudoers.d", policyDir)
			script = strings.ReplaceAll(script, "/usr/bin/install -d -o root -g root", "/usr/bin/install -d")
			script = strings.ReplaceAll(script, "/usr/bin/chown root:root", "/usr/bin/true")
			validator := "/usr/bin/true"
			if failure == "validation" {
				validator = "/usr/bin/false"
			}
			script = strings.Replace(script, `/usr/sbin/visudo -cf "$policyStage"`, validator, 1)
			if failure == "helper-move" {
				script = strings.Replace(script, `/usr/bin/mv -f -- "$helperStage" "$helper"`, "/usr/bin/false", 1)
			}
			if failure == "policy-move" {
				script = strings.Replace(script, `/usr/bin/mv -f -- "$policyStage" "$policy"`, "/usr/bin/false", 1)
			}
			for range 2 {
				cmd := exec.Command("/bin/sh")
				cmd.Stdin = strings.NewReader(script)
				output, err := cmd.CombinedOutput()
				if (err != nil) != (failure != "") {
					t.Fatalf("installer: %v: %s", err, output)
				}
				for _, path := range []string{helper, policy} {
					data, err := os.ReadFile(path)
					if err != nil {
						t.Fatal(err)
					}
					if failure != "" {
						if string(data) != "previous-"+filepath.Base(filepath.Dir(path)) {
							t.Fatalf("active file changed after %s: %s", failure, path)
						}
					} else {
						info, err := os.Stat(path)
						if err != nil {
							t.Fatal(err)
						}
						wantMode := os.FileMode(0440)
						if path == helper {
							wantMode = 0755
							if string(data) != hoststombstones.Script {
								t.Fatal("activated helper differs")
							}
						}
						if info.Mode().Perm() != wantMode {
							t.Fatalf("mode %s: %o", path, info.Mode().Perm())
						}
					}
				}
				for _, path := range []string{helperDir, policyDir} {
					entries, err := os.ReadDir(path)
					if err != nil || len(entries) != 1 {
						t.Fatalf("staging files remain in %s: %v, %v", path, entries, err)
					}
				}
			}
		})
	}
}
