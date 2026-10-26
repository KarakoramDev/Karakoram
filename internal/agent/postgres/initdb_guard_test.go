// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package postgres

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/engine/postgres/pgspec"
	"gopkg.in/yaml.v3"
)

func haInitDBFixture(t *testing.T, fail bool) (pgspec.Spec, paths, string) {
	t.Helper()
	p := dataPaths(filepath.Join(t.TempDir(), "volume with spaces"))
	if err := privateDir(p.root); err != nil {
		t.Fatal(err)
	}
	s := testSpec()
	s.Topology = pgspec.TopologyHA
	s.BinDir = filepath.Join(p.root, "native binaries")
	if err := privateDir(s.BinDir); err != nil {
		t.Fatal(err)
	}
	body := "#!/bin/sh\nset -eu\ntest -f " + shellQuote(filepath.Join(p.root, "initdb-attempt.json")) + "\nprintf 'called\\n' >> " + shellQuote(filepath.Join(p.root, "native-calls")) + "\nprintf '%s\\n' \"$@\" > " + shellQuote(filepath.Join(p.root, "native-args")) + "\n"
	if fail {
		body += "exit 23\n"
	} else {
		body += "mkdir -p " + shellQuote(p.pgdata) + "\nprintf '17\\n' > " + shellQuote(filepath.Join(p.pgdata, "PG_VERSION")) + "\n"
	}
	if err := os.WriteFile(filepath.Join(s.BinDir, "initdb"), []byte(body), 0700); err != nil {
		t.Fatal(err)
	}
	// A valid volume sentinel must not substitute for initialization consumption.
	if err := os.WriteFile(filepath.Join(filepath.Dir(p.root), "volume-identity.json"), []byte(`{"volumeID":"same-volume"}`), 0600); err != nil {
		t.Fatal(err)
	}
	files, err := render(s, p, nil)
	if err != nil {
		t.Fatal(err)
	}
	var native map[string]any
	for _, file := range files {
		mode := os.FileMode(0600)
		if file.executable {
			mode = 0700
		}
		if err := os.WriteFile(filepath.Join(p.root, file.name), []byte(file.content), mode); err != nil {
			t.Fatal(err)
		}
		if file.name == "patroni.yml" {
			if err := yaml.Unmarshal([]byte(file.content), &native); err != nil {
				t.Fatal(err)
			}
		}
	}
	bootstrap := native["bootstrap"].(map[string]any)
	if _, custom := bootstrap["method"]; custom {
		t.Fatal("guard replaced Patroni's native bootstrap lifecycle")
	}
	pg := native["postgresql"].(map[string]any)
	wrapper := pg["bin_name"].(map[string]any)["initdb"].(string)
	return s, p, wrapper
}

func TestHAInitDBCachedRestartConsumesAuthorization(t *testing.T) {
	for _, scenario := range []string{"interrupted-initdb", "missing-data-before-publication", "failed-post-bootstrap"} {
		t.Run(scenario, func(t *testing.T) {
			s, p, wrapper := haInitDBFixture(t, scenario == "interrupted-initdb")
			args := []string{"-D", p.pgdata, "--username=postgres", "--pwfile=/private/password with spaces", "--data-checksums", "--encoding=UTF8", "--locale=C.UTF-8", "--auth-host=scram-sha-256", "--auth-local=scram-sha-256"}
			_, err := exec.CommandContext(t.Context(), wrapper, args...).CombinedOutput()
			if (err != nil) != (scenario == "interrupted-initdb") {
				t.Fatalf("unexpected first native initdb outcome: %v", err)
			}
			data, err := os.ReadFile(filepath.Join(p.root, "native-args"))
			if err != nil || !reflect.DeepEqual(strings.Split(strings.TrimSuffix(string(data), "\n"), "\n"), args) {
				t.Fatalf("Patroni's native initdb arguments changed: %q, %v", data, err)
			}
			data, err = os.ReadFile(filepath.Join(p.root, "initdb-attempt.json"))
			var consumed bool
			if err != nil || json.Unmarshal(data, &consumed) != nil || !consumed {
				t.Fatalf("initdb ran without its durable consumption receipt: %q, %v", data, err)
			}
			if scenario == "failed-post-bootstrap" {
				if err := os.WriteFile(filepath.Join(s.BinDir, "psql"), []byte("#!/bin/sh\nexit 23\n"), 0700); err != nil {
					t.Fatal(err)
				}
				if err := exec.CommandContext(t.Context(), filepath.Join(p.root, "bootstrap-roles.sh")).Run(); err == nil {
					t.Fatal("post_bootstrap failure fixture succeeded")
				}
				if _, err := os.Stat(filepath.Join(p.root, "initialized.json")); err != nil {
					t.Fatal("post_bootstrap did not preserve its initialization receipt", err)
				}
			}
			if scenario != "interrupted-initdb" {
				// Patroni moves failed bootstrap data aside. Do not rerender or pull
				// any configuration before restarting with the same cached YAML.
				if err := os.Rename(p.pgdata, p.pgdata+".failed"); err != nil {
					t.Fatal(err)
				}
			}
			output, err := exec.CommandContext(t.Context(), wrapper, args...).CombinedOutput()
			if err == nil || !strings.Contains(string(output), missingDataMessage) {
				t.Fatalf("cached native bootstrap did not refuse a second identity: %q, %v", output, err)
			}
			calls, err := os.ReadFile(filepath.Join(p.root, "native-calls"))
			if err != nil || string(calls) != "called\n" {
				t.Fatalf("native initdb executed more than once: %q, %v", calls, err)
			}
		})
	}
}

func TestHAInitDBGuardRequiresFreshAuthorization(t *testing.T) {
	for _, scenario := range []string{"completed", "revoked", "restore", "other-seed", "missing-bootstrap"} {
		t.Run(scenario, func(t *testing.T) {
			s, p, wrapper := haInitDBFixture(t, false)
			switch scenario {
			case "completed":
				if err := recordInitialization(p); err != nil {
					t.Fatal(err)
				}
			case "revoked":
				s.Bootstrap.AllowInitDB = false
			case "restore":
				s.Bootstrap.Method = "restore"
			case "other-seed":
				s.Bootstrap.SeedMember = "member1"
			case "missing-bootstrap":
				s.Bootstrap = nil
			}
			if err := os.WriteFile(wrapper, []byte(renderInitDBGuard(s, p)), 0700); err != nil {
				t.Fatal(err)
			}
			if output, err := exec.CommandContext(t.Context(), wrapper).CombinedOutput(); err == nil || !strings.Contains(string(output), missingDataMessage) {
				t.Fatalf("unauthorized native initdb was not refused: %q, %v", output, err)
			}
			if _, err := os.Stat(filepath.Join(p.root, "native-calls")); !os.IsNotExist(err) {
				t.Fatal("unauthorized native initdb executed", err)
			}
		})
	}
}

func TestHAInitDBGuardSerializesNativeAttempts(t *testing.T) {
	_, p, wrapper := haInitDBFixture(t, false)
	var workers sync.WaitGroup
	for range 8 {
		workers.Go(func() {
			_ = exec.CommandContext(t.Context(), wrapper).Run()
		})
	}
	workers.Wait()
	calls, err := os.ReadFile(filepath.Join(p.root, "native-calls"))
	if err != nil || string(calls) != "called\n" {
		t.Fatalf("competing bootstrap attempts created multiple identities: %q, %v", calls, err)
	}
}
