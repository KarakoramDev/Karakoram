// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package agent

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func backendVolumeScaffold(t *testing.T, aDataDir, backend string) {
	t.Helper()
	switch backend {
	case "nomad", "dokploy", "combined":
		// Both entrypoints acquire this descriptor before exec'ing the agent.
		if err := os.WriteFile(filepath.Join(aDataDir, ".karakoram-writer.lock"), nil, 0600); err != nil {
			t.Fatal(err)
		}
	}
	if backend == "zerops" || backend == "combined" {
		// A newly formatted ext4 volume has a root-owned namespace that the
		// unprivileged agent cannot necessarily enumerate.
		path := filepath.Join(aDataDir, "lost+found")
		if err := os.Mkdir(path, 0700); err != nil {
			t.Fatal(err)
		}
		if err := os.Chmod(path, 0000); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = os.Chmod(path, 0700) })
	}
}

func TestVolumeSafetyFreshBackendScaffoldingEveryFamily(t *testing.T) {
	for _, engine := range []string{"", "psmdb", "rabbitmq", "elasticsearch", "postgres", "mysql", "supabase", "openbao", "neo4j"} {
		for _, backend := range []string{"nomad", "dokploy", "zerops", "combined"} {
			t.Run(engine+"/"+backend, func(t *testing.T) {
				a, c := volumeFixture(t, engine, false)
				backendVolumeScaffold(t, a.o.DataDir, backend)
				if err := a.volumeSafety(t.Context(), c); err != nil {
					t.Fatal("fresh backend volume refused initialization", err)
				}
				if status := a.volumeObservation(); !status.Verified || status.Lost {
					t.Fatal("fresh backend volume not verified", status)
				}
				// The sentinel itself must not block subsequent checks. Its
				// contents still go through the identity validator before scanning.
				if err := a.volumeSafety(t.Context(), c); err != nil {
					t.Fatal("published sentinel blocked retry", err)
				}
			})
		}
	}
}

func TestVolumeSafetyInitializedBackendScaffoldingRemainsEmpty(t *testing.T) {
	for _, engine := range []string{"", "psmdb", "rabbitmq", "elasticsearch", "postgres", "mysql", "supabase", "openbao", "neo4j"} {
		for _, backend := range []string{"nomad", "dokploy", "zerops", "combined"} {
			t.Run(engine+"/"+backend, func(t *testing.T) {
				a, c := volumeFixture(t, engine, true)
				backendVolumeScaffold(t, a.o.DataDir, backend)
				if err := a.volumeSafety(t.Context(), c); err == nil || !strings.Contains(err.Error(), "VolumeLost") {
					t.Fatal("scaffolding obscured empty initialized storage", err)
				}
				if status := a.volumeObservation(); !status.Lost || status.Verified {
					t.Fatal("empty initialized native storage not reported lost", status)
				}
				if _, err := os.Stat(filepath.Join(a.o.DataDir, volumeSentinel)); !os.IsNotExist(err) {
					t.Fatal("empty initialized storage acquired a sentinel", err)
				}
			})
		}
	}
}

func TestVolumeSafetyBackendScaffoldingDoesNotHidePartialData(t *testing.T) {
	for _, engine := range []string{"", "psmdb", "rabbitmq", "elasticsearch", "postgres", "mysql", "supabase", "openbao", "neo4j"} {
		for _, initialized := range []bool{false, true} {
			t.Run(engine+"/initialized="+map[bool]string{false: "false", true: "true"}[initialized], func(t *testing.T) {
				a, c := volumeFixture(t, engine, initialized)
				backendVolumeScaffold(t, a.o.DataDir, "combined")
				volumeFile(t, a, "retained-native/partial-data", []byte("must preserve"))
				if err := a.volumeSafety(t.Context(), c); err == nil || !strings.Contains(err.Error(), "VolumeIdentityUnavailable") {
					t.Fatal("infrastructure exclusions hid retained data", err)
				}
				if status := a.volumeObservation(); status.Lost || status.Verified {
					t.Fatal("partial storage authorized destructive recovery", status)
				}
			})
		}
	}
}

func TestVolumeSafetyInfrastructureNamesDoNotPermitSymlinkOrSentinelBypass(t *testing.T) {
	for _, name := range []string{".karakoram-writer.lock", "lost+found", volumeSentinel} {
		t.Run(name, func(t *testing.T) {
			a, c := volumeFixture(t, "postgres", false)
			if name == volumeSentinel {
				volumeFile(t, a, name, []byte("corrupt identity"))
			} else {
				target := filepath.Join(t.TempDir(), "foreign")
				if err := os.WriteFile(target, []byte("retained"), 0600); err != nil {
					t.Fatal(err)
				}
				if err := os.Symlink(target, filepath.Join(a.o.DataDir, name)); err != nil {
					t.Fatal(err)
				}
			}
			if err := a.volumeSafety(t.Context(), c); err == nil || !strings.Contains(err.Error(), "VolumeIdentityUnavailable") {
				t.Fatal("infrastructure name bypassed native/identity safety", err)
			}
			if status := a.volumeObservation(); status.Lost || status.Verified {
				t.Fatal("ambiguous infrastructure path was adopted", status)
			}
		})
	}
}
