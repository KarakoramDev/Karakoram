// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package contracttest

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/postgres"
	"github.com/KarakoramDev/Karakoram/internal/infra"
	"github.com/KarakoramDev/Karakoram/internal/secrets"
	"github.com/KarakoramDev/Karakoram/internal/store"
)

type importArtifact []byte

func (a importArtifact) Open(context.Context) (io.ReadCloser, error) {
	return io.NopCloser(bytes.NewReader(a)), nil
}

type importClock struct{ at time.Time }

func (c *importClock) Now() time.Time { return c.at }

// RunPostgresImportRecovery exercises ordinary provider Sync against a stateful
// backend fixture. inject must refuse import setup after recording a reservation;
// its return value removes the fault. It must not replay ImportServices itself.
// Both initial runtime+volume and runtime-only retained-volume imports are tested,
// reconstructing the provider/backend from persisted CP state after each fault.
func RunPostgresImportRecovery(t *testing.T, newBackend func() infra.Backend, artifact []byte, inject func(infra.Backend) func()) {
	t.Helper()
	sealer, err := secrets.New(strings.Repeat("x", 32))
	if err != nil {
		t.Fatal(err)
	}
	st, err := store.Open(filepath.Join(t.TempDir(), "state.db"), sealer)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	options := postgres.Options{Store: st, Logger: logger, ControlPlaneURL: "https://control.example"}
	provider := postgres.New(options)
	cluster := &domain.Cluster{Name: "demo", Namespace: "default", Generation: 1, Spec: domain.ClusterSpec{Topology: domain.Topology{Type: "single"}}}
	provider.ApplyDefaults(&cluster.Spec)
	proxy := cluster.Spec.Components.Extra["proxy"]
	proxy.Replicas = 0
	cluster.Spec.Components.Extra["proxy"] = proxy
	if err = st.CreateCluster(t.Context(), cluster); err != nil {
		t.Fatal(err)
	}
	clock := &importClock{at: time.Now().UTC()}
	rc := &engine.Context{Cluster: cluster, Store: st, Secrets: st, Backend: newBackend(), Artifact: importArtifact(artifact), Logger: logger, Clock: clock}
	persist := func() {
		t.Helper()
		if err := st.UpdateClusterState(t.Context(), rc.Cluster); err != nil {
			t.Fatal(err)
		}
	}
	reconstruct := func() {
		t.Helper()
		var err error
		rc.Cluster, err = st.GetCluster(t.Context(), cluster.Ref())
		if err != nil {
			t.Fatal(err)
		}
		rc.Backend = newBackend()
		provider = postgres.New(options)
	}
	cleanup := func(retain bool) {
		t.Helper()
		for range 150 {
			done, err := provider.Cleanup(t.Context(), rc, domain.DeleteOptions{KeepVolumes: retain, KeepBackups: true})
			persist()
			if err != nil {
				t.Fatal("ordinary cluster cleanup failed", err)
			}
			if done {
				return
			}
			time.Sleep(time.Millisecond)
		}
		t.Fatal("ordinary cluster cleanup did not converge")
	}
	volumeID := ""
	for _, mode := range []string{"initial", "runtime-only"} {
		removeFault := inject(rc.Backend)
		failed := false
		for range 40 {
			_, err = provider.Sync(t.Context(), rc)
			persist()
			if err != nil {
				failed = true
				break
			}
		}
		if !failed {
			removeFault()
			t.Fatalf("%s setup fault was not exercised", mode)
		}
		services, err := rc.Backend.ListServices(t.Context(), "demo")
		if err != nil {
			removeFault()
			t.Fatal(err)
		}
		identities := map[string]string{}
		incomplete := false
		for _, service := range services {
			identities[service.Hostname] = service.ID
			incomplete = incomplete || service.Importing
		}
		if !incomplete {
			removeFault()
			t.Fatalf("%s failure left no explicit incomplete reservation", mode)
		}
		removeFault()
		reconstruct()
		// Resume only after the caller's persisted import retry deadline, just
		// as the outer reconciler would; do not bypass or erase its journal.
		clock.at = clock.at.Add(time.Minute)
		running := false
		for range 150 {
			if _, err = provider.Sync(t.Context(), rc); err != nil {
				t.Fatalf("%s ordinary Sync could not resume import: %v", mode, err)
			}
			persist()
			runtime, err := rc.Backend.GetService(t.Context(), "demop0")
			if err != nil {
				t.Fatal(err)
			}
			if runtime != nil && !runtime.Importing && (runtime.Status == infra.StatusRunning || runtime.Status == infra.StatusActive) {
				running = true
				break
			}
			time.Sleep(time.Millisecond)
		}
		if !running {
			t.Fatalf("%s ordinary Sync never deployed the resumed runtime", mode)
		}
		services, err = rc.Backend.ListServices(t.Context(), "demo")
		if err != nil || len(services) != 2 {
			t.Fatalf("%s lost or duplicated infrastructure: %d %v", mode, len(services), err)
		}
		for _, service := range services {
			if service.Importing || identities[service.Hostname] != "" && identities[service.Hostname] != service.ID {
				t.Fatalf("%s replaced an original reservation: %+v", mode, service)
			}
			if service.Kind == infra.KindVolume {
				if volumeID != "" && volumeID != service.ID {
					t.Fatal("runtime-only recovery replaced retained storage")
				}
				volumeID = service.ID
			}
		}
		if mode == "initial" {
			cleanup(true)
			reconstruct()
		}
	}
	cleanup(false)
	remaining, err := rc.Backend.ListServices(t.Context(), "demo")
	if err != nil || len(remaining) != 0 {
		t.Fatalf("resumed cluster could not be fully deleted: %d %v", len(remaining), err)
	}
}
