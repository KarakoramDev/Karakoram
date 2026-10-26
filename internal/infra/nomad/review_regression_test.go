// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package nomad

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/infra"
	"github.com/KarakoramDev/Karakoram/internal/infra/contracttest"
	"github.com/KarakoramDev/Karakoram/internal/namespaces"
)

func TestRejectedDeployCannotChangeIncompleteImport(t *testing.T) {
	f := newFakeNomad(t)
	b := fakeBackend(t, f, nil)
	runtime := runtimeSpec("runtime", "volume")
	runtime.Env = map[string]string{"AGENT_TOKEN": "private-import-value"}
	specs := []infra.ServiceSpec{volumeSpec("volume"), runtime}
	base := b.c.http.Transport
	b.c.http.Transport = testRoundTripper(func(r *http.Request) (*http.Response, error) {
		if r.Method == http.MethodPut && r.URL.Path == "/v1/var/nomad/jobs/runtime" {
			return nil, io.ErrUnexpectedEOF
		}
		return base.RoundTrip(r)
	})
	partial, err := b.ImportServices(t.Context(), specs)
	if err == nil || len(partial.Services) != 2 {
		t.Fatalf("incomplete import fault not exercised: %+v %v", partial, err)
	}
	// Reconstruct the backend before normal observation and rejected admission.
	b = fakeBackend(t, f, nil)
	service, err := b.GetService(t.Context(), "runtime")
	if err != nil || service == nil || !service.Importing || service.Status != infra.StatusCreating {
		t.Fatalf("incomplete reservation not explicit: %+v %v", service, err)
	}
	before, err := b.require(t.Context(), "runtime")
	if err != nil {
		t.Fatal(err)
	}
	beforeJSON, _ := json.Marshal(before)
	envBefore, err := b.readVariable(t.Context(), "nomad/jobs/runtime")
	if err != nil {
		t.Fatal(err)
	}
	envBeforeJSON, _ := json.Marshal(envBefore)
	d := testDeployment(t, "volume")
	d.Runtime.Env = map[string]string{"KARAKORAM_URL": "https://control.example", "KARAKORAM_ENGINE": "postgres"}
	if _, err = b.Deploy(t.Context(), "runtime", d); !errors.Is(err, infra.ErrConflict) {
		t.Fatalf("incomplete import accepted deployment: %v", err)
	}
	after, err := b.require(t.Context(), "runtime")
	if err != nil {
		t.Fatal(err)
	}
	afterJSON, _ := json.Marshal(after)
	envAfter, err := b.readVariable(t.Context(), "nomad/jobs/runtime")
	if err != nil {
		t.Fatal(err)
	}
	envAfterJSON, _ := json.Marshal(envAfter)
	if string(beforeJSON) != string(afterJSON) || string(envBeforeJSON) != string(envAfterJSON) {
		t.Fatal("rejected Deploy mutated import-owned record/environment")
	}
	completed, err := b.ImportServices(t.Context(), specs)
	if err != nil || completed.Services["runtime"] != partial.Services["runtime"] || completed.Services["volume"] != partial.Services["volume"] {
		t.Fatalf("original batch could not resume: %+v %v", completed, err)
	}
	awaitFake(t, b, completed)
	deployFake(t, b, "runtime", "volume")
}

func TestRelayRenderHasNoCanaryOverlap(t *testing.T) {
	b, r := renderFixture(t)
	r.Volume, r.Node, r.VolumeID = "", "", ""
	r.Min, r.Max = 1, 1
	r.PublicPorts = nil
	r.Deployment.Runtime = namespaces.RelayRuntime("https://control.example", "test")
	_, group, _ := renderParts(t, b, r)
	update := group["Update"].(map[string]any)
	if update["Canary"] != 0 || update["MaxParallel"] != 1 || update["HealthCheck"] != "task_states" {
		t.Fatalf("relay replacement permits canary overlap: %#v", update)
	}
	// Application components retain serving-gated start-first canaries.
	r.Deployment.Runtime.Args = []string{"agent"}
	_, group, _ = renderParts(t, b, r)
	update = group["Update"].(map[string]any)
	if update["Canary"] != 1 || update["AutoPromote"] != true || update["HealthCheck"] != "checks" {
		t.Fatalf("application lost rolling availability: %#v", update)
	}
	surge := b.Capabilities().StatelessRolloutSurge
	if surge != 1 || update["Canary"] != surge || update["MaxParallel"] != surge {
		t.Fatalf("renderer exceeds advertised stateless rollout budget %d: %#v", surge, update)
	}
}

func TestRelayLifecycleStopsBeforeRegisteringReplacement(t *testing.T) {
	f := newFakeNomad(t)
	b := fakeBackend(t, f, nil)
	importFake(t, b, runtimeSpec("relay", ""))
	for range 2 {
		d := testDeployment(t, "")
		d.Runtime = namespaces.RelayRuntime("https://control.example", "test")
		op, err := b.Deploy(t.Context(), "relay", d)
		if err != nil {
			t.Fatal(err)
		}
		stored, err := decodeOp(op)
		if err != nil || stored.Phase != "stopping" {
			t.Fatalf("relay skipped singleton shutdown fence: %+v %v", stored, err)
		}
		awaitFake(t, b, op)
	}
}

func TestEngineSyncResumesBackendImportAfterRestart(t *testing.T) {
	f := newFakeNomad(t)
	contracttest.RunPostgresImportRecovery(t,
		func() infra.Backend { return fakeBackend(t, f, nil) },
		fakeArchive(t),
		func(backend infra.Backend) func() {
			b := backend.(*Backend)
			base := b.c.http.Transport
			b.c.http.Transport = testRoundTripper(func(r *http.Request) (*http.Response, error) {
				if r.Method == http.MethodPut && r.URL.Path == "/v1/var/nomad/jobs/demop0" {
					return nil, io.ErrUnexpectedEOF
				}
				return base.RoundTrip(r)
			})
			return func() { b.c.http.Transport = base }
		},
	)
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.volumes) != 0 {
		t.Fatal("cluster cleanup left a physical volume")
	}
}
