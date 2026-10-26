// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package dokploy

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/infra"
	"github.com/KarakoramDev/Karakoram/internal/infra/contracttest"
	"github.com/KarakoramDev/Karakoram/internal/namespaces"
)

func TestDefinitiveUploadRefusalReleasesOnlyRejectedAttempt(t *testing.T) {
	for _, status := range []int{400, 401, 403, 429} {
		t.Run(fmt.Sprint(status), func(t *testing.T) {
			f := newFakeAPI(t)
			b := fakeBackend(t, f, nil)
			id := importFake(t, b, runtimeSpec("runtime", "")).Services["runtime"]
			f.mu.Lock()
			f.failureOnce["application.dropDeployment"] = fakeResponse{status, "BAD_REQUEST", "submission refused"}
			f.mu.Unlock()
			base := http.DefaultTransport
			b.c.http.Transport = restoreTransport(func(r *http.Request) (*http.Response, error) {
				response, err := base.RoundTrip(r)
				if err == nil && response.StatusCode == 429 {
					response.Header.Set("Retry-After", "1")
				}
				return response, err
			})
			op, err := b.Deploy(t.Context(), "runtime", testDeployment(t, ""))
			if err == nil || op.ID != "" || fakeMeta(t, f, "runtime").Pending != nil {
				t.Fatalf("definite refusal retained an ambiguous receipt: %+v %v", op, err)
			}
			if status == 429 {
				var retry *infra.RetryAfterError
				if !errors.As(err, &retry) || !retry.At.After(time.Now()) {
					t.Fatalf("Retry-After lost: %v", err)
				}
			}
			f.mu.Lock()
			builds := len(f.apps[id].deployments)
			f.mu.Unlock()
			if builds != 0 {
				t.Fatal("refusal unexpectedly accepted a build")
			}
			// Nothing volatile from the refused submission is needed for retry.
			b = fakeBackend(t, f, nil)
			op, err = b.Deploy(t.Context(), "runtime", testDeployment(t, ""))
			if err != nil {
				t.Fatal(err)
			}
			awaitFake(t, b, op)
			f.mu.Lock()
			builds = len(f.apps[id].deployments)
			f.mu.Unlock()
			if builds != 1 || fakeCalls(f, "application.dropDeployment") != 2 {
				t.Fatalf("retry did not accept exactly one build: %d", builds)
			}
			stop, err := b.Stop(t.Context(), "runtime")
			if err != nil {
				t.Fatal(err)
			}
			awaitFake(t, b, stop)
			deleted, err := b.DeleteService(t.Context(), "runtime")
			if err != nil {
				t.Fatal(err)
			}
			awaitFake(t, b, deleted)
		})
	}
}

func TestSettingsConsumePriorDeploymentBeforeMergingIntent(t *testing.T) {
	for _, change := range []string{"resources", "count"} {
		t.Run(change, func(t *testing.T) {
			f := newFakeAPI(t)
			b := fakeBackend(t, f, nil)
			id := importFake(t, b, runtimeSpec("runtime", "")).Services["runtime"]
			if _, err := b.Deploy(t.Context(), "runtime", testDeployment(t, "")); err != nil {
				t.Fatal(err)
			}
			// The platform is terminal, but the CP has not consumed its receipt.
			f.mu.Lock()
			a := f.apps[id]
			f.apply(a)
			a.deployments[0].status = "done"
			for _, task := range a.tasks {
				task["currentState"] = "Running 1 second ago"
			}
			f.mu.Unlock()
			var op infra.Op
			var err error
			if change == "resources" {
				op, err = b.SetResources(t.Context(), "runtime", infra.Resources{MinCPU: 2, MaxCPU: 3, MinRAM: 2, MaxRAM: 4})
			} else {
				op, err = b.SetContainerCount(t.Context(), "runtime", 2, 3)
			}
			if err != nil {
				t.Fatal(err)
			}
			awaitFake(t, b, op)
			b = fakeBackend(t, f, nil)
			service, err := b.GetService(t.Context(), "runtime")
			if err != nil || service == nil {
				t.Fatal("updated service unavailable", err)
			}
			if change == "resources" && (service.Resources.MinCPU != 2 || service.Resources.MaxCPU != 3 || service.Resources.MaxRAM != 4) || change == "count" && (service.MinContainers != 2 || service.MaxContainers != 3) {
				t.Fatalf("new settings overwritten by old completion: %+v", service)
			}
			deployFake(t, b, "runtime", "")
			metadata := fakeMeta(t, f, "runtime")
			row := fakeAppSnapshot(t, f, "runtime")
			if change == "resources" && (metadata.Resources.MaxCPU != 3 || row["cpuLimit"] != "3000000000") || change == "count" && (metadata.Max != 3 || fakeInt(row, "replicas") != 3) {
				t.Fatalf("accepted settings were not durable: %+v %#v", metadata, row)
			}
		})
	}
}

func TestTargetGuardWaitHonorsCancellationDuringUpload(t *testing.T) {
	f := newFakeAPI(t)
	b := fakeBackend(t, f, nil)
	importFake(t, b, runtimeSpec("upload", ""), runtimeSpec("other", ""))
	entered, release := make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	defer unblock()
	base := http.DefaultTransport
	b.c.http.Transport = restoreTransport(func(r *http.Request) (*http.Response, error) {
		if r.URL.Path == "/api/application.dropDeployment" {
			close(entered)
			<-release
		}
		return base.RoundTrip(r)
	})
	done := make(chan error, 1)
	d := testDeployment(t, "")
	go func() { _, err := b.Deploy(t.Context(), "upload", d); done <- err }()
	<-entered
	other := fakeBackend(t, f, nil)
	for _, action := range []string{"observe", "stop"} {
		ctx, cancel := context.WithTimeout(t.Context(), 25*time.Millisecond)
		returned := make(chan error, 1)
		go func() {
			if action == "observe" {
				_, err := other.GetService(ctx, "other")
				returned <- err
			} else {
				_, err := other.Stop(ctx, "other")
				returned <- err
			}
		}()
		select {
		case err := <-returned:
			if !errors.Is(err, context.DeadlineExceeded) {
				t.Errorf("%s ignored cancellation: %v", action, err)
			}
		case <-time.After(time.Second):
			t.Error("target exclusion trapped canceled request")
		}
		cancel()
	}
	if fakeCalls(f, "application.stop") != 0 {
		t.Fatal("canceled stop mutated an unrelated application")
	}
	unblock()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if fakeMeta(t, f, "upload").Pending == nil || fakeCalls(f, "application.dropDeployment") != 1 {
		t.Fatal("canceled observer changed upload's durable fence")
	}
}

func TestRelayUpdatesRemainStopFirst(t *testing.T) {
	f := newFakeAPI(t)
	b := fakeBackend(t, f, nil)
	importFake(t, b, runtimeSpec("relay", ""), runtimeSpec("proxy", ""))
	for _, name := range []string{"relay", "proxy"} {
		d := testDeployment(t, "")
		want := "start-first"
		if name == "relay" {
			d.Runtime = namespaces.RelayRuntime("https://control.example", "test")
			want = "stop-first"
		}
		op, err := b.Deploy(t.Context(), name, d)
		if err != nil {
			t.Fatal(err)
		}
		awaitFake(t, b, op)
		row := fakeAppSnapshot(t, f, name)
		for _, key := range []string{"updateConfigSwarm", "rollbackConfigSwarm"} {
			if settings, ok := row[key].(map[string]any); !ok || settings["Order"] != want || fakeInt(settings, "Parallelism") != 1 {
				t.Fatalf("%s %s violates rollout classification: %#v", name, key, row[key])
			}
			if name == "proxy" && (b.Capabilities().StatelessRolloutSurge != 1 || fakeInt(row[key].(map[string]any), "Parallelism") != b.Capabilities().StatelessRolloutSurge) {
				t.Fatalf("renderer exceeds advertised stateless rollout budget: %#v", row[key])
			}
		}
	}
}

func TestEngineSyncResumesBackendImportAfterRestart(t *testing.T) {
	f := newFakeAPI(t)
	contracttest.RunPostgresImportRecovery(t,
		func() infra.Backend { return fakeBackend(t, f, nil) },
		renderTestArtifact(t),
		func(infra.Backend) func() {
			f.mu.Lock()
			f.failure["application.saveEnvironment"] = fakeResponse{500, "INTERNAL_SERVER_ERROR", "temporary setup refusal"}
			f.mu.Unlock()
			return func() {
				f.mu.Lock()
				delete(f.failure, "application.saveEnvironment")
				f.mu.Unlock()
			}
		},
	)
}

func TestStorageBindsHoldersOwnIdentityNotRuntimesMarker(t *testing.T) {
	f := newFakeAPI(t)
	b := fakeBackend(t, f, nil)
	volume := volumeSpec("volume")
	volume.Vars = map[string]string{"KARAKORAM_OWNER": "volume-specific-marker"}
	runtime := runtimeSpec("runtime", "volume")
	runtime.Vars = map[string]string{"KARAKORAM_OWNER": "runtime-specific-marker"}
	ids := importFake(t, b, volume, runtime).Services
	deployFake(t, b, "runtime", "volume")
	m := fakeMeta(t, f, "runtime")
	if m.VolumeID != ids["volume"] || m.VolumeOwner != volume.Vars["KARAKORAM_OWNER"] {
		t.Fatalf("runtime did not bind original holder identity: %+v", m)
	}
	f.mu.Lock()
	holder := f.apps[ids["volume"]]
	vm, _ := parseMetadata(fakeString(holder.row, "description"))
	vm.Vars["KARAKORAM_OWNER"] = "foreign-marker"
	holder.row["description"] = vm.description()
	f.mu.Unlock()
	if _, err := b.GetService(t.Context(), "runtime"); !errors.Is(err, infra.ErrConflict) {
		t.Fatalf("changed holder ownership was accepted: %v", err)
	}
	f.mu.Lock()
	vm.Vars["KARAKORAM_OWNER"] = volume.Vars["KARAKORAM_OWNER"]
	holder.row["description"] = vm.description()
	f.mu.Unlock()
	if service, err := b.GetService(t.Context(), "runtime"); err != nil || service == nil || service.Status != infra.StatusRunning {
		t.Fatalf("original differently-marked holder was not restored: %+v %v", service, err)
	}
}
