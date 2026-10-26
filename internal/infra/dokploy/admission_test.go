// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package dokploy

import (
	"errors"
	"strings"
	"sync"
	"testing"
)

func holdDeploymentApps(f *fakeAPI) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, a := range f.apps {
		a.holdDeployments = true
	}
}

func TestDeploymentSlotsBackpressureAndTerminalRelease(t *testing.T) {
	f := newFakeAPI(t)
	b := fakeBackend(t, f, nil)
	imported := importFake(t, b, runtimeSpec("one", ""), runtimeSpec("two", ""), runtimeSpec("three", ""))
	holdDeploymentApps(f)
	for _, name := range []string{"one", "two"} {
		if _, err := b.Deploy(t.Context(), name, testDeployment(t, "")); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := b.Deploy(t.Context(), "three", testDeployment(t, "")); !isDeploymentWait(err) || !strings.Contains(err.Error(), "2/2 in flight") {
		t.Fatalf("backpressure: %v", err)
	}
	if fakeCalls(f, "application.dropDeployment") != 2 {
		t.Fatal("waiting deployment submitted an archive")
	}
	f.mu.Lock()
	f.apps[imported.Services["one"]].deployments[0].status = "done"
	f.mu.Unlock()
	if _, err := b.Deploy(t.Context(), "three", testDeployment(t, "")); err != nil {
		t.Fatal(err)
	}
	if fakeCalls(f, "application.dropDeployment") != 3 {
		t.Fatal("terminal deployment did not release capacity")
	}
}

func TestDeploymentSlotsSharedAcrossEnvironments(t *testing.T) {
	f := newFakeAPI(t)
	b := fakeBackend(t, f, nil)
	importFake(t, b, runtimeSpec("one", ""), runtimeSpec("two", ""))
	holdDeploymentApps(f)
	for _, name := range []string{"one", "two"} {
		if _, err := b.Deploy(t.Context(), name, testDeployment(t, "")); err != nil {
			t.Fatal(err)
		}
	}
	f.mu.Lock()
	f.environments["other"] = map[string]any{"environmentId": "other", "projectId": "project", "name": "other"}
	f.mu.Unlock()
	target := b.target
	target.ID = "other"
	factory := newFactory(func(key string) string {
		if key == "KARAKORAM_DOKPLOY_ALLOWED_URLS" {
			return f.server.URL
		}
		return ""
	})
	backend, err := factory.New(target)
	if err != nil {
		t.Fatal(err)
	}
	other := backend.(*Backend)
	importFake(t, other, runtimeSpec("three", ""))
	if _, err := other.Deploy(t.Context(), "three", testDeployment(t, "")); !isDeploymentWait(err) || !strings.Contains(err.Error(), "2/2 in flight") {
		t.Fatalf("cross-environment cap: %v", err)
	}
	if fakeCalls(f, "application.dropDeployment") != 2 {
		t.Fatal("other environment bypassed server cap")
	}
}

func TestDeploymentSlotsConcurrentLastSlot(t *testing.T) {
	f := newFakeAPI(t)
	b := fakeBackend(t, f, nil)
	importFake(t, b, runtimeSpec("one", ""), runtimeSpec("two", ""), runtimeSpec("three", ""))
	holdDeploymentApps(f)
	if _, err := b.Deploy(t.Context(), "one", testDeployment(t, "")); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	errs := make(chan error, 2)
	for _, name := range []string{"two", "three"} {
		d := testDeployment(t, "")
		wg.Add(1)
		go func() { defer wg.Done(); _, err := fakeBackend(t, f, nil).Deploy(t.Context(), name, d); errs <- err }()
	}
	wg.Wait()
	close(errs)
	admitted, waiting := 0, 0
	for err := range errs {
		if err == nil {
			admitted++
		} else if isDeploymentWait(err) {
			waiting++
		} else {
			t.Fatal(err)
		}
	}
	if admitted != 1 || waiting != 1 || fakeCalls(f, "application.dropDeployment") != 2 {
		t.Fatalf("admitted=%d waiting=%d", admitted, waiting)
	}
}

func TestRedeployWaitsForExternalRunningDeployment(t *testing.T) {
	f := newFakeAPI(t)
	b := fakeBackend(t, f, nil)
	imported := importFake(t, b, runtimeSpec("runtime", ""))
	holdDeploymentApps(f)
	if err := b.c.post(t.Context(), "application.deploy", map[string]any{"applicationId": imported.Services["runtime"]}, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := b.Deploy(t.Context(), "runtime", testDeployment(t, "")); !isDeploymentWait(err) || !strings.Contains(err.Error(), "previous platform deployment") {
		t.Fatalf("redeploy while running: %v", err)
	}
	if fakeCalls(f, "application.dropDeployment") != 0 {
		t.Fatal("redeploy submitted during external deployment")
	}
}

func TestDeploymentSlotReservationAndCanceledPreparation(t *testing.T) {
	f := newFakeAPI(t)
	b := fakeBackend(t, f, nil)
	importFake(t, b, runtimeSpec("runtime", ""))
	id := fakeString(fakeAppSnapshot(t, f, "runtime"), "applicationId")
	release, err := b.deploymentSlot(t.Context(), id)
	if err != nil {
		t.Fatal(err)
	}
	release(false)
	release, err = b.deploymentSlot(t.Context(), id)
	if err != nil {
		t.Fatal(err)
	}
	release(true)
	if _, err := b.deploymentSlot(t.Context(), id); !isDeploymentWait(err) {
		t.Fatalf("unknown submission replayed: %v", err)
	}
}

func TestDeploymentLimitValidation(t *testing.T) {
	for _, raw := range []string{"0", "-1", "invalid"} {
		t.Run(raw, func(t *testing.T) {
			t.Setenv("KARAKORAM_DOKPLOY_MAX_INFLIGHT_DEPLOYS", raw)
			if _, err := deploymentLimit(); err == nil {
				t.Fatal("invalid cap accepted")
			}
		})
	}
	t.Setenv("KARAKORAM_DOKPLOY_MAX_INFLIGHT_DEPLOYS", "3")
	if n, err := deploymentLimit(); n != 3 || err != nil {
		t.Fatalf("cap=%d err=%v", n, err)
	}
}

func isDeploymentWait(err error) bool {
	var wait interface{ DeploymentWaiting() bool }
	return errors.As(err, &wait) && wait.DeploymentWaiting()
}
