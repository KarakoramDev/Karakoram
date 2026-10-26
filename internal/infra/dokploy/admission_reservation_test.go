// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package dokploy

import (
	"errors"
	"sync"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/infra"
)

func TestDeploymentSlotAtomicReservation(t *testing.T) {
	t.Setenv("KARAKORAM_DOKPLOY_MAX_INFLIGHT_DEPLOYS", "1")
	f := newFakeAPI(t)
	b := fakeBackend(t, f, nil)
	imported := importFake(t, b, runtimeSpec("one", ""), runtimeSpec("two", ""))
	errs := make(chan error, 2)
	var wg sync.WaitGroup
	for _, name := range []string{"one", "two"} {
		wg.Add(1)
		go func() {
			defer wg.Done()
			release, err := b.deploymentSlot(t.Context(), imported.Services[name])
			if err == nil {
				release(true)
			}
			errs <- err
		}()
	}
	wg.Wait()
	close(errs)
	admitted := 0
	for err := range errs {
		if err == nil {
			admitted++
		} else if !isDeploymentWait(err) {
			t.Fatal(err)
		}
	}
	if admitted != 1 {
		t.Fatalf("last slot admitted %d concurrent reservations", admitted)
	}
}

func TestAmbiguousWipeConsumesSlotAfterProcessRestart(t *testing.T) {
	t.Setenv("KARAKORAM_DOKPLOY_MAX_INFLIGHT_DEPLOYS", "1")
	f := newFakeAPI(t)
	b := fakeBackend(t, f, nil)
	importFake(t, b, volumeSpec("volume"), runtimeSpec("runtime", ""))
	f.mu.Lock()
	f.failure["application.deploy"] = fakeResponse{500, "INTERNAL_SERVER_ERROR", "delivery unknown"}
	f.mu.Unlock()
	if _, err := b.DeleteService(t.Context(), "volume"); !errors.Is(err, infra.ErrUnavailable) {
		t.Fatal(err)
	}
	deploymentServers.Delete(b.target.Config["url"])
	if _, err := fakeBackend(t, f, nil).Deploy(t.Context(), "runtime", testDeployment(t, "")); !isDeploymentWait(err) {
		t.Fatalf("ambiguous wipe lost capacity fence: %v", err)
	}
	if fakeCalls(f, "application.dropDeployment") != 0 {
		t.Fatal("runtime submitted despite durable wipe fence")
	}
}
