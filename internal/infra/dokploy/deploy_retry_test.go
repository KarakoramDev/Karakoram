// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package dokploy

import (
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/infra"
)

func TestFailedDeploymentObservationAllowsExplicitRetry(t *testing.T) {
	for _, status := range []string{"error", "cancelled"} {
		for _, persisted := range []bool{false, true} {
			t.Run(status+map[bool]string{false: "/unobserved", true: "/persisted"}[persisted], func(t *testing.T) {
				f := newFakeAPI(t)
				b := fakeBackend(t, f, nil)
				id := importFake(t, b, volumeSpec("volume"), runtimeSpec("runtime", "volume")).Services["runtime"]
				first, err := b.Deploy(t.Context(), "runtime", testDeployment(t, "volume"))
				if err != nil {
					t.Fatal(err)
				}
				f.mu.Lock()
				a := f.apps[id]
				a.holdDeployments = true
				a.deployments[0].status = status
				a.deployments[0].finishedAt = time.Now().UTC().Format(time.RFC3339Nano)
				a.row["applicationStatus"] = status
				f.mu.Unlock()
				if persisted {
					state, err := b.OpStatus(t.Context(), first)
					if err != nil || !state.Failed {
						t.Fatalf("failed receipt: %+v %v", state, err)
					}
				}
				b = fakeBackend(t, f, nil)
				// The engine's ownership gate must still see the imported identity
				// after a terminal build failure, including after CP reconstruction.
				for range 3 {
					service, err := b.GetService(t.Context(), "runtime")
					if err != nil || service == nil || service.ID != id || service.Status != infra.StatusFailed || service.Hostname != "runtime" || service.Kind != infra.KindRuntime {
						t.Fatalf("failed runtime identity: %+v %v", service, err)
					}
				}
				if fakeCalls(f, "application.dropDeployment") != 1 || fakeCalls(f, "application.deploy") != 0 || fakeCalls(f, "application.reload") != 0 {
					t.Fatal("observation retried a failed deployment")
				}
				if m := fakeMeta(t, f, "runtime"); m.Pending == nil || !m.Pending.Failed || m.Pending.Uncertain {
					t.Fatalf("terminal receipt not retained: %+v", m.Pending)
				}
				f.mu.Lock()
				a.holdDeployments = false
				f.mu.Unlock()
				retry, err := b.Deploy(t.Context(), "runtime", testDeployment(t, "volume"))
				if err != nil || retry.ID == first.ID {
					t.Fatalf("explicit retry blocked or reused failed receipt: %+v %v", retry, err)
				}
				awaitFake(t, b, retry)
				if fakeCalls(f, "application.dropDeployment") != 2 {
					t.Fatal("retry did not submit exactly one new build")
				}
			})
		}
	}
}

func TestUncertainDeploymentObservationDoesNotPermitReplay(t *testing.T) {
	for _, missing := range []bool{false, true} {
		t.Run(map[bool]string{false: "building", true: "unknown-queue"}[missing], func(t *testing.T) {
			f := newFakeAPI(t)
			b := fakeBackend(t, f, nil)
			id := importFake(t, b, runtimeSpec("runtime", "")).Services["runtime"]
			first, err := b.Deploy(t.Context(), "runtime", testDeployment(t, ""))
			if err != nil {
				t.Fatal(err)
			}
			f.mu.Lock()
			f.apps[id].holdDeployments = true
			if missing {
				f.apps[id].deployments = nil
			}
			f.mu.Unlock()
			o, err := decodeOp(first)
			if err != nil {
				t.Fatal(err)
			}
			b.now = func() time.Time { return o.Issued.Add(buildProgressWindow + time.Second) }
			for range 2 {
				if _, err := b.GetService(t.Context(), "runtime"); err == nil {
					t.Fatal("uncertain build was exposed as safely retryable")
				}
			}
			recovered, err := b.Deploy(t.Context(), "runtime", testDeployment(t, ""))
			if err != nil {
				t.Fatal(err)
			}
			receipt, err := decodeOp(recovered)
			if err != nil || !sameOperation(&receipt, o) || !receipt.Uncertain || fakeCalls(f, "application.dropDeployment") != 1 {
				t.Fatalf("uncertain build replayed: %+v %v", recovered, err)
			}
		})
	}
}
