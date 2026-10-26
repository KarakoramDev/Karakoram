// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package dokploy

import (
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/infra"
)

func finishPublicOperation(t *testing.T, b *Backend, f *fakeAPI, name string) {
	t.Helper()
	m := fakeMeta(t, f, name)
	if m.Pending == nil {
		t.Fatal("public endpoint update lost its operation journal")
	}
	awaitFake(t, b, encodeOp(*m.Pending))
}

func TestPublicPortCutoverRestoresOriginalPolicy(t *testing.T) {
	for _, tc := range []struct {
		persistent bool
		replicas   int
	}{{false, 1}, {false, 2}, {false, 3}, {true, 1}} {
		persistent := tc.persistent
		t.Run(fmt.Sprintf("%s/%d", map[bool]string{false: "proxy", true: "direct-data"}[persistent], tc.replicas), func(t *testing.T) {
			f := newFakeAPI(t)
			b := fakeBackend(t, f, nil)
			volume := ""
			if persistent {
				volume = "data"
				importFake(t, b, volumeSpec(volume))
			}
			spec := runtimeSpec("runtime", volume)
			spec.MinContainers, spec.MaxContainers = tc.replicas, tc.replicas
			id := importFake(t, b, spec).Services["runtime"]
			deployFake(t, b, "runtime", volume)
			original := fakeAppSnapshot(t, f, "runtime")["updateConfigSwarm"]
			port := infra.PublicPort{Port: 15432, TargetPort: 5432, Protocol: "tcp", IPType: "IPV4"}
			for _, publish := range []bool{true, false} {
				var err error
				if publish {
					err = b.EnsurePublicPort(t.Context(), "runtime", port)
				} else {
					err = b.DeletePublicPorts(t.Context(), "runtime")
				}
				if err != nil {
					t.Fatal(err)
				}
				policy := fakeAppSnapshot(t, f, "runtime")["updateConfigSwarm"].(map[string]any)
				if policy["Order"] != "stop-first" || policy["Parallelism"] != float64(0) {
					t.Fatalf("endpoint cutover must stop all replicas in one batch: %#v", policy)
				}
				// Reconstruct after submission, before journal completion.
				b = fakeBackend(t, f, nil)
				finishPublicOperation(t, b, f, "runtime")
				if got := fakeAppSnapshot(t, f, "runtime")["updateConfigSwarm"]; !reflect.DeepEqual(got, original) {
					t.Fatalf("public=%v restored policy %#v, want original %#v", publish, got, original)
				}
				op, err := b.SetResources(t.Context(), "runtime", infra.Resources{MinRAM: 128 << 20})
				if err != nil {
					t.Fatal(err)
				}
				awaitFake(t, b, op)
				f.mu.Lock()
				orders := append([]string(nil), f.apps[id].reloadOrders...)
				f.mu.Unlock()
				want := original.(map[string]any)["Order"]
				if len(orders) == 0 || orders[len(orders)-1] != want {
					t.Fatalf("subsequent reload orders=%v, want original %v", orders, want)
				}
			}
		})
	}
}

func TestPublishedEndpointRepairIsBoundedAndProbeAcknowledged(t *testing.T) {
	for _, replicas := range []int{1, 2, 3} {
		t.Run(fmt.Sprintf("%d-replicas", replicas), func(t *testing.T) {
			testPublishedEndpointRepairBudget(t, replicas)
		})
	}
}

func testPublishedEndpointRepairBudget(t *testing.T, replicas int) {
	t.Helper()
	f := newFakeAPI(t)
	b := fakeBackend(t, f, nil)
	spec := runtimeSpec("proxy", "")
	spec.MinContainers, spec.MaxContainers = replicas, replicas
	importFake(t, b, spec)
	deployFake(t, b, "proxy", "")
	port := infra.PublicPort{Port: 15432, TargetPort: 5432, Protocol: "tcp", IPType: "IPV4"}
	if err := b.EnsurePublicPort(t.Context(), "proxy", port); err != nil {
		t.Fatal(err)
	}
	finishPublicOperation(t, b, f, "proxy")
	before := fakeCalls(f, "application.reload")
	if started, err := b.RepairPublishedEndpoint(t.Context(), "proxy", true); err != nil || started {
		t.Fatalf("healthy endpoint triggered repair: started=%v err=%v", started, err)
	}
	if err := b.EnsurePublicPort(t.Context(), "proxy", port); err != nil {
		t.Fatal(err)
	}
	if fakeCalls(f, "application.reload") != before {
		t.Fatal("matching public port is not idempotent")
	}
	original := fakeAppSnapshot(t, f, "proxy")["updateConfigSwarm"]
	now := time.Now().UTC()
	b.now = func() time.Time { return now }
	for attempt := 1; attempt <= endpointRepairLimit; attempt++ {
		started, err := b.RepairPublishedEndpoint(t.Context(), "proxy", false)
		if err != nil || !started {
			t.Fatalf("matching-port repair attempt %d: started=%v err=%v", attempt, started, err)
		}
		m := fakeMeta(t, f, "proxy")
		if m.EndpointRepairAttempts != attempt || m.Pending == nil || len(m.Pending.RestoreUpdateConfig) == 0 {
			t.Fatalf("repair not durably journaled: %+v", m)
		}
		policy := fakeAppSnapshot(t, f, "proxy")["updateConfigSwarm"].(map[string]any)
		if policy["Order"] != "stop-first" || policy["Parallelism"] != float64(0) {
			t.Fatalf("endpoint repair must stop all replicas in one batch: %#v", policy)
		}
		// A reconstructed backend completes the journal but cannot acknowledge
		// logical recovery from Swarm task convergence alone.
		b = fakeBackend(t, f, nil)
		b.now = func() time.Time { return now }
		finishPublicOperation(t, b, f, "proxy")
		if got := fakeMeta(t, f, "proxy").EndpointRepairAttempts; got != attempt {
			t.Fatalf("task convergence incorrectly acknowledged logical recovery: %d", got)
		}
		if got := fakeAppSnapshot(t, f, "proxy")["updateConfigSwarm"]; !reflect.DeepEqual(got, original) {
			t.Fatalf("repair changed original policy: %#v", got)
		}
		started, err = b.RepairPublishedEndpoint(t.Context(), "proxy", false)
		var exhausted *infra.EndpointRepairExhaustedError
		if started || (attempt < endpointRepairLimit && err != nil) || (attempt == endpointRepairLimit && !errors.As(err, &exhausted)) {
			t.Fatalf("repair cooldown/exhaustion: attempt=%d started=%v err=%v", attempt, started, err)
		}
		now = now.Add(endpointRepairCooldown + time.Second)
	}
	started, err := b.RepairPublishedEndpoint(t.Context(), "proxy", false)
	var exhausted *infra.EndpointRepairExhaustedError
	if started || !errors.As(err, &exhausted) || exhausted.Attempts != endpointRepairLimit {
		t.Fatalf("repair failed to report exhausted attempt limit: started=%v err=%v", started, err)
	}
	if fakeCalls(f, "application.reload") != before+endpointRepairLimit {
		t.Fatal("repair performed an unbounded or missing reload")
	}
	if _, err := b.RepairPublishedEndpoint(t.Context(), "proxy", true); err != nil {
		t.Fatal(err)
	}
	if m := fakeMeta(t, f, "proxy"); m.EndpointRepairAttempts != 0 || !m.EndpointRepairAfter.IsZero() {
		t.Fatalf("successful logical probe did not acknowledge recovery: %+v", m)
	}
}

func TestPublishedEndpointRepairHonorsOwnershipAndDeliveryFence(t *testing.T) {
	for _, guard := range []string{"ownership", "delivery"} {
		t.Run(guard, func(t *testing.T) {
			f := newFakeAPI(t)
			b := fakeBackend(t, f, nil)
			id := importFake(t, b, runtimeSpec("proxy", "")).Services["proxy"]
			deployFake(t, b, "proxy", "")
			if err := b.EnsurePublicPort(t.Context(), "proxy", infra.PublicPort{Port: 15432, TargetPort: 5432, Protocol: "tcp", IPType: "IPV4"}); err != nil {
				t.Fatal(err)
			}
			finishPublicOperation(t, b, f, "proxy")
			before := fakeCalls(f, "application.reload")
			f.mu.Lock()
			if guard == "ownership" {
				f.apps[id].row["name"] = "foreign"
			} else {
				m, _ := parseMetadata(fakeString(f.apps[id].row, "description"))
				m.MutationUntil = time.Now().Add(time.Hour)
				f.apps[id].row["description"] = m.description()
			}
			f.mu.Unlock()
			if started, err := b.RepairPublishedEndpoint(t.Context(), "proxy", false); started || !errors.Is(err, infra.ErrConflict) {
				t.Fatalf("repair bypassed %s: started=%v err=%v", guard, started, err)
			}
			if fakeCalls(f, "application.reload") != before {
				t.Fatal("guarded repair reloaded service")
			}
		})
	}
}

func TestPublishedEndpointRepairRetriesUndeliveredJournalAfterReconstruction(t *testing.T) {
	f := newFakeAPI(t)
	b := fakeBackend(t, f, nil)
	importFake(t, b, runtimeSpec("proxy", ""))
	deployFake(t, b, "proxy", "")
	if err := b.EnsurePublicPort(t.Context(), "proxy", infra.PublicPort{Port: 15432, TargetPort: 5432, Protocol: "tcp", IPType: "IPV4"}); err != nil {
		t.Fatal(err)
	}
	finishPublicOperation(t, b, f, "proxy")
	original := fakeAppSnapshot(t, f, "proxy")["updateConfigSwarm"]
	now := time.Now().UTC()
	b.now = func() time.Time { return now }
	f.mu.Lock()
	// Reject before apply: exactly the durable state left by a control-plane
	// crash after journal persistence but before application.reload delivery.
	f.failureOnce["application.reload"] = fakeResponse{status: 500, code: "INTERNAL_SERVER_ERROR", message: "reload not delivered"}
	f.mu.Unlock()
	if started, err := b.RepairPublishedEndpoint(t.Context(), "proxy", false); err == nil || started {
		t.Fatalf("undelivered repair: started=%v err=%v", started, err)
	}
	pending := fakeMeta(t, f, "proxy").Pending
	if pending == nil || pending.Action != "endpoint-repair" {
		t.Fatalf("undelivered repair lost its distinct journal: %+v", pending)
	}
	b = fakeBackend(t, f, nil)
	b.now = func() time.Time { return now }
	before := fakeCalls(f, "application.reload")
	if started, err := b.RepairPublishedEndpoint(t.Context(), "proxy", false); err != nil || started {
		t.Fatalf("in-progress repair was superseded: started=%v err=%v", started, err)
	}
	if fakeCalls(f, "application.reload") != before {
		t.Fatal("repair resubmitted before the startup deadline")
	}
	now = now.Add(taskStartupWindow + time.Second)
	// Sync verifies ownership via GetService before reaching the repair hook.
	// A terminal repair failure must not make that observation fail forever.
	service, err := b.GetService(t.Context(), "proxy")
	if err != nil || service == nil || service.Status != infra.StatusRunning {
		t.Fatalf("terminal repair blocked ordinary healthy-task observation: service=%+v err=%v", service, err)
	}
	if got := fakeAppSnapshot(t, f, "proxy")["updateConfigSwarm"]; !reflect.DeepEqual(got, original) {
		t.Fatalf("failed repair did not restore original policy: %#v", got)
	}
	if started, err := b.RepairPublishedEndpoint(t.Context(), "proxy", false); err != nil || !started {
		t.Fatalf("terminal undelivered repair did not consume next bounded attempt: started=%v err=%v", started, err)
	}
	if m := fakeMeta(t, f, "proxy"); m.EndpointRepairAttempts != 2 || m.Pending == nil || m.Pending.Failed {
		t.Fatalf("retry journal is not fresh and bounded: %+v", m)
	}
	finishPublicOperation(t, b, f, "proxy")
	if got := fakeMeta(t, f, "proxy").EndpointRepairAttempts; got != 2 {
		t.Fatalf("retry task convergence falsely acknowledged endpoint recovery: %d", got)
	}
}

func TestPublishedEndpointRepairNeverSupersedesUncertainDeployment(t *testing.T) {
	f := newFakeAPI(t)
	b := fakeBackend(t, f, nil)
	id := importFake(t, b, runtimeSpec("proxy", "")).Services["proxy"]
	deployFake(t, b, "proxy", "")
	if err := b.EnsurePublicPort(t.Context(), "proxy", infra.PublicPort{Port: 15432, TargetPort: 5432, Protocol: "tcp", IPType: "IPV4"}); err != nil {
		t.Fatal(err)
	}
	finishPublicOperation(t, b, f, "proxy")
	m := fakeMeta(t, f, "proxy")
	a, _, err := b.require(t.Context(), "proxy")
	if err != nil {
		t.Fatal(err)
	}
	o, err := b.operation(t.Context(), a, m, "deploy")
	if err != nil {
		t.Fatal(err)
	}
	o.Issued = time.Now().Add(-buildProgressWindow - time.Second)
	o.Failed, o.Uncertain = true, true
	m.Pending = &o
	f.mu.Lock()
	f.apps[id].row["description"] = m.description()
	f.mu.Unlock()
	before := fakeCalls(f, "application.reload")
	if started, err := b.RepairPublishedEndpoint(t.Context(), "proxy", false); err != nil || started {
		t.Fatalf("uncertain deployment superseded: started=%v err=%v", started, err)
	}
	if fakeCalls(f, "application.reload") != before || fakeMeta(t, f, "proxy").EndpointRepairAttempts != 0 {
		t.Fatal("uncertain deployment consumed a repair attempt")
	}
}

func TestPublicPortCutoverInterruptedRestoresExactPolicy(t *testing.T) {
	for _, replicas := range []int{1, 2, 3} {
		for _, action := range []string{"stop", "restart", "resources"} {
			t.Run(fmt.Sprintf("%d/%s", replicas, action), func(t *testing.T) {
				f := newFakeAPI(t)
				b := fakeBackend(t, f, nil)
				spec := runtimeSpec("proxy", "")
				spec.MinContainers, spec.MaxContainers = replicas, replicas
				id := importFake(t, b, spec).Services["proxy"]
				deployFake(t, b, "proxy", "")
				custom := map[string]any{"Parallelism": 2, "Order": "start-first", "FailureAction": "rollback", "Delay": 1234567}
				if err := b.c.post(t.Context(), "application.update", map[string]any{"applicationId": id, "updateConfigSwarm": custom}, nil); err != nil {
					t.Fatal(err)
				}
				original := fakeAppSnapshot(t, f, "proxy")["updateConfigSwarm"]
				if err := b.EnsurePublicPort(t.Context(), "proxy", infra.PublicPort{Port: 15432, TargetPort: 5432, Protocol: "tcp", IPType: "IPV4"}); err != nil {
					t.Fatal(err)
				}
				// Lose all in-memory backend state while the cutover journal is pending.
				b = fakeBackend(t, f, nil)
				var err error
				switch action {
				case "stop":
					_, err = b.Stop(t.Context(), "proxy")
				case "restart":
					_, err = b.Restart(t.Context(), "proxy")
				case "resources":
					_, err = b.SetResources(t.Context(), "proxy", infra.Resources{MinRAM: 128 << 20})
				}
				if err != nil {
					t.Fatal(err)
				}
				if got := fakeAppSnapshot(t, f, "proxy")["updateConfigSwarm"]; !reflect.DeepEqual(got, original) {
					t.Fatalf("superseding %s did not restore exact policy: got=%#v want=%#v", action, got, original)
				}
			})
		}
	}
}
