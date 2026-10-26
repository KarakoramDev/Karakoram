// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package dokploy

import (
	"errors"
	"net/http"
	"reflect"
	"strconv"
	"strings"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/infra"
)

func foreignPublishedService(port int, protocol, mode string) map[string]any {
	return map[string]any{"ID": "foreign0000000000000000001", "Spec": map[string]any{"Name": "outside-environment", "EndpointSpec": map[string]any{"Ports": []map[string]any{{"PublishedPort": port, "Protocol": protocol, "PublishMode": mode}}}}}
}

func assertPrivatePortUnchanged(t *testing.T, b *Backend, f *fakeAPI, before map[string]any, tasks []map[string]any) {
	t.Helper()
	row := fakeAppSnapshot(t, f, "runtime")
	for _, key := range []string{"endpointSpecSwarm", "updateConfigSwarm", "networkSwarm", "applicationStatus"} {
		if !reflect.DeepEqual(row[key], before[key]) {
			t.Fatalf("private %s changed: got=%v want=%v", key, row[key], before[key])
		}
	}
	m := fakeMeta(t, f, "runtime")
	if len(m.PublicPorts) != 0 || m.PortsPending || m.PortChange != nil || m.Pending != nil {
		t.Fatalf("public staging survived refusal: %+v", m)
	}
	f.mu.Lock()
	for _, app := range f.apps {
		if app.row["name"] == "runtime" {
			if len(app.ports) != 0 || len(app.appliedPorts) != 0 || !reflect.DeepEqual(app.tasks, tasks) {
				t.Errorf("private task/ports changed: rows=%v applied=%v tasks=%v", app.ports, app.appliedPorts, app.tasks)
			}
		}
	}
	f.mu.Unlock()
	service, err := b.GetService(t.Context(), "runtime")
	if err != nil || service == nil || service.Status != infra.StatusRunning {
		t.Fatalf("private service unhealthy: %+v %v", service, err)
	}
}

func privatePortFixture(t *testing.T) (*Backend, *fakeAPI, map[string]any, []map[string]any) {
	t.Helper()
	f := newFakeAPI(t)
	b := fakeBackend(t, f, nil)
	importFake(t, b, runtimeSpec("runtime", ""))
	deployFake(t, b, "runtime", "")
	before := fakeAppSnapshot(t, f, "runtime")
	f.mu.Lock()
	var tasks []map[string]any
	for _, app := range f.apps {
		if app.row["name"] == "runtime" {
			tasks = fakeRowsCopy(app.tasks)
		}
	}
	f.mu.Unlock()
	return b, f, before, tasks
}

func TestPublicPortConflictPreflightDoesNotMutate(t *testing.T) {
	for _, port := range []int{5432, 5671, 27017, 9200} {
		t.Run(strconv.Itoa(port), func(t *testing.T) {
			b, f, before, tasks := privatePortFixture(t)
			f.mu.Lock()
			f.services["outside-environment"] = foreignPublishedService(port, "tcp", "ingress")
			f.mu.Unlock()
			updates, reloads := fakeCalls(f, "application.update"), fakeCalls(f, "application.reload")
			err := b.EnsurePublicPort(t.Context(), "runtime", infra.PublicPort{Port: port, TargetPort: port, Protocol: "tcp", IPType: "IPV4"})
			var conflict *infra.PublicPortConflictError
			if !errors.As(err, &conflict) || conflict.Port != port || !errors.Is(err, infra.ErrRejected) || !strings.Contains(err.Error(), "keep this cluster private") {
				t.Fatalf("actionable conflict: %v", err)
			}
			if fakeCalls(f, "application.update") != updates || fakeCalls(f, "application.reload") != reloads || fakeCalls(f, "port.create") != 0 {
				t.Fatal("preflight conflict mutated application")
			}
			assertPrivatePortUnchanged(t, b, f, before, tasks)
		})
	}
}

func TestPublicPortConflictRollsBackRejectedPublication(t *testing.T) {
	for _, procedure := range []string{"port.create", "application.reload"} {
		t.Run(procedure, func(t *testing.T) {
			b, f, before, tasks := privatePortFixture(t)
			f.mu.Lock()
			response := fakeResponse{status: http.StatusBadRequest, code: "BAD_REQUEST", message: "port '5432' is already in use by service 'other' (foreign) as an ingress port"}
			if procedure == "application.reload" {
				response = fakeResponse{status: 500, code: "INTERNAL_SERVER_ERROR", message: "Error reloading application"}
			}
			f.failureOnce[procedure] = response
			f.failureEffect[procedure] = func() { f.services["outside-environment"] = foreignPublishedService(5432, "tcp", "ingress") }
			f.mu.Unlock()
			reloads := fakeCalls(f, "application.reload")
			err := b.EnsurePublicPort(t.Context(), "runtime", infra.PublicPort{Port: 5432, TargetPort: 5432, Protocol: "tcp", IPType: "IPV4"})
			var conflict *infra.PublicPortConflictError
			if !errors.As(err, &conflict) {
				t.Fatalf("conflict not classified: %v", err)
			}
			want := reloads
			if procedure == "application.reload" {
				want++
			}
			if fakeCalls(f, "application.reload") != want {
				t.Fatal("rollback reloaded the healthy private service")
			}
			assertPrivatePortUnchanged(t, b, f, before, tasks)
			// The real reload handler has set applicationStatus=error. Once
			// rolled back, a private correction and a later enable must work.
			b = fakeBackend(t, f, nil)
			if err := b.DeletePublicPorts(t.Context(), "runtime"); err != nil {
				t.Fatal(err)
			}
			assertPrivatePortUnchanged(t, b, f, before, tasks)
			f.mu.Lock()
			delete(f.services, "outside-environment")
			f.mu.Unlock()
			p := infra.PublicPort{Port: 5432, TargetPort: 5432, Protocol: "tcp", IPType: "IPV4"}
			if err := b.EnsurePublicPort(t.Context(), "runtime", p); err != nil {
				t.Fatalf("freed port could not be enabled after refusal: %v", err)
			}
			finishPublicOperation(t, b, f, "runtime")
			service, err := b.GetService(t.Context(), "runtime")
			if err != nil || service == nil || service.Status != infra.StatusRunning {
				t.Fatalf("reenabled service failed observation: %+v %v", service, err)
			}
		})
	}
}

func TestPublicPortRollbackResumesAfterControlPlaneReconstruction(t *testing.T) {
	b, f, before, tasks := privatePortFixture(t)
	f.mu.Lock()
	f.failureOnce["application.reload"] = fakeResponse{status: 500, message: "Error reloading application"}
	f.failureEffect["application.reload"] = func() {
		f.services["outside-environment"] = foreignPublishedService(5432, "tcp", "ingress")
		f.failureOnce["port.delete"] = fakeResponse{status: 503, message: "cleanup unavailable"}
	}
	f.mu.Unlock()
	p := infra.PublicPort{Port: 5432, TargetPort: 5432, Protocol: "tcp", IPType: "IPV4"}
	err := b.EnsurePublicPort(t.Context(), "runtime", p)
	var conflict *infra.PublicPortConflictError
	if err == nil || errors.As(err, &conflict) {
		t.Fatalf("failed rollback claimed terminal clean refusal: %v", err)
	}
	if m := fakeMeta(t, f, "runtime"); m.PortChange == nil || m.PortChange.RollbackPort != 5432 {
		t.Fatalf("rollback not durable: %+v", m)
	}
	b = fakeBackend(t, f, nil)
	reloads := fakeCalls(f, "application.reload")
	err = b.EnsurePublicPort(t.Context(), "runtime", p)
	if !errors.As(err, &conflict) {
		t.Fatalf("resumed cleanup: %v", err)
	}
	if fakeCalls(f, "application.reload") != reloads {
		t.Fatal("resumed rollback reloaded private service")
	}
	assertPrivatePortUnchanged(t, b, f, before, tasks)
}

func TestPublicPortConflictInventoryFailsClosed(t *testing.T) {
	b, f, before, tasks := privatePortFixture(t)
	f.mu.Lock()
	f.services["outside-environment"] = foreignPublishedService(5432, "tcp", "ingress")
	f.uninspectable["outside-environment"] = true
	f.mu.Unlock()
	updates := fakeCalls(f, "application.update")
	err := b.EnsurePublicPort(t.Context(), "runtime", infra.PublicPort{Port: 5432, TargetPort: 5432, Protocol: "tcp", IPType: "IPV4"})
	if !errors.Is(err, infra.ErrUnavailable) || fakeCalls(f, "application.update") != updates {
		t.Fatalf("incomplete inventory not fail-closed: %v", err)
	}
	assertPrivatePortUnchanged(t, b, f, before, tasks)
}

func TestPublicPortPreflightProtocolAndExistingOwner(t *testing.T) {
	for _, mode := range []string{"other-protocol", "host-mode", "existing-published"} {
		t.Run(mode, func(t *testing.T) {
			b, f, _, _ := privatePortFixture(t)
			p := infra.PublicPort{Port: 5432, TargetPort: 5432, Protocol: "tcp", IPType: "IPV4"}
			if mode == "existing-published" {
				if err := b.EnsurePublicPort(t.Context(), "runtime", p); err != nil {
					t.Fatal(err)
				}
				finishPublicOperation(t, b, f, "runtime")
				f.mu.Lock()
				f.services["outside-environment"] = foreignPublishedService(5432, "tcp", "ingress")
				f.mu.Unlock()
			} else {
				protocol, publishMode := "udp", "ingress"
				if mode == "host-mode" {
					protocol, publishMode = "tcp", "host"
				}
				f.mu.Lock()
				f.services["outside-environment"] = foreignPublishedService(5432, protocol, publishMode)
				f.mu.Unlock()
			}
			before := fakeCalls(f, "application.reload")
			if err := b.EnsurePublicPort(t.Context(), "runtime", p); err != nil {
				t.Fatal(err)
			}
			want := before + 1
			if mode == "existing-published" {
				want = before
			}
			if fakeCalls(f, "application.reload") != want {
				t.Fatal("existing publication changed or non-conflicting protocol blocked")
			}
		})
	}
}

func TestPublicPortUnrelatedReloadErrorIsNotClaimedAsConflict(t *testing.T) {
	b, f, _, _ := privatePortFixture(t)
	f.mu.Lock()
	f.failureOnce["application.reload"] = fakeResponse{status: 503, message: "backend unavailable"}
	f.mu.Unlock()
	err := b.EnsurePublicPort(t.Context(), "runtime", infra.PublicPort{Port: 5432, TargetPort: 5432, Protocol: "tcp", IPType: "IPV4"})
	var conflict *infra.PublicPortConflictError
	if !errors.Is(err, infra.ErrUnavailable) || errors.As(err, &conflict) {
		t.Fatalf("unrelated failure falsely claimed as a refused conflict: %v", err)
	}
	if m := fakeMeta(t, f, "runtime"); !m.PortsPending || m.PortChange == nil || m.PortChange.RollbackPort != 0 {
		t.Fatalf("uncertain delivery lost its retry state: %+v", m)
	}
}

func TestPublicPortHiddenOwnerReloadRefusalPreservesPrivate(t *testing.T) {
	b, f, before, tasks := privatePortFixture(t)
	f.mu.Lock()
	f.failureOnce["application.reload"] = fakeResponse{status: 500, code: "INTERNAL_SERVER_ERROR", message: "Error reloading application"}
	f.failureEffect["application.reload"] = func() {
		service := foreignPublishedService(5432, "tcp", "ingress")
		service["Spec"].(map[string]any)["Name"] = "dokploy-hidden"
		f.services["dokploy-hidden"] = service
	}
	f.mu.Unlock()
	reloads := fakeCalls(f, "application.reload")
	err := b.EnsurePublicPort(t.Context(), "runtime", infra.PublicPort{Port: 5432, TargetPort: 5432, Protocol: "tcp", IPType: "IPV4"})
	var failure *infra.PublicPortPublishError
	var conflict *infra.PublicPortConflictError
	if !errors.As(err, &failure) || errors.As(err, &conflict) || !strings.Contains(err.Error(), "check Swarm-wide port ownership") {
		t.Fatalf("hidden cause not reported truthfully: %v", err)
	}
	if fakeCalls(f, "application.reload") != reloads+1 {
		t.Fatal("hidden-owner rollback reloaded healthy private service")
	}
	assertPrivatePortUnchanged(t, b, f, before, tasks)
}

func TestPublicPortGenericReloadErrorAfterApplyRemainsUncertain(t *testing.T) {
	b, f, _, _ := privatePortFixture(t)
	f.mu.Lock()
	f.failureOnce["application.reload"] = fakeResponse{status: 500, message: "Error reloading application"}
	f.failureEffect["application.reload"] = func() {
		for _, app := range f.apps {
			if app.row["name"] == "runtime" {
				f.apply(app)
			}
		}
	}
	f.mu.Unlock()
	err := b.EnsurePublicPort(t.Context(), "runtime", infra.PublicPort{Port: 5432, TargetPort: 5432, Protocol: "tcp", IPType: "IPV4"})
	var failure *infra.PublicPortPublishError
	var conflict *infra.PublicPortConflictError
	if !errors.Is(err, infra.ErrUnavailable) || errors.As(err, &failure) || errors.As(err, &conflict) {
		t.Fatalf("applied service falsely claimed private/refused: %v", err)
	}
	if m := fakeMeta(t, f, "runtime"); !m.PortsPending || m.PortChange == nil || !m.PortChange.ReloadCompleted || m.PortChange.RollbackPort != 0 {
		t.Fatalf("post-apply failure lost retry state: %+v", m)
	}
	if status := fakeAppSnapshot(t, f, "runtime")["applicationStatus"]; status != "error" {
		t.Fatalf("fake lost real post-apply error status: %v", status)
	}
	b = fakeBackend(t, f, nil)
	reloads := fakeCalls(f, "application.reload")
	var service *infra.Service
	for range 10 {
		service, err = b.GetService(t.Context(), "runtime")
		if err == nil && service != nil && service.Status == infra.StatusRunning {
			break
		}
	}
	if err != nil || service == nil || service.Status != infra.StatusRunning {
		t.Fatalf("completed publication never recovered after reconstruction: %+v %v", service, err)
	}
	if fakeCalls(f, "application.reload") != reloads || fakeAppSnapshot(t, f, "runtime")["applicationStatus"] != "done" {
		t.Fatal("post-apply status repair reloaded or failed to repair status")
	}
	if m := fakeMeta(t, f, "runtime"); m.PortChange != nil || m.PortsPending {
		t.Fatalf("completed publication still fenced: %+v", m)
	}
	if err := b.DeletePublicPorts(t.Context(), "runtime"); err != nil {
		t.Fatalf("private correction remained blocked: %v", err)
	}
	finishPublicOperation(t, b, f, "runtime")
	service, err = b.GetService(t.Context(), "runtime")
	if err != nil || service == nil || service.Status != infra.StatusRunning {
		t.Fatalf("private correction did not recover healthy observation: %+v %v", service, err)
	}
	row := fakeAppSnapshot(t, f, "runtime")
	if endpoint, ok := row["endpointSpecSwarm"].(map[string]any); !ok || endpoint["Mode"] != "dnsrr" {
		t.Fatalf("private correction did not restore DNSRR: %v", row["endpointSpecSwarm"])
	}
}

func TestPublicPortUnknownReloadKeepsFenceUntilDelayedApply(t *testing.T) {
	for _, fault := range []string{"lost-response", "upstream-timeout"} {
		t.Run(fault, func(t *testing.T) {
			b, f, _, _ := privatePortFixture(t)
			p := infra.PublicPort{Port: 5432, TargetPort: 5432, Protocol: "tcp", IPType: "IPV4"}
			addOwner := func() {
				f.services["outside-environment"] = foreignPublishedService(5432, "tcp", "ingress")
			}
			if fault == "lost-response" {
				transport := b.c.http.Transport
				if transport == nil {
					transport = http.DefaultTransport
				}
				b.c.http.Transport = clientRoundTripFunc(func(r *http.Request) (*http.Response, error) {
					if r.URL.Path == "/api/application.reload" {
						f.mu.Lock()
						addOwner()
						f.mu.Unlock()
						return nil, errors.New("reload response lost while handler remains outstanding")
					}
					return transport.RoundTrip(r)
				})
			} else {
				f.mu.Lock()
				f.failureOnce["application.reload"] = fakeResponse{status: 408, message: "upstream request timed out"}
				f.failureEffect["application.reload"] = addOwner
				f.mu.Unlock()
			}
			err := b.EnsurePublicPort(t.Context(), "runtime", p)
			var conflict *infra.PublicPortConflictError
			var failure *infra.PublicPortPublishError
			if !errors.Is(err, infra.ErrUnavailable) || errors.As(err, &conflict) || errors.As(err, &failure) {
				t.Fatalf("unknown delivery became terminal conflict: %v", err)
			}
			original := fakeMeta(t, f, "runtime")
			if original.Pending == nil || original.PortChange == nil || !original.PortChange.ReloadPending || original.PortChange.RollbackPort != 0 {
				t.Fatalf("lost original delivery fence: %+v", original)
			}
			b = fakeBackend(t, f, nil)
			if err := b.EnsurePublicPort(t.Context(), "runtime", p); !errors.Is(err, infra.ErrUnavailable) || errors.As(err, &conflict) {
				t.Fatalf("next-pass owner observation retired outstanding reload: %v", err)
			}
			if err := b.DeletePublicPorts(t.Context(), "runtime"); !errors.Is(err, infra.ErrUnavailable) {
				t.Fatalf("private correction superseded an outstanding reload: %v", err)
			}
			current := fakeMeta(t, f, "runtime")
			if !sameOperation(current.Pending, *original.Pending) || current.PortChange == nil || !current.PortsPending {
				t.Fatalf("outstanding journal changed: %+v", current)
			}
			// The winner releases its port before the already-outstanding
			// reload reaches Docker. That captured publication now succeeds.
			f.mu.Lock()
			delete(f.services, "outside-environment")
			for _, app := range f.apps {
				if app.row["name"] == "runtime" {
					f.apply(app)
				}
			}
			f.mu.Unlock()
			finishPublicOperation(t, b, f, "runtime")
			reloads := fakeCalls(f, "application.reload")
			if err := b.EnsurePublicPort(t.Context(), "runtime", p); err != nil {
				t.Fatalf("observed delayed publication did not recover: %v", err)
			}
			if fakeCalls(f, "application.reload") != reloads {
				t.Fatal("delivery observation resubmitted a reload")
			}
			current = fakeMeta(t, f, "runtime")
			if current.PortChange != nil || current.PortsPending || len(current.PublicPorts) != 1 {
				t.Fatalf("delayed publication acknowledged as private: %+v", current)
			}
		})
	}
}

func TestInterruptedSettingsOnlyPublicPortDoesNotFenceStart(t *testing.T) {
	for _, state := range []string{"stopped", "undeployed"} {
		t.Run(state, func(t *testing.T) {
			f := newFakeAPI(t)
			b := fakeBackend(t, f, nil)
			importFake(t, b, runtimeSpec("runtime", ""))
			if state == "stopped" {
				deployFake(t, b, "runtime", "")
				op, err := b.Stop(t.Context(), "runtime")
				if err != nil {
					t.Fatal(err)
				}
				awaitFake(t, b, op)
			}
			updates := 0
			loseCommittedReply(b, func(r *http.Request) bool {
				if r.URL.Path != "/api/application.update" {
					return false
				}
				updates++
				return updates == 2
			})
			p := infra.PublicPort{Port: 5432, TargetPort: 5432, Protocol: "tcp", IPType: "IPV4"}
			reloads := fakeCalls(f, "application.reload")
			if err := b.EnsurePublicPort(t.Context(), "runtime", p); err == nil {
				t.Fatal("settings-only committed-response loss not exercised")
			}
			m := fakeMeta(t, f, "runtime")
			if m.PortChange == nil || !m.PortsPending || m.PortChange.ReloadPending || m.Pending != nil {
				t.Fatalf("settings-only publication acquired a reload fence: %+v", m)
			}
			b = fakeBackend(t, f, nil)
			if err := b.EnsurePublicPort(t.Context(), "runtime", p); err != nil {
				t.Fatalf("settings-only completion required an applied service: %v", err)
			}
			if fakeCalls(f, "application.reload") != reloads {
				t.Fatal("settings-only reconciliation started a service")
			}
			if m := fakeMeta(t, f, "runtime"); m.PortChange != nil || m.PortsPending {
				t.Fatalf("settings-only inverse not retired: %+v", m)
			}
			if state == "undeployed" {
				// Start's existing not-deployed contract remains truthful; no
				// publication-delivery fence may replace it.
				if _, err := b.Start(t.Context(), "runtime"); !errors.Is(err, infra.ErrRejected) {
					t.Fatalf("undeployed Start changed contract: %v", err)
				}
				deployFake(t, b, "runtime", "")
			}
			op, err := b.Start(t.Context(), "runtime")
			if err != nil {
				t.Fatalf("Start blocked by settings-only inverse: %v", err)
			}
			awaitFake(t, b, op)
			service, err := b.GetService(t.Context(), "runtime")
			if err != nil || service == nil || service.Status != infra.StatusRunning {
				t.Fatalf("Start did not produce healthy observation: %+v %v", service, err)
			}
		})
	}
}

func TestCompletedPortRefusalResumesBeforeRollbackDecision(t *testing.T) {
	for _, fault := range []string{"receipt-write-response-lost", "inspection-unavailable"} {
		for _, hidden := range []bool{false, true} {
			name := fault + "/visible-owner"
			if hidden {
				name = fault + "/hidden-owner"
			}
			t.Run(name, func(t *testing.T) {
				b, f, before, tasks := privatePortFixture(t)
				owner := "outside-environment"
				if hidden {
					owner = "dokploy-hidden"
				}
				// Called only while holding f.mu.
				receiptPending := func() bool {
					for _, app := range f.apps {
						if app.row["name"] == "runtime" {
							m, ok := parseMetadata(fakeString(app.row, "description"))
							return ok && m.PortChange != nil && m.PortChange.ReloadCompleted && m.PortChange.RollbackPort == 0
						}
					}
					return false
				}
				f.mu.Lock()
				f.failureOnce["application.reload"] = fakeResponse{status: 500, message: "Error reloading application"}
				f.failureEffect["application.reload"] = func() {
					service := foreignPublishedService(5432, "tcp", "ingress")
					service["Spec"].(map[string]any)["Name"] = owner
					f.services[owner] = service
				}
				if fault == "inspection-unavailable" {
					f.requestHook = func(w http.ResponseWriter, r *http.Request) (bool, func()) {
						if r.URL.Path != "/api/docker.getConfig" {
							return false, nil
						}
						f.mu.Lock()
						blocked := receiptPending()
						f.mu.Unlock()
						if !blocked {
							return false, nil
						}
						fakeError(w, 503, "SERVICE_UNAVAILABLE", "inspection temporarily unavailable", "docker.getConfig")
						return true, nil
					}
				}
				f.mu.Unlock()
				if fault == "receipt-write-response-lost" {
					loseCommittedReply(b, func(r *http.Request) bool {
						if r.URL.Path != "/api/application.update" {
							return false
						}
						f.mu.Lock()
						defer f.mu.Unlock()
						return receiptPending()
					})
				}
				p := infra.PublicPort{Port: 5432, TargetPort: 5432, Protocol: "tcp", IPType: "IPV4"}
				err := b.EnsurePublicPort(t.Context(), "runtime", p)
				var conflict *infra.PublicPortConflictError
				var failure *infra.PublicPortPublishError
				if !errors.Is(err, infra.ErrUnavailable) || errors.As(err, &conflict) || errors.As(err, &failure) {
					t.Fatalf("gap before refusal classification not exercised: %v", err)
				}
				m := fakeMeta(t, f, "runtime")
				if m.PortChange == nil || !m.PortChange.ReloadCompleted || !m.PortChange.ReloadPending || m.PortChange.RollbackPort != 0 || m.Pending == nil {
					t.Fatalf("completed-response recovery state not retained: %+v", m)
				}
				b = fakeBackend(t, f, nil)
				reloads := fakeCalls(f, "application.reload")
				if fault == "inspection-unavailable" {
					if _, err := b.GetService(t.Context(), "runtime"); !errors.Is(err, infra.ErrUnavailable) {
						t.Fatalf("transient inspection did not remain safely pending: %v", err)
					}
					f.mu.Lock()
					f.requestHook = nil
					f.mu.Unlock()
				}
				_, err = b.GetService(t.Context(), "runtime")
				if hidden {
					if !errors.As(err, &failure) || errors.As(err, &conflict) {
						t.Fatalf("hidden refusal not resumed truthfully: %v", err)
					}
				} else if !errors.As(err, &conflict) {
					t.Fatalf("visible conflict not resumed: %v", err)
				}
				if fakeCalls(f, "application.reload") != reloads {
					t.Fatal("completed refusal recovery reloaded private tasks")
				}
				assertPrivatePortUnchanged(t, b, f, before, tasks)
				if err := b.DeletePublicPorts(t.Context(), "runtime"); err != nil {
					t.Fatalf("private correction remained fenced: %v", err)
				}
				f.mu.Lock()
				delete(f.services, owner)
				f.mu.Unlock()
				if err := b.EnsurePublicPort(t.Context(), "runtime", p); err != nil {
					t.Fatalf("re-enable after recovered refusal failed: %v", err)
				}
				finishPublicOperation(t, b, f, "runtime")
			})
		}
	}
}
