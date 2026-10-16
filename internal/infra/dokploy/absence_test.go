// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package dokploy

import (
	"errors"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/infra"
)

// These literals intentionally predate any cleanup-specific handle additions.
const legacyDeleteHandle = "dokploy:v1:eyJhY3Rpb24iOiJkZWxldGUiLCJhcHBJZCI6ImxlZ2FjeS1hcHBsaWNhdGlvbiIsImFwcE5hbWUiOiJsZWdhY3ktcnVudGltZS1hYmMxMjMiLCJob3N0bmFtZSI6InJ1bnRpbWUiLCJpc3N1ZWQiOiIyMDI2LTEwLTE2VDEyOjAwOjAwWiJ9"
const legacyWipeHandle = "dokploy:v1:eyJhY3Rpb24iOiJ3aXBlIiwiYXBwSWQiOiJsZWdhY3ktYXBwbGljYXRpb24iLCJhcHBOYW1lIjoibGVnYWN5LXJ1bnRpbWUtYWJjMTIzIiwiaG9zdG5hbWUiOiJydW50aW1lIiwiaXNzdWVkIjoiMjAyNi0xMC0xNlQxMjowMDowMFoifQ"

var absenceFailures = []struct {
	name     string
	response fakeResponse
	want     error
}{
	{"unauthorized", fakeResponse{401, "UNAUTHORIZED", "access denied"}, infra.ErrUnauthorized},
	{"forbidden", fakeResponse{403, "FORBIDDEN", "access denied"}, infra.ErrUnauthorized},
	{"unavailable", fakeResponse{500, "INTERNAL_SERVER_ERROR", "platform unavailable"}, infra.ErrUnavailable},
}

func TestAbsenceRepeatedCleanupAndObservation(t *testing.T) {
	f := newFakeAPI(t)
	b := fakeBackend(t, f, nil)
	importFake(t, b, runtimeSpec("runtime", ""))
	deployFake(t, b, "runtime", "")
	port := infra.PublicPort{Port: 18080, TargetPort: 8080, Protocol: "tcp", IPType: "IPV4"}
	if err := b.EnsurePublicPort(t.Context(), "runtime", port); err != nil {
		t.Fatal(err)
	}
	op, err := b.SetSubdomainAccess(t.Context(), "runtime", true)
	if err != nil {
		t.Fatal(err)
	}
	awaitFake(t, b, op)
	op, err = b.DeleteService(t.Context(), "runtime")
	if err != nil {
		t.Fatal(err)
	}
	awaitFake(t, b, op)
	for range 3 {
		s, err := b.GetService(t.Context(), "runtime")
		if err != nil || s != nil {
			t.Fatalf("deleted GetService: %+v %v", s, err)
		}
		if err = b.DeletePublicPorts(t.Context(), "runtime"); err != nil {
			t.Fatalf("deleted public-port cleanup: %v", err)
		}
		op, err = b.SetSubdomainAccess(t.Context(), "runtime", false)
		if err != nil || op.ID != "" {
			t.Fatalf("deleted HTTPS cleanup: %+v %v", op, err)
		}
		containers, err := b.Containers(t.Context(), "runtime")
		if err != nil || len(containers) != 0 {
			t.Fatalf("deleted Containers: %+v %v", containers, err)
		}
		urls, err := b.SubdomainURLs(t.Context(), "runtime")
		if err != nil || len(urls) != 0 {
			t.Fatalf("deleted SubdomainURLs: %v %v", urls, err)
		}
	}
	if _, err = b.SetSubdomainAccess(t.Context(), "runtime", true); !errors.Is(err, infra.ErrNotFound) {
		t.Fatalf("enabling absent HTTPS must stay strict: %v", err)
	}
	if _, err = b.Start(t.Context(), "runtime"); !errors.Is(err, infra.ErrNotFound) {
		t.Fatalf("starting absent runtime must stay strict: %v", err)
	}
	if _, err = b.Deploy(t.Context(), "runtime", testDeployment(t, "")); !errors.Is(err, infra.ErrNotFound) {
		t.Fatalf("deploying absent runtime must stay strict: %v", err)
	}
}

func TestAbsenceLiteralLegacyCleanupHandles(t *testing.T) {
	for _, id := range []string{legacyDeleteHandle, legacyWipeHandle} {
		t.Run(id[:24], func(t *testing.T) {
			f := newFakeAPI(t)
			b := fakeBackend(t, f, nil)
			f.failure["docker.getServiceContainersByAppName"] = fakeResponse{404, "NOT_FOUND", "service not found"}
			op := roundTripOp(t, infra.Op{ID: id})
			for range 2 {
				state, err := b.OpStatus(t.Context(), op)
				if err != nil || !state.Done || state.Failed || state.Expired {
					t.Fatalf("legacy cleanup with missing application and tasks: %+v %v", state, err)
				}
			}
			if fakeCalls(f, "application.one") == 0 || fakeCalls(f, "docker.getServiceContainersByAppName") == 0 {
				t.Fatal("legacy operation did not check missing application/tasks")
			}
		})
	}
}

func TestAbsenceCleanupRacesApplicationDisappearance(t *testing.T) {
	for _, procedure := range []string{"application.stop", "application.delete"} {
		t.Run(procedure, func(t *testing.T) {
			f := newFakeAPI(t)
			b := fakeBackend(t, f, nil)
			imported := importFake(t, b, runtimeSpec("runtime", ""))
			deployFake(t, b, "runtime", "")
			id := imported.Services["runtime"]
			f.mu.Lock()
			f.failureOnce[procedure] = fakeResponse{404, "NOT_FOUND", "application already deleted"}
			f.failureEffect[procedure] = func() { delete(f.apps, id) }
			f.mu.Unlock()
			op, err := b.DeleteService(t.Context(), "runtime")
			if err != nil {
				t.Fatalf("cleanup %s404 must succeed: %v", procedure, err)
			}
			awaitFake(t, b, op)
			if fakeCalls(f, procedure) == 0 {
				t.Fatalf("race did not reach %s", procedure)
			}
			if s, err := b.GetService(t.Context(), "runtime"); err != nil || s != nil {
				t.Fatalf("concurrently deleted application retained: %+v %v", s, err)
			}
			if _, err = b.DeleteService(t.Context(), "runtime"); err != nil {
				t.Fatalf("repeated cleanup after race: %v", err)
			}
		})
	}
}

func TestAbsencePortAndDomainDeleteRaces(t *testing.T) {
	for _, procedure := range []string{"port.delete", "domain.delete"} {
		t.Run(procedure, func(t *testing.T) {
			f := newFakeAPI(t)
			b := fakeBackend(t, f, nil)
			imported := importFake(t, b, runtimeSpec("runtime", ""))
			deployFake(t, b, "runtime", "")
			if procedure == "port.delete" {
				if err := b.EnsurePublicPort(t.Context(), "runtime", infra.PublicPort{Port: 18080, TargetPort: 8080, Protocol: "tcp", IPType: "IPV4"}); err != nil {
					t.Fatal(err)
				}
			} else {
				op, err := b.SetSubdomainAccess(t.Context(), "runtime", true)
				if err != nil {
					t.Fatal(err)
				}
				awaitFake(t, b, op)
			}
			id := imported.Services["runtime"]
			f.mu.Lock()
			f.failureOnce[procedure] = fakeResponse{404, "NOT_FOUND", "routing row already removed"}
			f.failureEffect[procedure] = func() {
				if procedure == "port.delete" {
					f.apps[id].ports = nil
				} else {
					f.apps[id].domains = nil
				}
			}
			f.mu.Unlock()
			for range 2 {
				if procedure == "port.delete" {
					if err := b.DeletePublicPorts(t.Context(), "runtime"); err != nil {
						t.Fatalf("already-deleted port: %v", err)
					}
				} else {
					op, err := b.SetSubdomainAccess(t.Context(), "runtime", false)
					if err != nil {
						t.Fatalf("already-deleted domain: %v", err)
					}
					awaitFake(t, b, op)
				}
			}
			if fakeCalls(f, procedure) != 1 {
				t.Fatalf("routing deletion should race once, got %d", fakeCalls(f, procedure))
			}
		})
	}
}

func TestAbsenceManagerVolumeRemovalRace(t *testing.T) {
	f := newFakeAPI(t)
	b := fakeBackend(t, f, nil)
	importFake(t, b, volumeSpec("volume"))
	volume := fakeString(fakeAppSnapshot(t, f, "volume"), "appName")
	op, err := b.DeleteService(t.Context(), "volume")
	if err != nil {
		t.Fatal(err)
	}
	f.mu.Lock()
	f.failureOnce["dockerVolume.removeVolume"] = fakeResponse{404, "NOT_FOUND", "volume already removed"}
	f.failureEffect["dockerVolume.removeVolume"] = func() { delete(f.volumes, volume) }
	f.mu.Unlock()
	awaitFake(t, b, op)
	if fakeCalls(f, "dockerVolume.removeVolume") != 1 {
		t.Fatal("manager removal race not exercised")
	}
}

func TestAbsenceLookupsDoNotHideAccessOrAvailabilityErrors(t *testing.T) {
	for _, failure := range absenceFailures {
		t.Run(failure.name, func(t *testing.T) {
			f := newFakeAPI(t)
			b := fakeBackend(t, f, nil)
			f.failure["environment.one"] = failure.response
			checks := []struct {
				name string
				call func() error
			}{
				{"GetService", func() error { _, err := b.GetService(t.Context(), "runtime"); return err }},
				{"DeletePublicPorts", func() error { return b.DeletePublicPorts(t.Context(), "runtime") }},
				{"DisableSubdomain", func() error { _, err := b.SetSubdomainAccess(t.Context(), "runtime", false); return err }},
				{"Containers", func() error { _, err := b.Containers(t.Context(), "runtime"); return err }},
				{"SubdomainURLs", func() error { _, err := b.SubdomainURLs(t.Context(), "runtime"); return err }},
			}
			for _, check := range checks {
				t.Run(check.name, func(t *testing.T) {
					if err := check.call(); !errors.Is(err, failure.want) {
						t.Fatalf("lookup error was swallowed/misclassified: %v, want %v", err, failure.want)
					}
				})
			}
		})
	}
}

func TestAbsenceApplicationCleanupDoesNotHideRealErrors(t *testing.T) {
	for _, procedure := range []string{"application.stop", "application.delete"} {
		t.Run(procedure, func(t *testing.T) {
			for _, failure := range absenceFailures {
				t.Run(failure.name, func(t *testing.T) {
					f := newFakeAPI(t)
					b := fakeBackend(t, f, nil)
					importFake(t, b, runtimeSpec("runtime", ""))
					deployFake(t, b, "runtime", "")
					f.mu.Lock()
					f.failure[procedure] = failure.response
					f.mu.Unlock()
					op, err := b.DeleteService(t.Context(), "runtime")
					if procedure == "application.delete" && err == nil {
						for range 10 {
							_, err = b.OpStatus(t.Context(), op)
							if err != nil {
								break
							}
						}
					}
					if !errors.Is(err, failure.want) {
						t.Fatalf("cleanup error was swallowed/misclassified: %v, want %v", err, failure.want)
					}
					s, lookupErr := b.GetService(t.Context(), "runtime")
					if lookupErr != nil || s == nil {
						t.Fatalf("failed cleanup unexpectedly removed application: %+v %v", s, lookupErr)
					}
				})
			}
		})
	}
}

func TestAbsenceRoutingCleanupDoesNotHideRealErrors(t *testing.T) {
	for _, procedure := range []string{"port.delete", "domain.delete"} {
		t.Run(procedure, func(t *testing.T) {
			for _, failure := range absenceFailures {
				t.Run(failure.name, func(t *testing.T) {
					f := newFakeAPI(t)
					b := fakeBackend(t, f, nil)
					importFake(t, b, runtimeSpec("runtime", ""))
					deployFake(t, b, "runtime", "")
					if procedure == "port.delete" {
						if err := b.EnsurePublicPort(t.Context(), "runtime", infra.PublicPort{Port: 18080, TargetPort: 8080, Protocol: "tcp", IPType: "IPV4"}); err != nil {
							t.Fatal(err)
						}
					} else {
						op, err := b.SetSubdomainAccess(t.Context(), "runtime", true)
						if err != nil {
							t.Fatal(err)
						}
						awaitFake(t, b, op)
					}
					f.mu.Lock()
					f.failure[procedure] = failure.response
					f.mu.Unlock()
					var err error
					if procedure == "port.delete" {
						err = b.DeletePublicPorts(t.Context(), "runtime")
					} else {
						_, err = b.SetSubdomainAccess(t.Context(), "runtime", false)
					}
					if !errors.Is(err, failure.want) {
						t.Fatalf("routing cleanup error was swallowed/misclassified: %v, want %v", err, failure.want)
					}
				})
			}
		})
	}
}

func TestAbsenceLegacyTaskLookupDoesNotHideRealErrors(t *testing.T) {
	for _, failure := range absenceFailures {
		t.Run(failure.name, func(t *testing.T) {
			f := newFakeAPI(t)
			b := fakeBackend(t, f, nil)
			f.failure["docker.getServiceContainersByAppName"] = failure.response
			for _, id := range []string{legacyDeleteHandle, legacyWipeHandle} {
				state, err := b.OpStatus(t.Context(), infra.Op{ID: id})
				if !errors.Is(err, failure.want) || state.Done {
					t.Fatalf("legacy task error was swallowed/misclassified: %+v %v", state, err)
				}
			}
		})
	}
}

func TestAbsenceManagerVolumeRemovalDoesNotHideAccessErrors(t *testing.T) {
	for _, failure := range absenceFailures[:2] {
		t.Run(failure.name, func(t *testing.T) {
			f := newFakeAPI(t)
			b := fakeBackend(t, f, nil)
			importFake(t, b, volumeSpec("volume"))
			op, err := b.DeleteService(t.Context(), "volume")
			if err != nil {
				t.Fatal(err)
			}
			f.mu.Lock()
			f.failure["dockerVolume.removeVolume"] = failure.response
			f.mu.Unlock()
			var state infra.OpState
			for range 10 {
				state, err = b.OpStatus(t.Context(), op)
				if err != nil {
					break
				}
			}
			if !errors.Is(err, failure.want) || state.Done {
				t.Fatalf("manager volume access error was swallowed/misclassified: %+v %v", state, err)
			}
		})
	}
}
