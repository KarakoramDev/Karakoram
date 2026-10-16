// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package dokploy

import (
	"errors"
	"reflect"
	"sort"
	"strings"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/infra"
)

func domainTestDeploy(t *testing.T, b *Backend, hostname string, ports ...infra.Port) {
	t.Helper()
	d := testDeployment(t, "")
	d.Runtime.Ports = ports
	op, err := b.Deploy(t.Context(), hostname, d)
	if err != nil {
		t.Fatal(err)
	}
	awaitFake(t, b, op)
}

func domainTestCreate(t *testing.T, b *Backend, appID, host string, port int) {
	t.Helper()
	if err := b.c.post(t.Context(), "domain.create", map[string]any{"applicationId": appID, "host": host, "port": port, "https": true, "domainType": "application", "certificateType": "none", "path": "/", "stripPath": false}, nil); err != nil {
		t.Fatal(err)
	}
}

func domainTestHosts(t *testing.T, f *fakeAPI, appID string) []string {
	t.Helper()
	f.mu.Lock()
	defer f.mu.Unlock()
	a := f.apps[appID]
	if a == nil {
		t.Fatal("domain test application missing")
	}
	hosts := make([]string, 0, len(a.domains))
	for _, d := range a.domains {
		hosts = append(hosts, fakeString(d, "host"))
	}
	sort.Strings(hosts)
	return hosts
}

func domainTestAssertHosts(t *testing.T, f *fakeAPI, appID string, want ...string) {
	t.Helper()
	if want == nil {
		want = []string{}
	}
	sort.Strings(want)
	if got := domainTestHosts(t, f, appID); !reflect.DeepEqual(got, want) {
		t.Fatalf("domain hosts=%v, want %v", got, want)
	}
}

func domainTestEnable(t *testing.T, b *Backend, hostname string, enabled bool) {
	t.Helper()
	op, err := b.SetSubdomainAccess(t.Context(), hostname, enabled)
	if err != nil {
		t.Fatal(err)
	}
	awaitFake(t, b, op)
}

func TestDomainsHTTPPortBecomingNonHTTPRemovesRoutesBeforeDisable(t *testing.T) {
	f := newFakeAPI(t)
	b := fakeBackend(t, f, nil)
	imported := importFake(t, b, runtimeSpec("runtime", ""))
	id := imported.Services["runtime"]
	domainTestDeploy(t, b, "runtime", infra.Port{Port: 8080, Protocol: "tcp", HTTP: true})
	domainTestEnable(t, b, "runtime", true)
	owned := b.domainHost("runtime", 8080)
	unrelated := "operator-owned.apps.example.test"
	domainTestCreate(t, b, id, unrelated, 8080)
	domainTestAssertHosts(t, f, id, owned, unrelated)
	// Reuse the same numeric port, but no longer advertise it as HTTP.
	domainTestDeploy(t, b, "runtime", infra.Port{Port: 8080, Protocol: "tcp"})
	domainTestAssertHosts(t, f, id, unrelated)
	assertIsolatedNetwork(t, b, appliedNetworks(t, f, "runtime"))
	urls, err := b.SubdomainURLs(t.Context(), "runtime")
	if err != nil || len(urls) != 0 {
		t.Fatalf("non-HTTP deployment retained subdomain URLs: %v %v", urls, err)
	}
	for range 2 {
		domainTestEnable(t, b, "runtime", false)
	}
	domainTestAssertHosts(t, f, id, unrelated)
	assertIsolatedNetwork(t, b, appliedNetworks(t, f, "runtime"))
	if fakeCalls(f, "domain.delete") != 1 {
		t.Fatal("disable recreated/deleted unrelated or already reconciled routes")
	}
}

func TestDomainsRedeployReconcilesHTTPPortAndPreservesUnrelated(t *testing.T) {
	for _, hostname := range []string{"runtime", strings.Repeat("a", 62) + "b"} {
		t.Run(hostname, func(t *testing.T) {
			f := newFakeAPI(t)
			b := fakeBackend(t, f, nil)
			imported := importFake(t, b, runtimeSpec(hostname, ""))
			id := imported.Services[hostname]
			domainTestDeploy(t, b, hostname, infra.Port{Port: 8080, Protocol: "tcp", HTTP: true})
			domainTestEnable(t, b, hostname, true)
			oldHost, newHost := b.domainHost(hostname, 8080), b.domainHost(hostname, 8081)
			unrelated := "operator-owned.apps.example.test"
			domainTestCreate(t, b, id, unrelated, 8443)
			// This is in the same app and suffix but not this environment's managed
			// namespace. It must not be treated as a stale Karakoram route.
			foreign := "runtime-8080-otherenv.apps.example.test"
			domainTestCreate(t, b, id, foreign, 8080)
			domainTestDeploy(t, b, hostname, infra.Port{Port: 8081, Protocol: "tcp", HTTP: true})
			domainTestAssertHosts(t, f, id, newHost, unrelated, foreign)
			urls, err := b.SubdomainURLs(t.Context(), hostname)
			if err != nil || !reflect.DeepEqual(urls, []string{"https://" + newHost}) {
				t.Fatalf("URLs do not match current desired HTTP ports: %v %v", urls, err)
			}
			if oldHost == newHost {
				t.Fatal("distinct HTTP ports produced colliding domain names")
			}
			networks := appliedNetworks(t, f, hostname)
			if len(networks) != 2 || networks[1]["Target"] != "dokploy-network" {
				t.Fatalf("desired HTTPS route lost shared network: %+v", networks)
			}
			creates, deletes, reloads := fakeCalls(f, "domain.create"), fakeCalls(f, "domain.delete"), fakeCalls(f, "application.reload")
			domainTestEnable(t, b, hostname, true)
			if fakeCalls(f, "domain.create") != creates || fakeCalls(f, "domain.delete") != deletes || fakeCalls(f, "application.reload") != reloads {
				t.Fatal("already reconciled enable was not a no-op")
			}
			domainTestDeploy(t, b, hostname, infra.Port{Port: 8081, Protocol: "tcp", HTTP: true})
			if fakeCalls(f, "domain.create") != creates || fakeCalls(f, "domain.delete") != deletes {
				t.Fatal("unchanged HTTP redeploy rewrote domain rows")
			}
			// Inject a former-port route after reconciliation. URLs are the
			// intersection of desired and existing, not every app-owned domain.
			domainTestCreate(t, b, id, oldHost, 8080)
			urls, err = b.SubdomainURLs(t.Context(), hostname)
			if err != nil || !reflect.DeepEqual(urls, []string{"https://" + newHost}) {
				t.Fatalf("stale route leaked into URLs: %v %v", urls, err)
			}
			domainTestEnable(t, b, hostname, false)
			domainTestAssertHosts(t, f, id, unrelated, foreign)
			assertIsolatedNetwork(t, b, appliedNetworks(t, f, hostname))
		})
	}
}

func TestDomainsDisabledURLsHideAndCleanupManagedDrift(t *testing.T) {
	f := newFakeAPI(t)
	b := fakeBackend(t, f, nil)
	imported := importFake(t, b, runtimeSpec("runtime", ""))
	id := imported.Services["runtime"]
	domainTestDeploy(t, b, "runtime", infra.Port{Port: 8080, Protocol: "tcp", HTTP: true})
	current, former := b.domainHost("runtime", 8080), b.domainHost("runtime", 8443)
	unrelated := "operator-owned.apps.example.test"
	domainTestCreate(t, b, id, current, 8080)
	domainTestCreate(t, b, id, former, 8443)
	domainTestCreate(t, b, id, unrelated, 8443)
	urls, err := b.SubdomainURLs(t.Context(), "runtime")
	if err != nil || len(urls) != 0 {
		t.Fatalf("disabled metadata must hide drifted domains: %v %v", urls, err)
	}
	domainTestEnable(t, b, "runtime", false)
	domainTestAssertHosts(t, f, id, unrelated)
	assertIsolatedNetwork(t, b, appliedNetworks(t, f, "runtime"))
	deletes := fakeCalls(f, "domain.delete")
	domainTestEnable(t, b, "runtime", false)
	if fakeCalls(f, "domain.delete") != deletes {
		t.Fatal("idempotent disabled reconciliation repeated deletions")
	}
}

func TestDomainsRedeployToleratesConcurrentStaleDomainRemoval(t *testing.T) {
	f := newFakeAPI(t)
	b := fakeBackend(t, f, nil)
	imported := importFake(t, b, runtimeSpec("runtime", ""))
	id := imported.Services["runtime"]
	domainTestDeploy(t, b, "runtime", infra.Port{Port: 8080, Protocol: "tcp", HTTP: true})
	domainTestEnable(t, b, "runtime", true)
	oldHost := b.domainHost("runtime", 8080)
	f.mu.Lock()
	f.failureOnce["domain.delete"] = fakeResponse{404, "NOT_FOUND", "domain already removed"}
	f.failureEffect["domain.delete"] = func() {
		a := f.apps[id]
		kept := a.domains[:0]
		for _, d := range a.domains {
			if d["host"] != oldHost {
				kept = append(kept, d)
			}
		}
		a.domains = kept
	}
	f.mu.Unlock()
	domainTestDeploy(t, b, "runtime", infra.Port{Port: 8081, Protocol: "tcp", HTTP: true})
	domainTestAssertHosts(t, f, id, b.domainHost("runtime", 8081))
	if fakeCalls(f, "domain.delete") != 1 {
		t.Fatal("stale domain deletion race was not exercised once")
	}
}

func TestDomainsReconciliationPreservesUpstreamErrors(t *testing.T) {
	for _, entrypoint := range []string{"disable", "redeploy"} {
		t.Run(entrypoint, func(t *testing.T) {
			for _, procedure := range []string{"domain.delete", "domain.create"} {
				if entrypoint == "disable" && procedure == "domain.create" {
					continue
				}
				t.Run(procedure, func(t *testing.T) {
					for _, failure := range []struct {
						name     string
						response fakeResponse
						want     error
					}{
						{"unauthorized", fakeResponse{401, "UNAUTHORIZED", "domain access denied"}, infra.ErrUnauthorized},
						{"unavailable", fakeResponse{500, "INTERNAL_SERVER_ERROR", "domain service unavailable"}, infra.ErrUnavailable},
					} {
						t.Run(failure.name, func(t *testing.T) {
							f := newFakeAPI(t)
							b := fakeBackend(t, f, nil)
							importFake(t, b, runtimeSpec("runtime", ""))
							domainTestDeploy(t, b, "runtime", infra.Port{Port: 8080, Protocol: "tcp", HTTP: true})
							domainTestEnable(t, b, "runtime", true)
							f.mu.Lock()
							f.failure[procedure] = failure.response
							f.mu.Unlock()
							var err error
							if entrypoint == "disable" {
								_, err = b.SetSubdomainAccess(t.Context(), "runtime", false)
							} else {
								d := testDeployment(t, "")
								d.Runtime.Ports = []infra.Port{{Port: 8081, Protocol: "tcp", HTTP: true}}
								_, err = b.Deploy(t.Context(), "runtime", d)
							}
							if !errors.Is(err, failure.want) {
								t.Fatalf("domain reconciliation error swallowed/misclassified: %v, want %v", err, failure.want)
							}
							if fakeCalls(f, procedure) == 0 {
								t.Fatalf("failure fixture never reached %s", procedure)
							}
						})
					}
				})
			}
		})
	}
}

func TestDomainsDisableRepairsStaleSharedNetworkWithoutMetadataChange(t *testing.T) {
	f := newFakeAPI(t)
	b := fakeBackend(t, f, nil)
	imported := importFake(t, b, runtimeSpec("runtime", ""))
	id := imported.Services["runtime"]
	domainTestDeploy(t, b, "runtime", infra.Port{Port: 8080, Protocol: "tcp"})
	m := fakeMeta(t, f, "runtime")
	if m.SubdomainEnabled || len(m.HTTPPorts) != 0 {
		t.Fatal("fixture must already have HTTPS disabled and no HTTP ports")
	}
	// Simulate stale Dokploy/Swarm configuration without altering Karakoram's
	// already-disabled metadata, as can remain after an earlier controller.
	stale := []map[string]any{{"Target": b.network(), "Aliases": []string{"runtime"}}, {"Target": "dokploy-network"}}
	if err := b.c.post(t.Context(), "application.update", map[string]any{"applicationId": id, "networkSwarm": stale}, nil); err != nil {
		t.Fatal(err)
	}
	f.mu.Lock()
	f.apply(f.apps[id])
	f.mu.Unlock()
	if got := appliedNetworks(t, f, "runtime"); len(got) != 2 {
		t.Fatal("fixture did not attach stale shared network")
	}
	reloads := fakeCalls(f, "application.reload")
	op, err := b.SetSubdomainAccess(t.Context(), "runtime", false)
	if err != nil {
		t.Fatal(err)
	}
	awaitFake(t, b, op)
	if fakeCalls(f, "application.reload") != reloads+1 {
		t.Fatal("unchanged disabled flag prevented stale network reconciliation")
	}
	assertIsolatedNetwork(t, b, appliedNetworks(t, f, "runtime"))
	row := fakeAppSnapshot(t, f, "runtime")
	networks, ok := row["networkSwarm"].([]any)
	if !ok || len(networks) != 1 || networks[0].(map[string]any)["Target"] != b.network() {
		t.Fatalf("database retained stale shared network: %+v", row["networkSwarm"])
	}
	domainTestEnable(t, b, "runtime", false)
	if fakeCalls(f, "application.reload") != reloads+1 {
		t.Fatal("repaired disabled configuration was not idempotent")
	}
}
