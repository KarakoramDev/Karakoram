// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package dokploy

import (
	"errors"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/infra"
)

func TestCompletedWipesReleaseReservations(t *testing.T) {
	f := newFakeAPI(t)
	b := fakeBackend(t, f, nil)
	importFake(t, b, volumeSpec("one"), volumeSpec("two"), runtimeSpec("runtime", ""))
	for _, name := range []string{"one", "two"} {
		op, err := b.DeleteService(t.Context(), name)
		if err != nil {
			t.Fatal(err)
		}
		awaitFake(t, b, op)
	}
	if _, err := b.Deploy(t.Context(), "runtime", testDeployment(t, "")); err != nil {
		t.Fatal(err)
	}
	if len(b.deploymentServer().reservations) != 1 {
		t.Fatal("deleted wipe reservations retained")
	}
}

func TestAbsentReservedApplicationCorroborated(t *testing.T) {
	t.Setenv("KARAKORAM_DOKPLOY_MAX_INFLIGHT_DEPLOYS", "1")
	f := newFakeAPI(t)
	b := fakeBackend(t, f, nil)
	ids := importFake(t, b, runtimeSpec("one", ""), runtimeSpec("two", "")).Services
	release, err := b.deploymentSlot(t.Context(), ids["one"])
	if err != nil {
		t.Fatal(err)
	}
	release(true)
	f.mu.Lock()
	delete(f.apps, ids["one"])
	f.mu.Unlock()
	before := fakeCalls(f, "application.one")
	release, err = b.deploymentSlot(t.Context(), ids["two"])
	if err != nil {
		t.Fatal(err)
	}
	release(false)
	if fakeCalls(f, "application.one") <= before {
		t.Fatal("absence not corroborated by exact ID")
	}
}

func TestEnqueueRefusalRetiresClaims(t *testing.T) {
	for _, action := range []string{"wipe", "rebuild"} {
		for _, code := range []int{400, 401, 403, 429, 408, 500, 0} {
			t.Run(fmt.Sprintf("%s/%d", action, code), func(t *testing.T) {
				t.Setenv("KARAKORAM_DOKPLOY_MAX_INFLIGHT_DEPLOYS", "1")
				f := newFakeAPI(t)
				b := fakeBackend(t, f, nil)
				ids := importFake(t, b, volumeSpec("volume"), runtimeSpec("runtime", ""), runtimeSpec("other", "")).Services
				if action == "rebuild" {
					deployFake(t, b, "runtime", "")
					removeFakeService(f, ids["runtime"])
					f.mu.Lock()
					f.images = map[string]bool{}
					f.mu.Unlock()
				}
				f.mu.Lock()
				f.requestHook = func(w http.ResponseWriter, r *http.Request) (bool, func()) {
					if strings.HasSuffix(r.URL.Path, "/application.deploy") {
						if code == 0 {
							conn, _, err := w.(http.Hijacker).Hijack()
							if err == nil {
								_ = conn.Close()
							}
							return true, nil
						}
						w.Header().Set("Retry-After", "45")
						fakeError(w, code, "REFUSED", "original refusal", "application.deploy")
						return true, nil
					}
					return false, nil
				}
				f.mu.Unlock()
				var err error
				if action == "wipe" {
					_, err = b.DeleteService(t.Context(), "volume")
				} else {
					_, err = b.GetService(t.Context(), "runtime")
				}
				var api *APIError
				if err == nil || code != 0 && (!errors.As(err, &api) || api.StatusCode != code) {
					t.Fatalf("original error lost: %v", err)
				}
				if code == 429 && infra.RetryAt(err).Before(time.Now().Add(40*time.Second)) {
					t.Fatalf("retry deadline lost: %v", err)
				}
				definitive := code >= 400 && code < 500 && code != 408
				m := fakeMeta(t, f, map[string]string{"wipe": "volume", "rebuild": "runtime"}[action])
				if definitive && (m.Wiping || m.Pending != nil || !m.MutationUntil.IsZero()) {
					t.Fatalf("durable refused receipt retained: %+v", m)
				}
				if definitive && len(b.deploymentServer().reservations) != 0 {
					t.Fatal("local refused claim retained")
				}
				f.mu.Lock()
				f.requestHook = nil
				f.mu.Unlock()
				deploymentServers.Delete(b.target.Config["url"])
				_, err = fakeBackend(t, f, nil).Deploy(t.Context(), "other", testDeployment(t, ""))
				if definitive && err != nil {
					t.Fatalf("restart reconstructed refused claim: %v", err)
				}
				if !definitive && err == nil {
					t.Fatal("ambiguous claim lost on restart")
				}
			})
		}
	}
}

func TestDeploymentWaitIdentity(t *testing.T) {
	f := newFakeAPI(t)
	b := fakeBackend(t, f, nil)
	ids := importFake(t, b, runtimeSpec("one", ""), runtimeSpec("two", ""), runtimeSpec("three", "")).Services
	holdDeploymentApps(f)
	for _, name := range []string{"one", "two"} {
		if _, err := b.Deploy(t.Context(), name, testDeployment(t, "")); err != nil {
			t.Fatal(err)
		}
	}
	for _, tc := range []struct{ id, reason string }{{ids["one"], "PreviousDeployment"}, {ids["three"], "DeploymentSlot"}} {
		key := ""
		for range 3 {
			start := time.Now()
			_, err := b.deploymentSlot(t.Context(), tc.id)
			var wait *infra.DeploymentWaitError
			if !errors.As(err, &wait) || wait.Reason != tc.reason || wait.Key == "" || wait.RetryAt.Before(start.Add(15*time.Second)) {
				t.Fatalf("wait contract: %#v %v", wait, err)
			}
			if key != "" && key != wait.Key {
				t.Fatalf("unstable wait identity %q -> %q", key, wait.Key)
			}
			key = wait.Key
		}
	}
}

func TestAmbiguousWipeResolvesAuthoritativeEvidence(t *testing.T) {
	for _, restart := range []bool{false, true} {
		for _, evidence := range []string{"running", "terminal", "absent", "hidden"} {
			// A hidden durable receipt after restart violates the complete admin
			// inventory requirement; exercise exact-ID fallback for local claims.
			if evidence == "hidden" && restart {
				continue
			}
			t.Run(fmt.Sprintf("%s/restart=%t", evidence, restart), func(t *testing.T) {
				t.Setenv("KARAKORAM_DOKPLOY_MAX_INFLIGHT_DEPLOYS", "1")
				f := newFakeAPI(t)
				b := fakeBackend(t, f, nil)
				ids := importFake(t, b, volumeSpec("volume"), runtimeSpec("runtime", "")).Services
				f.mu.Lock()
				f.failure["application.deploy"] = fakeResponse{500, "INTERNAL_SERVER_ERROR", "delivery unknown"}
				f.mu.Unlock()
				if _, err := b.DeleteService(t.Context(), "volume"); err == nil {
					t.Fatal("missing enqueue error")
				}
				if restart {
					deploymentServers.Delete(b.target.Config["url"])
					b = fakeBackend(t, f, nil)
				}
				f.mu.Lock()
				delete(f.failure, "application.deploy")
				a := f.apps[ids["volume"]]
				switch evidence {
				case "running", "terminal":
					status := "running"
					if evidence == "terminal" {
						status = "done"
					}
					a.holdDeployments = true
					a.deployments = []*fakeDeployment{{id: "observed-receipt", status: status}}
				case "absent":
					delete(f.apps, ids["volume"])
				case "hidden":
					f.omitApplications[ids["volume"]] = true
				}
				f.mu.Unlock()
				release, err := b.deploymentSlot(t.Context(), ids["runtime"])
				blocked := evidence == "running" || evidence == "hidden"
				if blocked {
					if !isDeploymentWait(err) {
						t.Fatalf("lost unresolved/live fence: %v", err)
					}
				} else {
					if err != nil {
						t.Fatal(err)
					}
					release(false)
					if len(b.deploymentServer().reservations) != 0 {
						t.Fatal("resolved receipt retained local capacity")
					}
				}
			})
		}
	}
}

func TestFailedWipeManualRemovalCompletes(t *testing.T) {
	f := newFakeAPI(t)
	b := fakeBackend(t, f, nil)
	ids := importFake(t, b, volumeSpec("volume")).Services
	op, err := b.DeleteService(t.Context(), "volume")
	if err != nil {
		t.Fatal(err)
	}
	f.mu.Lock()
	a := f.apps[ids["volume"]]
	a.deployments[0].status = "error"
	name := fakeString(a.row, "appName")
	f.mu.Unlock()
	state, err := b.OpStatus(t.Context(), op)
	if err != nil || !state.Failed {
		t.Fatalf("expected retained failed wipe: %+v %v", state, err)
	}
	// The operator has resolved the queue, verified no live writer, completed
	// erasure on the original node and removed this exact holder and volume.
	if err := b.c.post(t.Context(), "application.delete", map[string]any{"applicationId": ids["volume"]}, nil); err != nil {
		t.Fatal(err)
	}
	f.mu.Lock()
	delete(f.volumes, name)
	f.mu.Unlock()
	deploymentServers.Delete(b.target.Config["url"])
	state, err = fakeBackend(t, f, nil).OpStatus(t.Context(), op)
	if err != nil || !state.Done || state.Failed {
		t.Fatalf("manual wipe cleanup did not resolve operation: %+v %v", state, err)
	}
	if fakeCalls(f, "application.deploy") != 1 {
		t.Fatal("manual recovery automatically enqueued another wipe")
	}
}

func TestTrackedUnknownWipeManualRecoveryAfterMetadataReset(t *testing.T) {
	f := newFakeAPI(t)
	b := fakeBackend(t, f, nil)
	ids := importFake(t, b, volumeSpec("volume")).Services
	f.mu.Lock()
	f.failure["application.deploy"] = fakeResponse{500, "INTERNAL_SERVER_ERROR", "delivery unknown"}
	f.mu.Unlock()
	if _, err := b.DeleteService(t.Context(), "volume"); err == nil {
		t.Fatal("missing ambiguous enqueue error")
	}
	// An engine has already persisted this recovered operation handle.
	op, err := b.DeleteService(t.Context(), "volume")
	if err != nil {
		t.Fatal(err)
	}
	var a application
	if err := b.c.get(t.Context(), "application.one", map[string]string{"applicationId": ids["volume"]}, &a); err != nil {
		t.Fatal(err)
	}
	m, _ := parseMetadata(a.Description)
	m.Wiping = false
	m.WipePreviousDeployment = ""
	m.WipePreviousTasks = nil
	if err := b.update(t.Context(), &a, m, nil); err != nil {
		t.Fatal(err)
	}
	deploymentServers.Delete(b.target.Config["url"])
	b = fakeBackend(t, f, nil)
	state, err := b.OpStatus(t.Context(), op)
	if err != nil || state.Done || state.Failed {
		t.Fatalf("metadata reset must not pretend erasure completed: %+v %v", state, err)
	}
	// Documented recovery: exclude late delivery, erase the exact original
	// volume and remove its holder. Metadata reset alone is not a re-arm.
	if err := b.c.post(t.Context(), "application.delete", map[string]any{"applicationId": ids["volume"]}, nil); err != nil {
		t.Fatal(err)
	}
	f.mu.Lock()
	delete(f.volumes, a.AppName)
	f.mu.Unlock()
	state, err = b.OpStatus(t.Context(), op)
	if err != nil || !state.Done || state.Failed {
		t.Fatalf("manual cleanup did not resolve tracked handle: %+v %v", state, err)
	}
	if fakeCalls(f, "application.deploy") != 1 {
		t.Fatal("tracked-handle recovery enqueued a second wipe")
	}
}
