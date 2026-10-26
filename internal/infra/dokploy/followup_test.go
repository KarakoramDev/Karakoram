// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package dokploy

import (
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/infra"
)

func loseCommittedReply(b *Backend, match func(*http.Request) bool) {
	transport := b.c.http.Transport
	if transport == nil {
		transport = http.DefaultTransport
	}
	var lost atomic.Bool
	b.c.http.Transport = restoreTransport(func(r *http.Request) (*http.Response, error) {
		response, err := transport.RoundTrip(r)
		if err == nil && response.StatusCode >= 200 && response.StatusCode < 300 && match(r) && lost.CompareAndSwap(false, true) {
			_, _ = io.Copy(io.Discard, response.Body)
			response.Body.Close()
			return nil, io.ErrUnexpectedEOF
		}
		return response, err
	})
}

func TestImportRecoversCommittedCreateReplyLoss(t *testing.T) {
	f := newFakeAPI(t)
	b := fakeBackend(t, f, nil)
	loseCommittedReply(b, func(r *http.Request) bool { return r.URL.Path == "/api/application.create" })
	op, err := b.ImportServices(t.Context(), []infra.ServiceSpec{volumeSpec("volume"), runtimeSpec("runtime", "volume")})
	if err != nil {
		t.Fatal(err)
	}
	awaitFake(t, b, op)
	if len(op.Services) != 2 || fakeCalls(f, "application.create") != 2 {
		t.Fatalf("creation identities were lost or duplicated: %+v", op)
	}
	if fakeCalls(f, "application.delete") != 0 {
		t.Fatal("ambiguous import rolled back a committed ownership receipt")
	}
	if m := fakeMeta(t, f, "volume"); m.Importing || m.ImportDigest == "" || m.Environment != "environment" {
		t.Fatalf("import receipt not finalized: %+v", m)
	}
}

func TestInvisibleCreateOutcomeBlocksDuplicateUntilReceiptVisible(t *testing.T) {
	f := newFakeAPI(t)
	b := fakeBackend(t, f, nil)
	loseCommittedReply(b, func(r *http.Request) bool {
		if r.URL.Path != "/api/application.create" {
			return false
		}
		f.mu.Lock()
		defer f.mu.Unlock()
		for id := range f.apps {
			f.omitApplications[id] = true
			f.omitSearch[id] = true
		}
		return true
	})
	specs := []infra.ServiceSpec{runtimeSpec("runtime", "")}
	if _, err := b.ImportServices(t.Context(), specs); err == nil {
		t.Fatal("lost invisible creation not exercised")
	}
	other := fakeBackend(t, f, nil)
	if _, err := other.ImportServices(t.Context(), specs); !errors.Is(err, infra.ErrConflict) {
		t.Fatalf("unknown outcome not fenced: %v", err)
	}
	if fakeCalls(f, "application.create") != 1 {
		t.Fatal("unknown creation was duplicated")
	}
	f.mu.Lock()
	f.omitApplications = map[string]bool{}
	f.omitSearch = map[string]bool{}
	f.mu.Unlock()
	op, err := other.ImportServices(t.Context(), specs)
	if err != nil {
		t.Fatal(err)
	}
	awaitFake(t, other, op)
	if fakeCalls(f, "application.create") != 1 || len(op.Services) != 1 {
		t.Fatal("visible original receipt was not resumed")
	}
}

func TestConcurrentAndStaleImportsKeepOneIdentity(t *testing.T) {
	f := newFakeAPI(t)
	a := fakeBackend(t, f, nil)
	b := fakeBackend(t, f, nil)
	start := make(chan struct{})
	result := make(chan error, 2)
	for _, backend := range []*Backend{a, b} {
		go func(backend *Backend) {
			<-start
			_, err := backend.ImportServices(t.Context(), []infra.ServiceSpec{runtimeSpec("runtime", "")})
			result <- err
		}(backend)
	}
	close(start)
	first, second := <-result, <-result
	if (first == nil) == (second == nil) || first != nil && !errors.Is(first, infra.ErrConflict) || second != nil && !errors.Is(second, infra.ErrConflict) {
		t.Fatalf("concurrent outcomes: %v %v", first, second)
	}
	row := fakeAppSnapshot(t, f, "runtime")
	id := fakeString(row, "applicationId")
	f.mu.Lock()
	f.omitApplications[id] = true
	f.omitSearch[id] = true
	f.mu.Unlock()
	if _, err := fakeBackend(t, f, nil).ImportServices(t.Context(), []infra.ServiceSpec{runtimeSpec("runtime", "")}); !errors.Is(err, infra.ErrConflict) {
		t.Fatalf("stale listings lost known identity: %v", err)
	}
	if fakeCalls(f, "application.create") != 1 {
		t.Fatal("stale listing created another application")
	}
}

func TestFencedDeleteUsesPositiveIDLookupWithStaleListings(t *testing.T) {
	f := newFakeAPI(t)
	b := fakeBackend(t, f, nil)
	ids := importFake(t, b, runtimeSpec("runtime", "")).Services
	id := ids["runtime"]
	f.mu.Lock()
	f.omitApplications[id] = true
	f.omitSearch[id] = true
	f.mu.Unlock()
	b.remember("runtime", "")
	op, err := b.DeleteServiceByID(t.Context(), "runtime", id)
	if err != nil || op.ID == "" {
		t.Fatalf("positive expected ID was acknowledged absent: %+v %v", op, err)
	}
	awaitFake(t, b, op)
	if fakeCalls(f, "application.delete") != 1 {
		t.Fatal("existing expected resource was not deleted")
	}
}

func TestFencedDeleteRejectsExpectedForeignEnvironment(t *testing.T) {
	f := newFakeAPI(t)
	b := fakeBackend(t, f, nil)
	id := importFake(t, b, runtimeSpec("runtime", "")).Services["runtime"]
	f.mu.Lock()
	f.environments["other-environment"] = map[string]any{"environmentId": "other-environment", "projectId": "project", "name": "outside"}
	f.apps[id].row["environmentId"] = "other-environment"
	f.omitApplications[id] = true
	f.omitSearch[id] = true
	f.mu.Unlock()
	b.remember("runtime", "")
	if _, err := b.DeleteServiceByID(t.Context(), "runtime", id); !errors.Is(err, infra.ErrConflict) {
		t.Fatalf("foreign expected ID was trusted: %v", err)
	}
	if fakeCalls(f, "application.delete") != 0 {
		t.Fatal("foreign application was deleted")
	}
}

func TestPortRetryAppliesCommittedRowsAndMetadata(t *testing.T) {
	for _, fault := range []string{"port.create", "port.delete", "application.reload"} {
		t.Run(fault, func(t *testing.T) {
			f := newFakeAPI(t)
			b := fakeBackend(t, f, nil)
			id := importFake(t, b, runtimeSpec("runtime", "")).Services["runtime"]
			deployFake(t, b, "runtime", "")
			p := infra.PublicPort{Port: 19090, TargetPort: 9090, Protocol: "tcp", IPType: "IPV4"}
			if fault == "port.delete" {
				if err := b.EnsurePublicPort(t.Context(), "runtime", p); err != nil {
					t.Fatal(err)
				}
			}
			loseCommittedReply(b, func(r *http.Request) bool { return r.URL.Path == "/api/"+fault })
			var err error
			if fault == "port.delete" {
				err = b.DeletePublicPorts(t.Context(), "runtime")
			} else {
				err = b.EnsurePublicPort(t.Context(), "runtime", p)
			}
			if err == nil {
				t.Fatal("post-commit reply loss was not exercised")
			}
			other := fakeBackend(t, f, nil)
			if fault == "application.reload" {
				// A lost reload response is not permission to submit again.
				// Observe the original replacement before retiring its fence.
				finishPublicOperation(t, other, f, "runtime")
			}
			if fault == "port.delete" {
				err = other.DeletePublicPorts(t.Context(), "runtime")
			} else {
				err = other.EnsurePublicPort(t.Context(), "runtime", p)
			}
			if err != nil {
				t.Fatal(err)
			}
			f.mu.Lock()
			a := f.apps[id]
			rows, applied := len(a.ports), len(a.appliedPorts)
			f.mu.Unlock()
			want := 1
			if fault == "port.delete" {
				want = 0
			}
			if rows != want || applied != want || fakeMeta(t, f, "runtime").PortsPending {
				t.Fatalf("successful retry did not apply endpoint: rows=%d applied=%d want=%d", rows, applied, want)
			}
		})
	}
}

func TestDeploymentAttemptReceiptSurvivesLostCommitResponses(t *testing.T) {
	for _, stage := range []string{"upload", "final-metadata", "initial-metadata"} {
		t.Run(stage, func(t *testing.T) {
			f := newFakeAPI(t)
			b := fakeBackend(t, f, nil)
			id := importFake(t, b, runtimeSpec("runtime", "")).Services["runtime"]
			f.mu.Lock()
			f.apps[id].holdDeployments = true
			f.mu.Unlock()
			uploaded := false
			loseCommittedReply(b, func(r *http.Request) bool {
				if r.URL.Path == "/api/application.dropDeployment" {
					uploaded = true
					return stage == "upload"
				}
				return r.URL.Path == "/api/application.update" && (stage == "initial-metadata" && !uploaded || stage == "final-metadata" && uploaded)
			})
			op, err := b.Deploy(t.Context(), "runtime", testDeployment(t, ""))
			if err == nil || op.ID == "" {
				t.Fatalf("ambiguous attempt lost its returned handle: %+v %v", op, err)
			}
			m := fakeMeta(t, f, "runtime")
			if m.Pending == nil || m.Pending.Digest == "" {
				t.Fatal("immutable archive receipt was not saved before submission")
			}
			other := fakeBackend(t, f, nil)
			recovered, err := other.Deploy(t.Context(), "runtime", testDeployment(t, ""))
			if err != nil || recovered.ID != op.ID {
				t.Fatalf("retry did not recover original attempt: %+v %v", recovered, err)
			}
			want := 1
			if stage == "initial-metadata" {
				want = 0
			}
			if fakeCalls(f, "application.dropDeployment") != want {
				t.Fatal("deployment retry replayed an ambiguous upload")
			}
			if stage != "initial-metadata" {
				f.mu.Lock()
				f.apps[id].holdDeployments = false
				f.mu.Unlock()
				awaitFake(t, other, recovered)
			}
		})
	}
}

func TestMissingAndStalledBuildHaveBoundedActionableOutcome(t *testing.T) {
	for _, missing := range []bool{false, true} {
		t.Run(map[bool]string{false: "building", true: "unknown-queue"}[missing], func(t *testing.T) {
			f := newFakeAPI(t)
			b := fakeBackend(t, f, nil)
			id := importFake(t, b, runtimeSpec("runtime", "")).Services["runtime"]
			f.mu.Lock()
			f.apps[id].holdDeployments = true
			f.mu.Unlock()
			op, err := b.Deploy(t.Context(), "runtime", testDeployment(t, ""))
			if err != nil {
				t.Fatal(err)
			}
			if missing {
				f.mu.Lock()
				f.apps[id].deployments = nil
				f.mu.Unlock()
			}
			o, _ := decodeOp(op)
			b.now = func() time.Time { return o.Issued.Add(buildProgressWindow - time.Second) }
			state, err := b.OpStatus(t.Context(), op)
			if err != nil || state.Failed || state.Message == "" || state.Status == "pending" {
				t.Fatalf("no specific queue/build diagnosis: %+v %v", state, err)
			}
			b.now = func() time.Time { return o.Issued.Add(buildProgressWindow + time.Second) }
			state, err = b.OpStatus(t.Context(), op)
			if err != nil || !state.Failed || state.Status != "stalled" || !strings.Contains(state.Message, "original attempt may still commit") {
				t.Fatalf("missing terminal bound: %+v %v", state, err)
			}
			if _, err = b.Stop(t.Context(), "runtime"); !errors.Is(err, infra.ErrConflict) {
				t.Fatalf("stalled-but-ambiguous build was unsafely superseded: %v", err)
			}
			if fakeCalls(f, "application.dropDeployment") != 1 || fakeCalls(f, "application.stop") != 0 {
				t.Fatal("stalled attempt was replayed or falsely stopped")
			}
		})
	}
}

func TestPinnedNodeFailuresAreDiagnosedWithoutRelocation(t *testing.T) {
	for _, fault := range []string{"down", "drain", "missing"} {
		t.Run(fault, func(t *testing.T) {
			f := newFakeAPI(t)
			b := fakeBackend(t, f, nil)
			importFake(t, b, volumeSpec("volume"), runtimeSpec("runtime", "volume"))
			deployFake(t, b, "runtime", "volume")
			original := fakeRowsCopy(f.nodes)
			f.mu.Lock()
			if fault == "missing" {
				f.nodes = f.nodes[1:]
			} else if fault == "down" {
				f.nodes[0]["Status"].(map[string]any)["State"] = "down"
			} else {
				f.nodes[0]["Spec"].(map[string]any)["Availability"] = "drain"
			}
			f.mu.Unlock()
			for _, name := range []string{"volume", "runtime"} {
				s, err := b.GetService(t.Context(), name)
				if s == nil || s.Status != infra.StatusFailed || !errors.Is(err, infra.ErrUnavailable) || !strings.Contains(infra.SafeDetail(err), "original pinned storage node") {
					t.Fatalf("node loss was hidden for %s: %+v %v", name, s, err)
				}
			}
			if _, err := b.Start(t.Context(), "runtime"); !errors.Is(err, infra.ErrUnavailable) {
				t.Fatalf("unsafe restart on lost storage node: %v", err)
			}
			if _, err := b.Deploy(t.Context(), "runtime", testDeployment(t, "volume")); !errors.Is(err, infra.ErrUnavailable) {
				t.Fatalf("unsafe deploy on lost storage node: %v", err)
			}
			f.mu.Lock()
			f.nodes = original
			f.mu.Unlock()
			observeRestored(t, b, false)
			if fakeCalls(f, "application.reload") != 0 || fakeCalls(f, "application.dropDeployment") != 1 {
				t.Fatal("node fault caused relocation/redeployment")
			}
		})
	}
}

func TestWipeCannotAcknowledgeUnknownOriginalNode(t *testing.T) {
	f := newFakeAPI(t)
	b := fakeBackend(t, f, nil)
	op := encodeOp(operation{Action: "wipe", AppID: "missing-holder", AppName: "missing-volume", Hostname: "volume", Node: "missing-node", Issued: time.Now()})
	state, err := b.OpStatus(t.Context(), op)
	if err == nil || state.Done || !strings.Contains(infra.SafeDetail(err), "original node") {
		t.Fatalf("unidentifiable cleanup node was acknowledged: %+v %v", state, err)
	}
}

func TestRateLimitsCarryResetWithoutImmediateTransportRetry(t *testing.T) {
	for _, method := range []string{http.MethodGet, http.MethodPost} {
		for _, header := range []string{"3600", time.Now().Add(time.Hour).UTC().Format(http.TimeFormat), "invalid"} {
			t.Run(method+"/"+header, func(t *testing.T) {
				var calls atomic.Int32
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					calls.Add(1)
					w.Header().Set("Retry-After", header)
					w.WriteHeader(http.StatusTooManyRequests)
					_, _ = io.WriteString(w, `{"code":"TOO_MANY_REQUESTS","message":"rate limit exhausted"}`)
				}))
				defer server.Close()
				c := newClient(server.URL, "private-rate-limit-key")
				var err error
				if method == http.MethodGet {
					err = c.get(t.Context(), "environment.one", nil, nil)
				} else {
					err = c.post(t.Context(), "application.update", map[string]any{"applicationId": "app"}, nil)
				}
				if !errors.Is(err, infra.ErrUnavailable) || calls.Load() != 1 {
					t.Fatalf("rate limit amplified or misclassified: calls=%d err=%v", calls.Load(), err)
				}
				at := infra.RetryAt(err)
				if header != "invalid" && at.Before(time.Now().Add(59*time.Minute)) {
					t.Fatalf("long reset was lost/capped: %v", at)
				}
				if strings.Contains(infra.SafeDetail(err), "private-rate-limit-key") {
					t.Fatal("token leaked")
				}
			})
		}
	}
}

func TestExplicitPartialImportDeletionReleasesCreationClaim(t *testing.T) {
	f := newFakeAPI(t)
	b := fakeBackend(t, f, nil)
	f.failureOnce["mounts.create"] = fakeResponse{500, "INTERNAL_SERVER_ERROR", "temporary setup failure"}
	partial, err := b.ImportServices(t.Context(), []infra.ServiceSpec{volumeSpec("volume")})
	if err == nil || partial.Services["volume"] == "" {
		t.Fatalf("partial creation receipt missing: %+v %v", partial, err)
	}
	oldID := partial.Services["volume"]
	deletion, err := b.DeleteServiceByID(t.Context(), "volume", oldID)
	if err != nil {
		t.Fatal(err)
	}
	awaitFake(t, b, deletion)
	recreated := importFake(t, fakeBackend(t, f, nil), volumeSpec("volume"))
	if recreated.Services["volume"] == oldID {
		t.Fatal("explicit deletion retained a stale creation identity")
	}
}
