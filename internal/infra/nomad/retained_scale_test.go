// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package nomad

import (
	"fmt"
	"io"
	"net/http"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/infra"
)

func retainedAllocationIDs(t *testing.T, f *fakeNomad) map[string]bool {
	t.Helper()
	f.mu.Lock()
	defer f.mu.Unlock()
	ids := map[string]bool{}
	for id, a := range f.allocs {
		if a.row["ClientStatus"] == "running" && a.row["DesiredStatus"] == "run" {
			ids[id] = true
		}
	}
	return ids
}

func assertRetainedAllocations(t *testing.T, f *fakeNomad, before map[string]bool, desired int) {
	t.Helper()
	f.mu.Lock()
	defer f.mu.Unlock()
	running, stopped := 0, 0
	for id, a := range f.allocs {
		if !before[id] {
			t.Fatalf("count-only registration created allocation %s", id)
		}
		if a.row["ClientStatus"] == "running" && a.row["DesiredStatus"] == "run" {
			running++
		} else if fakeTerminal(a) {
			stopped++
		} else {
			t.Fatalf("allocation %s not running or terminal", id)
		}
	}
	if running != desired || stopped != len(before)-desired {
		t.Fatalf("running=%d stopped=%d want=%d/%d", running, stopped, desired, len(before)-desired)
	}
}

func TestCountOnlyScaleRetainsAllocationIdentity(t *testing.T) {
	for _, initial := range []int{2, 3} {
		for _, lost := range []bool{false, true} {
			t.Run(fmt.Sprintf("%d/lost=%t", initial, lost), func(t *testing.T) {
				f := newFakeNomad(t)
				b := fakeBackend(t, f, nil)
				spec := runtimeSpec("runtime", "")
				spec.MinContainers, spec.MaxContainers = initial, initial
				importFake(t, b, spec)
				deployFake(t, b, "runtime", "")
				before := retainedAllocationIDs(t, f)
				op, err := b.SetContainerCount(t.Context(), "runtime", initial-1, initial-1)
				if err != nil {
					t.Fatal(err)
				}
				requests := len(f.requests())
				injected := false
				if lost {
					transport := b.c.http.Transport
					b.c.http.Transport = testRoundTripper(func(req *http.Request) (*http.Response, error) {
						response, err := transport.RoundTrip(req)
						if !injected && err == nil && req.Method == http.MethodPost && req.URL.Path == "/v1/jobs" {
							injected = true
							_ = response.Body.Close()
							return nil, io.ErrUnexpectedEOF
						}
						return response, err
					})
					if _, err = b.OpStatus(t.Context(), op); err == nil || !injected {
						t.Fatalf("registration response not lost: %v", err)
					}
					r, err := b.require(t.Context(), "runtime")
					if err != nil {
						t.Fatal(err)
					}
					b = fakeBackend(t, f, nil)
					b.clock = func() time.Time { return r.Pending.Lease.Until.Add(restoreFence) }
				}
				awaitFake(t, b, roundTripOp(t, op))
				posts := 0
				for _, req := range f.requests()[requests:] {
					if req.Method == http.MethodPost && req.Path == "/v1/jobs" {
						posts++
					}
				}
				if posts != 1 {
					t.Fatalf("count-only registration submitted %d times", posts)
				}
				assertRetainedAllocations(t, f, before, initial-1)
				service, err := b.GetService(t.Context(), "runtime")
				if err != nil || service.Status != infra.StatusRunning {
					t.Fatalf("service not running: %+v %v", service, err)
				}
			})
		}
	}
}

func TestCountOnlyScaleIncreaseRetainsAllocationIdentity(t *testing.T) {
	for _, initial := range []int{1, 2} {
		for _, lost := range []bool{false, true} {
			t.Run(fmt.Sprintf("%d/lost=%t", initial, lost), func(t *testing.T) {
				f := newFakeNomad(t)
				b := fakeBackend(t, f, nil)
				spec := runtimeSpec("runtime", "")
				spec.MinContainers, spec.MaxContainers = initial, initial
				importFake(t, b, spec)
				deployFake(t, b, "runtime", "")
				before := retainedAllocationIDs(t, f)
				f.mu.Lock()
				meta := fakeCopy(fakeMap(f.jobs[fakeKey("default", "runtime")].row["Meta"]))
				f.mu.Unlock()
				op, err := b.SetContainerCount(t.Context(), "runtime", initial+1, initial+1)
				if err != nil {
					t.Fatal(err)
				}
				requests := len(f.requests())
				if lost {
					transport := b.c.http.Transport
					injected := false
					b.c.http.Transport = testRoundTripper(func(req *http.Request) (*http.Response, error) {
						response, err := transport.RoundTrip(req)
						if !injected && err == nil && req.Method == http.MethodPost && req.URL.Path == "/v1/jobs" {
							injected = true
							_ = response.Body.Close()
							return nil, io.ErrUnexpectedEOF
						}
						return response, err
					})
					if _, err = b.OpStatus(t.Context(), op); err == nil || !injected {
						t.Fatalf("registration response not lost: %v", err)
					}
					r, err := b.require(t.Context(), "runtime")
					if err != nil {
						t.Fatal(err)
					}
					b = fakeBackend(t, f, nil)
					b.clock = func() time.Time { return r.Pending.Lease.Until.Add(restoreFence) }
				}
				awaitFake(t, b, roundTripOp(t, op))
				posts := 0
				for _, req := range f.requests()[requests:] {
					if req.Method == http.MethodPost && req.Path == "/v1/jobs" {
						posts++
					}
				}
				if posts != 1 {
					t.Fatalf("count increase registered %d times", posts)
				}
				f.mu.Lock()
				defer f.mu.Unlock()
				job := f.jobs[fakeKey("default", "runtime")]
				update := fakeMap(fakeRows(job.row["TaskGroups"])[0]["Update"])
				if fakeInt(update, "Canary") != 0 || update["AutoPromote"] != false || !reflect.DeepEqual(meta, fakeMap(job.row["Meta"])) {
					t.Fatalf("count increase changed serving metadata or enabled canary: %+v", update)
				}
				if len(f.allocs) != initial+1 {
					t.Fatalf("created replacements: total=%d want=%d", len(f.allocs), initial+1)
				}
				for id := range before {
					a := f.allocs[id]
					if a == nil || a.row["ClientStatus"] != "running" || a.row["DesiredStatus"] != "run" {
						t.Fatalf("serving allocation %s replaced", id)
					}
					for _, state := range fakeMap(a.row["TaskStates"]) {
						if fakeInt(fakeMap(state), "Restarts") != 0 {
							t.Fatalf("serving allocation %s restarted", id)
						}
					}
				}
			})
		}
	}
}

func TestFailedScaleRecoveryRefusesRevertedIncreaseWithoutDesiredHealth(t *testing.T) {
	f, b, _ := failedStatelessScaleFixture(t, true)
	before := retainedAllocationIDs(t, f)
	if len(before) != 2 {
		t.Fatalf("fixture serving count=%d want=2", len(before))
	}
	requests := len(f.requests())
	for range 3 {
		op, accepted, err := b.RecoverFailedScale(t.Context(), "runtime", 3, 3)
		if err != nil || accepted || op.ID != "" {
			t.Fatalf("reverted increase automatically retried placement: %+v %t %v", op, accepted, err)
		}
	}
	for _, req := range f.requests()[requests:] {
		if req.Method == http.MethodPost && req.Path == "/v1/jobs" {
			t.Fatal("refused recovery registered a job")
		}
	}
	assertRetainedAllocations(t, f, before, 2)
}

func TestRetainOnlyRegistrationRefusesPlacementAfterNativeCountReduction(t *testing.T) {
	f, b, _ := failedStatelessScaleFixture(t, true)
	op, accepted, err := b.RecoverFailedScale(t.Context(), "runtime", 2, 2)
	if err != nil || !accepted {
		t.Fatalf("recovery admission: %t %v", accepted, err)
	}
	f.mu.Lock()
	job := f.jobs[fakeKey("default", "runtime")]
	fakeRows(job.row["TaskGroups"])[0]["Count"] = 1
	for _, a := range f.allocs {
		if a.row["ClientStatus"] == "running" {
			f.stopAlloc(a)
			break
		}
	}
	f.mu.Unlock()
	f.tick()
	requests := len(f.requests())
	state, err := b.OpStatus(t.Context(), op)
	if err != nil || !state.Failed {
		t.Fatalf("retain-only registration retried placement: %+v %v", state, err)
	}
	for _, req := range f.requests()[requests:] {
		if req.Method == http.MethodPost && req.Path == "/v1/jobs" {
			t.Fatal("retain-only recovery registered a placement-requiring increase")
		}
	}
}

func TestFakeCountIncreaseRejectsDestructiveRetention(t *testing.T) {
	for _, change := range []string{"meta", "canary"} {
		t.Run(change, func(t *testing.T) {
			f := newFakeNomad(t)
			body := fakeJobBody("member", "", 1)
			job := fakeMap(body["Job"])
			group := fakeRows(job["TaskGroups"])[0]
			group["Update"] = fakeRow{"Canary": 0, "AutoPromote": false}
			fakeRegister(t, f, body)
			f.tick()
			f.tick()
			before := retainedAllocationIDs(t, f)
			group["Count"] = 2
			if change == "meta" {
				job["Meta"] = fakeRow{"karakoram.operation": "changed"}
			} else {
				group["Update"] = fakeRow{"Canary": 1, "AutoPromote": true}
			}
			fakeRegister(t, f, body)
			f.tick()
			f.tick()
			f.tick()
			after := retainedAllocationIDs(t, f)
			for id := range before {
				if after[id] {
					t.Fatalf("%s change incorrectly retained serving allocation %s", change, id)
				}
			}
			if len(fakeAllocs(t, f, "member")) != 3 {
				t.Fatal("destructive count increase did not create replacements")
			}
		})
	}
}

func TestRetainedScaleRejectsSupersededReceipt(t *testing.T) {
	for _, lost := range []bool{false, true} {
		t.Run(fmt.Sprint(lost), func(t *testing.T) {
			f, b, _ := failedStatelessScaleFixture(t, true)
			op, accepted, err := b.RecoverFailedScale(t.Context(), "runtime", 2, 2)
			if err != nil || !accepted {
				t.Fatalf("recovery: %t %v", accepted, err)
			}
			if lost {
				transport := b.c.http.Transport
				injected := false
				b.c.http.Transport = testRoundTripper(func(req *http.Request) (*http.Response, error) {
					response, err := transport.RoundTrip(req)
					if !injected && err == nil && req.Method == http.MethodPost && req.URL.Path == "/v1/jobs" {
						injected = true
						_ = response.Body.Close()
						return nil, io.ErrUnexpectedEOF
					}
					return response, err
				})
				if _, err = b.OpStatus(t.Context(), op); err == nil || !injected {
					t.Fatalf("registration response not lost: %v", err)
				}
			} else {
				if _, err = b.OpStatus(t.Context(), op); err != nil {
					t.Fatal(err)
				}
			}
			f.mu.Lock()
			job := f.jobs[fakeKey("default", "runtime")]
			job.row["Version"] = fakeInt(job.row, "Version") + 1
			job.row["JobModifyIndex"] = f.nextIndex()
			f.mu.Unlock()
			r, err := b.require(t.Context(), "runtime")
			if err != nil {
				t.Fatal(err)
			}
			b = fakeBackend(t, f, nil)
			if r.Pending.Lease != nil {
				b.clock = func() time.Time { return r.Pending.Lease.Until.Add(restoreFence) }
			}
			state, err := b.OpStatus(t.Context(), op)
			if err != nil || !state.Failed || !strings.Contains(state.Message, "superseded") {
				t.Fatalf("superseded registration adopted: %+v %v", state, err)
			}
		})
	}
}

func TestRecoveryAdoptsLostRegistrationWithoutReplacingRetainedAllocations(t *testing.T) {
	f, b, _ := failedStatelessScaleFixture(t, true)
	before := retainedAllocationIDs(t, f)
	op, accepted, err := b.RecoverFailedScale(t.Context(), "runtime", 2, 2)
	if err != nil || !accepted {
		t.Fatalf("recovery: %t %v", accepted, err)
	}
	requests := len(f.requests())
	transport := b.c.http.Transport
	injected := false
	b.c.http.Transport = testRoundTripper(func(req *http.Request) (*http.Response, error) {
		response, err := transport.RoundTrip(req)
		if !injected && err == nil && req.Method == http.MethodPost && req.URL.Path == "/v1/jobs" {
			injected = true
			_ = response.Body.Close()
			return nil, io.ErrUnexpectedEOF
		}
		return response, err
	})
	if _, err = b.OpStatus(t.Context(), op); err == nil || !injected {
		t.Fatalf("registration response not lost: %v", err)
	}
	r, err := b.require(t.Context(), "runtime")
	if err != nil {
		t.Fatal(err)
	}
	b = fakeBackend(t, f, nil)
	b.clock = func() time.Time { return r.Pending.Lease.Until.Add(restoreFence) }
	awaitFake(t, b, op)
	assertRetainedAllocations(t, f, before, 2)
	posts := 0
	for _, req := range f.requests()[requests:] {
		if req.Method == http.MethodPost && req.Path == "/v1/jobs" {
			posts++
		}
	}
	if posts != 1 {
		t.Fatalf("recovery registered %d times", posts)
	}
}

func TestRetainedScaleAdoptsAlreadyAppliedSpecificationWithoutRegistration(t *testing.T) {
	f, b, _ := failedStatelessScaleFixture(t, true)
	f.mu.Lock()
	job := f.jobs[fakeKey("default", "runtime")]
	update := fakeMap(fakeRows(job.row["TaskGroups"])[0]["Update"])
	update["Canary"], update["AutoPromote"] = 0, false
	// Match the native successful evaluation to the simulated stable copy.
	for _, id := range job.evaluations {
		f.evals[id]["JobModifyIndex"] = job.row["JobModifyIndex"]
		f.evals[id]["Status"] = "complete"
	}
	f.mu.Unlock()
	before := retainedAllocationIDs(t, f)
	requests := len(f.requests())
	op, accepted, err := b.RecoverFailedScale(t.Context(), "runtime", 2, 2)
	if err != nil || !accepted {
		t.Fatalf("recovery: %t %v", accepted, err)
	}
	awaitFake(t, fakeBackend(t, f, nil), op)
	assertRetainedAllocations(t, f, before, 2)
	for _, req := range f.requests()[requests:] {
		if req.Method == http.MethodPost && req.Path == "/v1/jobs" {
			t.Fatal("identical specification unnecessarily re-registered")
		}
	}
}

func TestFakeCombinedTaskMetaUpdatesAreDestructive(t *testing.T) {
	for _, scope := range []string{"job", "group", "task"} {
		t.Run(scope, func(t *testing.T) {
			f := newFakeNomad(t)
			body := fakeJobBody("member", "", 1)
			fakeRegister(t, f, body)
			f.tick()
			f.tick()
			job := fakeMap(body["Job"])
			group := fakeRows(job["TaskGroups"])[0]
			task := fakeRows(group["Tasks"])[0]
			target := map[string]fakeRow{"job": job, "group": group, "task": task}[scope]
			target["Meta"] = fakeRow{"changed": "yes"}
			fakeRegister(t, f, body)
			if len(fakeAllocs(t, f, "member")) != 2 {
				t.Fatal("CombinedTaskMeta update did not replace task")
			}
		})
	}
}

func TestCountOnlyScaleRetainsAllocationsAcrossCredentialChange(t *testing.T) {
	for _, lost := range []bool{false, true} {
		t.Run(fmt.Sprintf("lost=%t", lost), func(t *testing.T) {
			f := newFakeNomad(t)
			b := fakeBackend(t, f, nil)
			spec := runtimeSpec("runtime", "")
			spec.MinContainers, spec.MaxContainers = 2, 2
			importFake(t, b, spec)
			deployFake(t, b, "runtime", "")
			before := retainedAllocationIDs(t, f)
			f.mu.Lock()
			originalAccessor := fakeString(f.jobs[fakeKey("default", "runtime")].row, "NomadTokenID")
			f.mu.Unlock()
			const replacementToken = "test-replacement-nomad-token"
			f.addToken(replacementToken, false, "default")
			b.SetToken(replacementToken)
			op, err := b.SetContainerCount(t.Context(), "runtime", 1, 1)
			if err != nil {
				t.Fatal(err)
			}
			requests := len(f.requests())
			if lost {
				transport := b.c.http.Transport
				injected := false
				b.c.http.Transport = testRoundTripper(func(req *http.Request) (*http.Response, error) {
					response, err := transport.RoundTrip(req)
					if !injected && err == nil && req.Method == http.MethodPost && req.URL.Path == "/v1/jobs" {
						injected = true
						_ = response.Body.Close()
						return nil, io.ErrUnexpectedEOF
					}
					return response, err
				})
				if _, err = b.OpStatus(t.Context(), op); err == nil || !injected {
					t.Fatalf("registration response not lost: %v", err)
				}
				r, err := b.require(t.Context(), "runtime")
				if err != nil {
					t.Fatal(err)
				}
				b = fakeBackend(t, f, nil)
				b.SetToken(replacementToken)
				b.clock = func() time.Time { return r.Pending.Lease.Until.Add(restoreFence) }
			}
			awaitFake(t, b, roundTripOp(t, op))
			assertRetainedAllocations(t, f, before, 1)
			f.mu.Lock()
			registeredAccessor := fakeString(f.jobs[fakeKey("default", "runtime")].row, "NomadTokenID")
			expectedAccessor := f.tokens[replacementToken].accessor
			f.mu.Unlock()
			if originalAccessor == "" || registeredAccessor == originalAccessor || registeredAccessor != expectedAccessor {
				t.Fatal("registration did not replace the old token accessor with the new credential's accessor")
			}
			posts := 0
			for _, req := range f.requests()[requests:] {
				if req.Method == http.MethodPost && req.Path == "/v1/jobs" {
					posts++
				}
			}
			if posts != 1 {
				t.Fatalf("credential change caused %d registrations", posts)
			}
			service, err := b.GetService(t.Context(), "runtime")
			if err != nil || service.Status != infra.StatusRunning {
				t.Fatalf("service not running after credential change: %+v %v", service, err)
			}
		})
	}
}
