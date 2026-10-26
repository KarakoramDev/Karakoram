// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package dokploy

import (
	"net/http"
	"strings"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/infra"
)

func TestDeleteRecoversStopSubmissionWithOriginalReceipt(t *testing.T) {
	for _, boundary := range []string{"before-submission", "authoritative-refusal"} {
		for _, resume := range []string{"delete-service", "poll-persisted-receipt"} {
			t.Run(boundary+"/"+resume, func(t *testing.T) {
				f := newFakeAPI(t)
				b := fakeBackend(t, f, nil)
				id := importFake(t, b, runtimeSpec("runtime", "")).Services["runtime"]
				deployFake(t, b, "runtime", "")
				var original infra.Op
				if boundary == "before-submission" {
					a, m, err := b.require(t.Context(), "runtime")
					if err != nil {
						t.Fatal(err)
					}
					service, err := b.inspectService(t.Context(), a.AppName)
					if err != nil {
						t.Fatal(err)
					}
					o := operation{Action: "delete", AppID: id, AppName: a.AppName, Hostname: "runtime", Issued: b.nowTime().UTC(), ServiceIdentity: service.ID}
					m.Desired = "deleting"
					m.Pending = &o
					if err := b.update(t.Context(), a, m, nil); err != nil {
						t.Fatal(err)
					}
					original = encodeOp(o)
				} else {
					f.mu.Lock()
					f.failureOnce["application.stop"] = fakeResponse{status: 403, message: "stop refused"}
					f.mu.Unlock()
					if _, err := b.DeleteServiceByID(t.Context(), "runtime", id); err == nil {
						t.Fatal("stop refusal not returned")
					}
					m := fakeMeta(t, f, "runtime")
					if m.Pending == nil || m.DeleteStopConfirmed {
						t.Fatal("refused stop recorded as confirmed")
					}
					original = encodeOp(*m.Pending)
				}
				b = fakeBackend(t, f, nil)
				if resume == "delete-service" {
					recovered, err := b.DeleteServiceByID(t.Context(), "runtime", id)
					if err != nil || recovered.ID != original.ID {
						t.Fatalf("original receipt changed: %+v %v", recovered, err)
					}
				} else {
					state, err := b.OpStatus(t.Context(), roundTripOp(t, original))
					if err != nil || state.Failed {
						t.Fatalf("poll recovery failed: %+v %v", state, err)
					}
				}
				expectedCalls := 1
				if boundary == "authoritative-refusal" {
					expectedCalls++
				}
				if fakeCalls(f, "application.stop") != expectedCalls || !fakeMeta(t, f, "runtime").DeleteStopConfirmed {
					t.Fatal("stop was not safely recovered")
				}
				awaitFake(t, b, original)
				if fakeCalls(f, "application.stop") != expectedCalls {
					t.Fatal("confirmed stop was replayed")
				}
			})
		}
	}
}

func TestLegacyDeleteConfirmsNativeZeroWithoutStopReplay(t *testing.T) {
	s := stuckDelete(t, false)
	s.age(t)
	s.f.mu.Lock()
	m, _ := parseMetadata(fakeString(s.f.apps[s.id].row, "description"))
	m.Pending.ServiceIdentity = ""
	m.DeleteStopConfirmed = false
	s.op = encodeOp(*m.Pending)
	s.f.apps[s.id].row["description"] = m.description()
	s.f.mu.Unlock()
	s.b = fakeBackend(t, s.f, nil)
	s.age(t)
	state, err := s.b.OpStatus(s.ctx, s.op)
	if err != nil || state.Done || !strings.Contains(state.Message, "escalated removal") || fakeCalls(s.f, "application.stop") != 1 {
		t.Fatalf("legacy native-zero recovery: %+v %v", state, err)
	}
	awaitFake(t, s.b, s.op)
}

func TestDeleteEscalationRequiresExplicitReplicasOnReinspection(t *testing.T) {
	for _, missing := range []bool{true, false} {
		t.Run(map[bool]string{true: "missing", false: "null"}[missing], func(t *testing.T) {
			s := stuckDelete(t, false)
			s.age(t)
			s.f.mu.Lock()
			s.f.requestHook = func(_ http.ResponseWriter, r *http.Request) (bool, func()) {
				if r.URL.Path == "/api/deployment.queueList" {
					s.f.mu.Lock()
					replicated := map[string]any{}
					if !missing {
						replicated["Replicas"] = nil
					}
					s.f.services[s.name]["Spec"].(map[string]any)["Mode"] = map[string]any{"Replicated": replicated}
					s.f.mu.Unlock()
				}
				return false, nil
			}
			s.f.mu.Unlock()
			state, err := s.b.OpStatus(s.ctx, s.op)
			if err != nil || state.Done || fakeCalls(s.f, "application.delete") != 0 || !strings.Contains(state.Message, "inspection changed or is stale") {
				t.Fatalf("incomplete second inspection authorized removal: %+v %v", state, err)
			}
		})
	}
}
