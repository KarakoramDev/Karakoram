// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package zerops

import (
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/infra"
)

func TestServingAutoscalingTerminalFailureReleasesSubmissionFence(t *testing.T) {
	for _, replaced := range []bool{false, true} {
		name := "failed-process"
		if replaced {
			name = "replaced-incumbents"
		}
		t.Run(name, func(t *testing.T) {
			server, f := newFakeAPIWithState(t)
			backend := func() *Backend {
				b, err := New(Options{BaseURL: server.URL, Token: "token", ProjectID: "project", MaxRetries: -1, PollInterval: time.Millisecond, WaitTimeout: time.Second})
				if err != nil {
					t.Fatal(err)
				}
				return b
			}
			b := backend()
			var service ServiceStack
			if err := json.Unmarshal(fixture(t, "service"), &service); err != nil {
				t.Fatal(err)
			}
			service.ID, service.Name, service.Base, service.Status, service.ProjectID = "proxy-id", "proxy", "ubuntu@24.04", "ACTIVE", "project"
			putJSON(service.CurrentAutoscaling, "horizontalAutoscaling", map[string]int{"minContainerCount": 1, "maxContainerCount": 1})
			f.services[service.ID] = &service
			f.replaceContainers = replaced
			op, err := b.SetContainerCount(t.Context(), "proxy", 2, 2)
			if err != nil {
				t.Fatal(err)
			}
			if !replaced {
				f.mu.Lock()
				for id, p := range f.processes {
					if p.ActionName == "autoscaling" {
						p.Status = "FAILED"
						f.processes[id] = p
					}
				}
				f.mu.Unlock()
			}
			b = backend()
			// Before observing terminal failure, lost-checkpoint replay adopts the
			// original operation without another PUT.
			replay, err := b.SetContainerCount(t.Context(), "proxy", 2, 2)
			if err != nil {
				t.Fatal(err)
			}
			if replay.ID != op.ID || f.autoscalingWrites != 1 {
				t.Fatalf("unobserved failed target retried: %+v writes=%d", replay, f.autoscalingWrites)
			}
			status, err := b.OpStatus(t.Context(), op)
			if err != nil || !status.Done || !status.Failed {
				t.Fatalf("terminal failure: %+v %v", status, err)
			}
			observed, err := backend().OpStatus(t.Context(), op)
			if err != nil || observed.Status != status.Status || observed.Message != status.Message || !observed.Failed {
				t.Fatalf("failure evidence lost: %+v %v", observed, err)
			}
			if replaced {
				f.replaceContainers = false
				superseding, err := b.SetContainerCount(t.Context(), "proxy", 3, 3)
				if err != nil {
					t.Fatalf("superseding target rejected: %v", err)
				}
				state, err := b.OpStatus(t.Context(), superseding)
				if err != nil || !state.Done || state.Failed {
					t.Fatalf("superseding target: %+v %v", state, err)
				}
			} else {
				for attempt := range 3 {
					retry, err := backend().SetContainerCount(t.Context(), "proxy", 2, 2)
					if err != nil || retry.ID == op.ID || f.autoscalingWrites != attempt+2 {
						t.Fatalf("observed failure could not retry once: %+v writes=%d err=%v", retry, f.autoscalingWrites, err)
					}
					f.mu.Lock()
					for id, process := range f.processes {
						if process.ActionName == "autoscaling" {
							process.Status = "FAILED"
							f.processes[id] = process
						}
					}
					f.mu.Unlock()
					state, err := b.OpStatus(t.Context(), retry)
					if err != nil || !state.Failed {
						t.Fatalf("retry failure: %+v %v", state, err)
					}
					failures, _, err := b.autoscalingFailures(t.Context(), service.ID)
					if err != nil || len(failures) != 1 {
						t.Fatalf("unbounded evidence: %d %v", len(failures), err)
					}
				}
			}
			stop, err := b.Stop(t.Context(), "proxy")
			if err != nil {
				t.Fatalf("recovery stop rejected: %v", err)
			}
			stopped, err := b.OpStatus(t.Context(), stop)
			if err != nil || !stopped.Done || stopped.Failed {
				t.Fatalf("recovery stop: %+v %v", stopped, err)
			}
			failures, _, err := b.autoscalingFailures(t.Context(), service.ID)
			if err != nil || len(failures) != 0 {
				t.Fatalf("successful recovery did not clear failure evidence: %v %v", failures, err)
			}
		})
	}
}

func TestServingAutoscalingUnresolvedSubmissionKeepsFence(t *testing.T) {
	server, f := newFakeAPIWithState(t)
	b, err := New(Options{BaseURL: server.URL, Token: "token", ProjectID: "project", MaxRetries: -1, PollInterval: time.Millisecond, WaitTimeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	var service ServiceStack
	if err = json.Unmarshal(fixture(t, "service"), &service); err != nil {
		t.Fatal(err)
	}
	service.ID, service.Name, service.Base, service.Status, service.ProjectID = "proxy-id", "proxy", "ubuntu@24.04", "ACTIVE", "project"
	putJSON(service.CurrentAutoscaling, "horizontalAutoscaling", map[string]int{"minContainerCount": 1, "maxContainerCount": 1})
	f.services[service.ID] = &service
	f.autoscalingPending = true
	op, err := b.SetContainerCount(t.Context(), "proxy", 2, 2)
	if err != nil {
		t.Fatal(err)
	}
	status, err := b.OpStatus(t.Context(), op)
	if err != nil || status.Done {
		t.Fatalf("unresolved receipt: %+v %v", status, err)
	}
	if _, err = b.SetContainerCount(t.Context(), "proxy", 3, 3); !errors.Is(err, infra.ErrConflict) {
		t.Fatalf("unresolved supersede not fenced: %v", err)
	}
	if _, err = b.SetResources(t.Context(), "proxy", infra.Resources{CPUMode: "DEDICATED"}); !errors.Is(err, infra.ErrConflict) {
		t.Fatalf("unresolved disruptive resource update not fenced: %v", err)
	}
	if _, err = b.Stop(t.Context(), "proxy"); !errors.Is(err, infra.ErrConflict) {
		t.Fatalf("unresolved stop not fenced: %v", err)
	}
	if f.autoscalingWrites != 1 {
		t.Fatalf("unresolved update resubmitted %d times", f.autoscalingWrites)
	}
}
