// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package dokploy

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/KarakoramDev/Karakoram/internal/infra"
)

const startupEnvFixture = "startup-env-fixture"

// The wall clock never advances implicitly; these tests do not sleep for the
// startup bound or depend on the time taken by deployment rendering.
type startupFixture struct {
	api     *fakeAPI
	backend *Backend
	op      infra.Op
	clock   time.Time
	issued  time.Time
	anchor  time.Time
	appID   string
}

func newStartupFixture(t *testing.T, action string, replicas int) *startupFixture {
	t.Helper()
	s := &startupFixture{api: newFakeAPI(t), clock: time.Date(2026, 10, 16, 12, 0, 0, 0, time.UTC)}
	s.backend = fakeBackend(t, s.api, nil)
	s.backend.now = func() time.Time { return s.clock }
	spec := runtimeSpec("runtime", "")
	spec.MaxContainers = replicas
	spec.Env = map[string]string{"PRIVATE_VALUE": startupEnvFixture}
	imported := importFake(t, s.backend, spec)
	s.appID = imported.Services["runtime"]
	deployFake(t, s.backend, "runtime", "")
	var err error
	switch action {
	case "deploy":
		s.op, err = s.backend.Deploy(t.Context(), "runtime", testDeployment(t, ""))
	case "restart":
		s.op, err = s.backend.Restart(t.Context(), "runtime")
	case "start":
		op, e := s.backend.Stop(t.Context(), "runtime")
		if e != nil {
			t.Fatal(e)
		}
		awaitFake(t, s.backend, op)
		s.op, err = s.backend.Start(t.Context(), "runtime")
	default:
		t.Fatalf("unsupported startup test action %q", action)
	}
	if err != nil {
		t.Fatal(err)
	}
	handle, err := decodeOp(s.op)
	if err != nil {
		t.Fatal(err)
	}
	s.issued = handle.Issued
	if !s.issued.Equal(s.clock) {
		t.Fatalf("operation did not use injected clock: issued=%s clock=%s", s.issued, s.clock)
	}
	s.anchor = s.issued
	s.api.mu.Lock()
	a := s.api.apps[s.appID]
	if action == "deploy" {
		// A long successful build must not consume the task startup allowance.
		s.anchor = s.issued.Add(45 * time.Minute)
		a.deployments[0].status = "done"
		a.deployments[0].finishedAt = s.anchor.Format(time.RFC3339Nano)
		s.api.apply(a)
	}
	a.holdTasks = true
	a.holdDeployments = true
	for _, task := range a.tasks {
		if task["state"] == "running" {
			task["currentState"] = "Starting 1 second ago"
		}
	}
	s.api.mu.Unlock()
	return s
}

func (s *startupFixture) setTasks(t *testing.T, desired, current, detail string) {
	t.Helper()
	handle, err := decodeOp(s.op)
	if err != nil {
		t.Fatal(err)
	}
	s.api.mu.Lock()
	defer s.api.mu.Unlock()
	for _, task := range s.api.apps[s.appID].tasks {
		if contains(handle.PreviousTasks, fakeString(task, "containerId")) {
			continue
		}
		task["state"] = desired
		task["currentState"] = current
		task["error"] = detail
	}
}

func TestStartupBoundFailsNonReadyTasksAfterFifteenMinutes(t *testing.T) {
	for _, action := range []string{"deploy", "start", "restart"} {
		t.Run(action, func(t *testing.T) {
			for _, scenario := range []struct{ name, desired, current, detail string }{
				{"stuckStarting", "running", "Starting 15 minutes ago", ""},
				{"failedCrash", "shutdown", "Failed 15 minutes ago", `task: non-zero exit (42)`},
				{"noSuitableNode", "running", "Pending 15 minutes ago", "no suitable node (scheduling constraints not satisfied)"},
			} {
				t.Run(scenario.name, func(t *testing.T) {
					s := newStartupFixture(t, action, 1)
					s.setTasks(t, scenario.desired, scenario.current, scenario.detail)
					s.clock = s.anchor.Add(15*time.Minute - time.Nanosecond)
					state, err := s.backend.OpStatus(t.Context(), s.op)
					if err != nil || state.Done || state.Failed {
						t.Fatalf("startup just below15 minutes must remain pending: %+v %v", state, err)
					}
					s.clock = s.anchor.Add(15*time.Minute + time.Nanosecond)
					state, err = s.backend.OpStatus(t.Context(), s.op)
					if err != nil || !state.Failed || state.Done || state.Expired || state.Message == "" {
						t.Fatalf("non-ready startup past15 minutes: %+v %v", state, err)
					}
				})
			}
		})
	}
}

func TestStartupDeployDeadlineUsesCorrelatedFinishedAt(t *testing.T) {
	s := newStartupFixture(t, "deploy", 1)
	// The operation is already45 minutes old at build completion. Startup still
	// receives15 full minutes from finishedAt, not from submission or createdAt.
	s.clock = s.anchor.Add(time.Minute)
	state, err := s.backend.OpStatus(t.Context(), s.op)
	if err != nil || state.Done || state.Failed {
		t.Fatalf("long build consumed startup allowance: %+v %v", state, err)
	}
	// Simulate the deployment record gaining a later actual completion timestamp.
	later := s.anchor.Add(time.Hour)
	s.api.mu.Lock()
	s.api.apps[s.appID].deployments[0].finishedAt = later.Format(time.RFC3339Nano)
	s.api.mu.Unlock()
	s.clock = later.Add(15*time.Minute - time.Nanosecond)
	state, err = s.backend.OpStatus(t.Context(), s.op)
	if err != nil || state.Done || state.Failed {
		t.Fatalf("deadline did not follow correlated completion record: %+v %v", state, err)
	}
	s.clock = later.Add(15*time.Minute + time.Nanosecond)
	state, err = s.backend.OpStatus(t.Context(), s.op)
	if err != nil || !state.Failed || state.Done {
		t.Fatalf("later completion startup deadline not enforced: %+v %v", state, err)
	}
}

func TestStartupQueuedAndRunningBuildsDoNotUseIssueDeadline(t *testing.T) {
	for _, build := range []string{"queued", "running", "onlyPreviousDone"} {
		t.Run(build, func(t *testing.T) {
			s := newStartupFixture(t, "deploy", 1)
			s.api.mu.Lock()
			a := s.api.apps[s.appID]
			switch build {
			case "queued":
				a.deployments = nil
			case "running":
				a.deployments[0].status = "running"
				a.deployments[0].finishedAt = ""
			case "onlyPreviousDone":
				a.deployments = a.deployments[1:]
				for _, d := range a.deployments {
					d.status = "done"
					d.finishedAt = s.issued.Add(-24 * time.Hour).Format(time.RFC3339Nano)
				}
			}
			a.tasks = nil
			s.api.mu.Unlock()
			s.clock = s.issued.Add(24 * time.Hour)
			state, err := s.backend.OpStatus(t.Context(), roundTripOp(t, s.op))
			if err != nil || state.Done || state.Failed {
				t.Fatalf("%s build cannot start a task deadline: %+v %v", build, state, err)
			}
		})
	}
}

func TestStartupRequiresAllNewReplicasAndKeepsReadySuccess(t *testing.T) {
	for _, action := range []string{"deploy", "start", "restart"} {
		t.Run(action, func(t *testing.T) {
			for _, allReady := range []bool{false, true} {
				t.Run(map[bool]string{false: "partial", true: "ready"}[allReady], func(t *testing.T) {
					s := newStartupFixture(t, action, 3)
					handle, err := decodeOp(s.op)
					if err != nil {
						t.Fatal(err)
					}
					s.api.mu.Lock()
					ready := 0
					for _, task := range s.api.apps[s.appID].tasks {
						if contains(handle.PreviousTasks, fakeString(task, "containerId")) {
							continue
						}
						if allReady || ready == 0 {
							task["state"] = "running"
							task["currentState"] = "Running 1 second ago"
						} else {
							task["state"] = "running"
							task["currentState"] = "Starting 1 second ago"
						}
						ready++
					}
					s.api.mu.Unlock()
					if ready != 3 {
						t.Fatalf("fixture has %d new replicas, want3", ready)
					}
					s.clock = s.anchor.Add(15*time.Minute - time.Nanosecond)
					state, err := s.backend.OpStatus(t.Context(), s.op)
					if err != nil || state.Done != allReady || state.Failed {
						t.Fatalf("readiness below deadline: %+v %v", state, err)
					}
					s.clock = s.anchor.Add(15*time.Minute + time.Nanosecond)
					state, err = s.backend.OpStatus(t.Context(), s.op)
					if err != nil || state.Done != allReady || state.Failed == allReady {
						t.Fatalf("readiness above deadline: %+v %v", state, err)
					}
				})
			}
		})
	}
}

func TestStartupTaskDiagnosticsAreBoundedAndCredentialSilent(t *testing.T) {
	s := newStartupFixture(t, "deploy", 1)
	private := fakeAPIKey + " " + startupEnvFixture + " https://private.example.test/path?token=" + fakeAPIKey
	s.setTasks(t, "shutdown", "Failed "+private, private+strings.Repeat(" diagnostic\n\x1b[31m", 1000))
	s.clock = s.anchor.Add(16 * time.Minute)
	state, err := s.backend.OpStatus(t.Context(), s.op)
	if err != nil || !state.Failed || state.Done {
		t.Fatalf("failed startup: %+v %v", state, err)
	}
	if state.Message == "" || len(state.Message) > 2048 || !utf8.ValidString(state.Message) {
		t.Fatalf("startup diagnostic is empty, unbounded or invalid UTF-8 (bytes=%d)", len(state.Message))
	}
	wire, marshalErr := json.Marshal(state)
	if marshalErr != nil {
		t.Fatal(marshalErr)
	}
	for _, value := range []string{fakeAPIKey, startupEnvFixture, "https://private.example.test", "\x1b[31m"} {
		if strings.Contains(string(wire), value) || strings.Contains(state.Message, value) {
			t.Fatal("startup state exposed raw task credentials/control output")
		}
	}
}

func TestStartupFakeDeploymentFinishedAtWireShape(t *testing.T) {
	f := newFakeAPI(t)
	b := fakeBackend(t, f, nil)
	imported := importFake(t, b, runtimeSpec("runtime", ""))
	op, err := b.Deploy(t.Context(), "runtime", testDeployment(t, ""))
	if err != nil {
		t.Fatal(err)
	}
	f.mu.Lock()
	a := f.apps[imported.Services["runtime"]]
	a.holdDeployments = true
	f.mu.Unlock()
	var rows []map[string]any
	query := map[string]string{"applicationId": imported.Services["runtime"]}
	if err = b.c.get(t.Context(), "deployment.all", query, &rows); err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || rows[0]["status"] != "running" || rows[0]["finishedAt"] != nil {
		t.Fatalf("running deployment wire shape: %+v", rows)
	}
	f.mu.Lock()
	a.holdDeployments = false
	f.mu.Unlock()
	awaitFake(t, b, op)
	if err = b.c.get(t.Context(), "deployment.all", query, &rows); err != nil {
		t.Fatal(err)
	}
	finished, err := time.Parse(time.RFC3339Nano, fakeString(rows[0], "finishedAt"))
	if err != nil || finished.IsZero() {
		t.Fatalf("done deployment missing finishedAt: %+v %v", rows, err)
	}
	f.mu.Lock()
	a.deployments[0].status = "error"
	a.deployments[0].finishedAt = ""
	f.mu.Unlock()
	if err = b.c.get(t.Context(), "deployment.all", query, &rows); err != nil {
		t.Fatal(err)
	}
	if _, err = time.Parse(time.RFC3339Nano, fakeString(rows[0], "finishedAt")); err != nil {
		t.Fatalf("error deployment missing finishedAt: %+v", rows)
	}
}

func TestStartupMalformedCompletionTimestampDoesNotUseIssueTime(t *testing.T) {
	for _, missing := range []bool{false, true} {
		t.Run(map[bool]string{false: "malformed", true: "missing"}[missing], func(t *testing.T) {
			for _, ready := range []bool{false, true} {
				t.Run(map[bool]string{false: "notReady", true: "allReady"}[ready], func(t *testing.T) {
					s := newStartupFixture(t, "deploy", 1)
					s.api.mu.Lock()
					d := s.api.apps[s.appID].deployments[0]
					d.finishedAt = "invalid-timestamp " + fakeAPIKey + " " + startupEnvFixture
					d.omitFinishedAt = missing
					s.api.mu.Unlock()
					if ready {
						s.setTasks(t, "running", "Running 1 second ago", "")
					}
					s.clock = s.issued.Add(24 * time.Hour)
					state, err := s.backend.OpStatus(t.Context(), s.op)
					if ready {
						if err != nil || !state.Done || state.Failed {
							t.Fatalf("all-ready deployment should not need finish timestamp: %+v %v", state, err)
						}
						return
					}
					if !errors.Is(err, infra.ErrUnavailable) || state.Done {
						t.Fatalf("corrupt completion timestamp must report unavailable, not guessed issue deadline: %+v %v", state, err)
					}
					for _, value := range []string{fakeAPIKey, startupEnvFixture} {
						if strings.Contains(err.Error(), value) || strings.Contains(state.Message, value) {
							t.Fatal("corrupt completion error exposed private timestamp contents")
						}
					}
				})
			}
		})
	}
}

func TestStartupDiagnosticsPreservePublicVariablesAndRedactSecrets(t *testing.T) {
	f := newFakeAPI(t)
	b := fakeBackend(t, f, nil)
	const sensitive = "private-fixture-value"
	const unclassified = "unclassified-fixture-value"
	env, err := encodeEnvironment(map[string]string{"PUBLIC": "a", "PRIVATE": sensitive, "UNCLASSIFIED": unclassified})
	if err != nil {
		t.Fatal(err)
	}
	m := metadata{Kind: infra.KindRuntime, Hostname: "runtime", VarKeys: []string{"PUBLIC"}, SecretKeys: []string{"PRIVATE"}}
	a := application{Description: m.description(), Env: env}
	message := b.startupFailure(a, operation{}, []task{{ID: "new", State: "running", CurrentState: "Starting 1 minute ago", Error: "exit denied: " + sensitive + " " + unclassified}}, 0, 1)
	if !strings.Contains(message, "Starting 1 minute ago") {
		t.Fatalf("short readable variables must not corrupt task-state diagnostics: %s", message)
	}
	for _, value := range []string{sensitive, unclassified, fakeAPIKey} {
		if strings.Contains(message, value) {
			t.Fatal("startup diagnostic exposed a credential")
		}
	}
}
