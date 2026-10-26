// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package pbm

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/infra"
)

type routerRetryClock struct{ at time.Time }

func (c *routerRetryClock) Now() time.Time { return c.at }

type retryRouterBackend struct {
	infra.Backend
	state          string
	failure        string
	failAction     string
	alreadyReached bool
	alwaysFail     bool
	attempts       map[string]int
	processes      map[string]string
	polls          int
	observations   int
}

func (b *retryRouterBackend) issue(action string) (infra.Op, error) {
	b.attempts[action]++
	id := fmt.Sprintf("%s-%d", action, b.attempts[action])
	b.processes[id] = action
	return infra.Op{ID: id}, nil
}
func (b *retryRouterBackend) Stop(context.Context, string) (infra.Op, error) {
	return b.issue("stop")
}
func (b *retryRouterBackend) Start(context.Context, string) (infra.Op, error) {
	return b.issue("start")
}
func (b *retryRouterBackend) reached(action string) {
	b.state = infra.StatusActive
	if action == "stop" {
		b.state = infra.StatusStopped
	}
}
func (b *retryRouterBackend) OpStatus(_ context.Context, op infra.Op) (infra.OpState, error) {
	b.polls++
	action := b.processes[op.ID]
	if action == b.failAction && (op.ID == action+"-1" || b.alwaysFail) {
		if b.alreadyReached {
			b.reached(action)
		}
		return infra.OpState{Done: true, Failed: true, Message: b.failure}, nil
	}
	b.reached(action)
	return infra.OpState{Done: true}, nil
}
func (b *retryRouterBackend) GetService(context.Context, string) (*infra.Service, error) {
	b.observations++
	return &infra.Service{Hostname: "routers", Status: b.state}, nil
}

func newRetryRouterBackend(action, failure string, alreadyReached bool) *retryRouterBackend {
	return &retryRouterBackend{state: infra.StatusActive, failAction: action, failure: failure,
		alreadyReached: alreadyReached, attempts: map[string]int{}, processes: map[string]string{}}
}

// Round-trip both durable state objects into fresh allocations after each call.
func resumeRouterRestore(t *testing.T, env *Env, op **domain.Operation, checkpoint json.RawMessage) {
	t.Helper()
	(*op).Checkpoint = checkpoint
	raw, err := json.Marshal(*op)
	if err != nil {
		t.Fatal(err)
	}
	var resumed domain.Operation
	if err := json.Unmarshal(raw, &resumed); err != nil {
		t.Fatal(err)
	}
	*op = &resumed
	raw, err = json.Marshal(env.RC.Cluster)
	if err != nil {
		t.Fatal(err)
	}
	var cluster domain.Cluster
	if err := json.Unmarshal(raw, &cluster); err != nil {
		t.Fatal(err)
	}
	// Internal is stored separately and intentionally excluded from API JSON.
	cluster.Internal = append(json.RawMessage(nil), env.RC.Cluster.Internal...)
	env.RC.Cluster = &cluster
}

func TestRestoreRouterFailedProcessRetryCrashResume(t *testing.T) {
	for _, failure := range []string{"FAILED", "CANCELLED", "CANCELED"} {
		for _, action := range []string{"stop", "start"} {
			for _, reached := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/%s/alreadyReached=%t", failure, action, reached), func(t *testing.T) {
					env, st, agent, _ := setup(t)
					ctx := context.Background()
					env.Routers, env.Sharded = []string{"routers"}, true
					env.Nodes[0].ReplSet = "cfg"
					backend := newRetryRouterBackend(action, failure, reached)
					clock := &routerRetryClock{at: time.Date(2024, 1, 6, 12, 0, 0, 0, time.UTC)}
					env.RC.Backend, env.RC.Clock = backend, clock
					if err := st.CreateBackup(ctx, &domain.Backup{Name: "base", Cluster: "target", Storage: "storage", Type: "logical", Phase: domain.PhaseSucceeded, PBMName: "2024-01-05T11:10:54Z"}); err != nil {
						t.Fatal(err)
					}
					if err := st.CreateRestore(ctx, &domain.Restore{Name: "restore", Cluster: "target", Source: domain.RestoreSource{Backup: "base"}}); err != nil {
						t.Fatal(err)
					}
					op := &domain.Operation{ID: "restore", Payload: json.RawMessage(`{"restore":"restore"}`)}
					retried, done := false, false
					for range 100 {
						res, err := RunRestore(ctx, env, op)
						if err != nil {
							if retried || res.Done {
								t.Fatalf("unexpected restore error: %v", err)
							}
							// Lose the failure transition before persistence: a restart must
							// safely recover from the same terminal platform process.
							res, err = RunRestore(ctx, env, op)
							if err == nil {
								t.Fatal("terminal process failure not reported")
							}
							resumeRouterRestore(t, &env, &op, res.Checkpoint)
							cp, err := loadCheckpoint(op)
							if err != nil {
								t.Fatal(err)
							}
							if cp.RouterOp != nil || cp.RouterRetry == nil || cp.RouterRetry.Failures != 1 || cp.Index != 0 || !cp.RouterRetry.RetryAfter.Equal(clock.at.Add(30*time.Second)) {
								t.Fatalf("failure transition not durable: %s", res.Checkpoint)
							}
							polls := backend.polls
							clock.at = cp.RouterRetry.RetryAfter.Add(-time.Nanosecond)
							wait, err := RunRestore(ctx, env, op)
							if err == nil || wait.Done || backend.polls != polls || backend.attempts[action] != 1 || backend.observations != 0 {
								t.Fatal("retry ignored backoff or polled failed identity")
							}
							resumeRouterRestore(t, &env, &op, wait.Checkpoint)
							clock.at = cp.RouterRetry.RetryAfter
							res, err = RunRestore(ctx, env, op)
							if err != nil {
								t.Fatal(err)
							}
							resumeRouterRestore(t, &env, &op, res.Checkpoint)
							cp, _ = loadCheckpoint(op)
							if reached {
								if backend.attempts[action] != 1 || cp.Index != 1 || cp.RouterRetry != nil {
									t.Fatal("already-reached state was reissued or not completed")
								}
							} else if backend.attempts[action] != 2 || cp.Index != 0 || cp.RouterOp == nil || cp.RouterOp.ID != action+"-2" {
								t.Fatal("retry did not issue a new process or advanced prematurely")
							}
							retried = true
							continue
						}
						resumeRouterRestore(t, &env, &op, res.Checkpoint)
						if res.Done {
							done = true
							break
						}
					}
					r, err := st.GetRestore(ctx, domain.ClusterRef{Name: "target"}, "restore")
					starts := agent.startCount()
					if err != nil || !retried || !done || r.Phase != domain.PhaseSucceeded || backend.state != infra.StatusActive || starts != 1 {
						t.Fatalf("restore did not recover: restore=%+v done=%t retried=%t starts=%d error=%v", r, done, retried, starts, err)
					}
				})
			}
		}
	}
}

func TestRouterRetryLostActionCheckpointObservesCompletion(t *testing.T) {
	for _, action := range []string{"stop", "start"} {
		t.Run(action, func(t *testing.T) {
			env, _, _, _ := setup(t)
			env.Routers = []string{"routers"}
			backend := newRetryRouterBackend(action, "FAILED", false)
			backend.state = infra.StatusStopped
			if action == "stop" {
				backend.state = infra.StatusActive
			}
			clock := &routerRetryClock{at: time.Date(2024, 1, 6, 12, 0, 0, 0, time.UTC)}
			env.RC.Backend, env.RC.Clock = backend, clock
			cp := checkpoint{Step: "routers-on", RouterRetry: &routerRetry{Failures: 1, RetryAfter: clock.at, Message: "FAILED"}}
			// The safe retry reaches Zerops, but the control plane crashes before
			// persisting its new process identity.
			res, err := routerService(context.Background(), env, &cp, action)
			if err != nil || res.Done || backend.attempts[action] != 1 {
				t.Fatalf("retry not issued: %v", err)
			}
			backend.reached(action)
			var resumed checkpoint
			if err := json.Unmarshal(result(checkpoint{Step: "routers-on", RouterRetry: &routerRetry{Failures: 1, RetryAfter: clock.at}}, false).Checkpoint, &resumed); err != nil {
				t.Fatal(err)
			}
			if _, err := routerService(context.Background(), env, &resumed, action); err != nil {
				t.Fatal(err)
			}
			if backend.attempts[action] != 1 || resumed.Index != 1 || resumed.RouterOp != nil || resumed.RouterRetry != nil {
				t.Fatal("crash resume reissued an already-completed action")
			}
		})
	}
}

func TestRouterRetryBackoffEscalatesAndCaps(t *testing.T) {
	env, _, _, _ := setup(t)
	env.Routers = []string{"routers"}
	backend := newRetryRouterBackend("start", "CANCELLED", false)
	backend.state, backend.alwaysFail = infra.StatusStopped, true
	clock := &routerRetryClock{at: time.Date(2024, 1, 6, 12, 0, 0, 0, time.UTC)}
	env.RC.Backend, env.RC.Clock = backend, clock
	cp := checkpoint{Step: "routers-on"}
	delay := 30 * time.Second
	for attempt := 1; attempt <= 12; attempt++ {
		if _, err := routerService(context.Background(), env, &cp, "start"); err != nil {
			t.Fatal(err)
		}
		res, err := routerService(context.Background(), env, &cp, "start")
		if err == nil || res.Done || cp.Index != 0 || cp.RouterOp != nil || cp.RouterRetry.Failures != attempt || !cp.RouterRetry.RetryAfter.Equal(clock.at.Add(delay)) {
			t.Fatalf("wrong retry transition: checkpoint=%s error=%v", res.Checkpoint, err)
		}
		if err := json.Unmarshal(res.Checkpoint, &cp); err != nil {
			t.Fatal(err)
		}
		clock.at = cp.RouterRetry.RetryAfter
		delay = min(delay*2, 10*time.Minute)
	}
}
