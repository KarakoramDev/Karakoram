// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package pbm

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
)

const restoreAuthError = `connect to mongodb: unable to authenticate using mechanism "SCRAM-SHA-256": (AuthenticationFailed) Authentication failed.`

func TestRestoreAuthenticationFailureTerminatesWithoutNativeReplay(t *testing.T) {
	for _, mode := range []string{"snapshot", "pitr", "failure-cleanup"} {
		t.Run(mode, func(t *testing.T) {
			env, agent, clock, op := restoreStartupSetup(t, false)
			agent.restoreDescribeError = restoreAuthError
			if mode == "pitr" {
				cp, _ := loadCheckpoint(op)
				cp.TargetTime = &clock.at
				op.Checkpoint, _ = json.Marshal(cp)
			}
			if mode == "failure-cleanup" {
				op.Step, op.LastError = "failure-cleanup", "original restore failure"
				if _, err := PrepareCancellation(op); err != nil {
					t.Fatal(err)
				}
			}
			res, err := RunRestore(t.Context(), env, op)
			if !res.Done || err == nil || !strings.Contains(err.Error(), restoreAuthError) {
				t.Fatal("authentication failure did not terminate truthfully", res, err)
			}
			op.Checkpoint = res.Checkpoint
			cp, _ := loadCheckpoint(op)
			starts := agent.startCount()
			if cp.Step != "done" || !cp.RestoreConsistencyUncertain || cp.FreshBase || starts != 1 {
				t.Fatal("lost consistency fence or replayed restore", cp, starts)
			}
			r, err := env.RC.Store.GetRestore(t.Context(), env.RC.Cluster.Ref(), "clone")
			if err != nil || r.Phase != domain.PhaseFailed || r.CompletedAt == nil || !strings.Contains(r.Error, "fresh name") {
				t.Fatal("missing actionable failed restore", r, err)
			}
			res, err = RunRestore(t.Context(), env, op)
			starts = agent.startCount()
			if !res.Done || err == nil || starts != 1 {
				t.Fatal("restart lost terminal failure", res, err)
			}
		})
	}
}

type isolatedIdentityHooks struct {
	fakeHooks
	agent              *fakeAgent
	ready              bool
	prepared, released int
}

func (h *isolatedIdentityHooks) PreparePITRIdentity(context.Context, string) (bool, error) {
	if h.agent.startCount() != 0 {
		panic("identity prepared after native restore started")
	}
	h.prepared++
	return h.ready, nil
}
func (h *isolatedIdentityHooks) FinishPITRIdentity(context.Context, string) (bool, error) {
	if len(h.reset) == 0 {
		panic("identity removed before target users were reset")
	}
	h.released++
	return true, nil
}

func TestPITRRestorePreparesIsolatedIdentityBeforeNativeStart(t *testing.T) {
	env, agent, clock, op := prepareRestoreStartup(t, false)
	cp, _ := loadCheckpoint(op)
	cp.TargetTime, cp.Step = &clock.at, "maintenance-on"
	op.Checkpoint, _ = json.Marshal(cp)
	hooks := &isolatedIdentityHooks{agent: agent}
	env.Hooks = hooks
	res, err := RunRestore(t.Context(), env, op)
	starts := agent.startCount()
	if err != nil || res.Step != "prepare-pitr-identity" || starts != 0 {
		t.Fatal(res, err, starts)
	}
	op.Checkpoint = res.Checkpoint
	res, err = RunRestore(t.Context(), env, op)
	starts = agent.startCount()
	if err != nil || res.Step != "prepare-pitr-identity" || starts != 0 {
		t.Fatal("native work escaped identity preparation", res, err)
	}
	op.Checkpoint = res.Checkpoint
	hooks.ready = true
	for range 30 {
		res, err = RunRestore(t.Context(), env, op)
		op.Checkpoint = res.Checkpoint
		if err != nil {
			t.Fatal(err)
		}
		if res.Done {
			break
		}
	}
	starts = agent.startCount()
	if !res.Done || starts != 1 || hooks.prepared < 2 || hooks.released != 1 {
		t.Fatal("isolated identity lifecycle incomplete", res, starts, hooks.prepared, hooks.released)
	}
}

func TestPITRIdentityPreparationCancellationNeverStartsNativeRestore(t *testing.T) {
	env, agent, clock, op := prepareRestoreStartup(t, false)
	cp, _ := loadCheckpoint(op)
	cp.TargetTime, cp.Step = &clock.at, "prepare-pitr-identity"
	op.Checkpoint, _ = json.Marshal(cp)
	op.Step, op.LastError = "failure-cleanup", "identity preparation failed"
	if _, err := PrepareCancellation(op); err != nil {
		t.Fatal(err)
	}
	drive(t, env, op, RunRestore, true)
	starts := agent.startCount()
	if starts != 0 {
		t.Fatal("cleanup initiated native restore", starts)
	}
}

func TestRestoreCleanupDeadlineSurvivesControllerRestart(t *testing.T) {
	env, agent, clock, op := restoreStartupSetup(t, false)
	op.Step, op.LastError = "failure-cleanup", restoreAuthError
	started := clock.at.Add(-time.Minute)
	op.FailureCleanupStartedAt = &started
	if _, err := PrepareCancellation(op); err != nil {
		t.Fatal(err)
	}
	agent.restoreRunning = true
	res, err := RunRestore(t.Context(), env, op)
	if !res.Done || err == nil || !strings.Contains(err.Error(), restoreAuthError) || !strings.Contains(err.Error(), "one-minute deadline") {
		t.Fatal("resumed cleanup did not terminate within its durable bound", res, err)
	}
	cp, _ := loadCheckpoint(&domain.Operation{Checkpoint: res.Checkpoint})
	starts := agent.startCount()
	polling := agent.restorePollingCount()
	if !cp.RestoreConsistencyUncertain || starts != 1 || polling != 0 {
		t.Fatal("expired cleanup polled or replayed native work", cp, starts, polling)
	}
}

func TestRestoreSASLTransportFailureRetainsRetryAndNativeIdentity(t *testing.T) {
	for _, kind := range []string{"snapshot", "physical", "pitr"} {
		for _, transport := range []string{"EOF", "read: connection reset by peer", "context deadline exceeded"} {
			t.Run(kind+"/"+transport, func(t *testing.T) {
				env, agent, clock, op := restoreStartupSetup(t, kind == "physical")
				cp, err := loadCheckpoint(op)
				if err != nil {
					t.Fatal(err)
				}
				if kind == "pitr" {
					cp.TargetTime = &clock.at
					op.Checkpoint, _ = json.Marshal(cp)
				}
				nativeName := cp.PBMName
				agent.restoreDescribeError = `connect to mongodb: auth error: unable to authenticate using mechanism "SCRAM-SHA-256": ` + transport
				res, err := RunRestore(t.Context(), env, op)
				if err == nil || res.Done || res.Step != "poll" || !strings.Contains(err.Error(), transport) {
					t.Fatal("SASL transport failure left the normal retry path", res, err)
				}
				cp, err = loadCheckpoint(&domain.Operation{Checkpoint: res.Checkpoint})
				starts := agent.startCount()
				if err != nil || cp.PBMName != nativeName || cp.RestoreConsistencyUncertain || cp.Failure != "" || starts != 1 {
				}
			})
		}
	}
}

func TestRestoreCompensationTransportFailureRetainsWorkerBackoffSignal(t *testing.T) {
	env, agent, clock, op := restoreStartupSetup(t, false)
	cp, _ := loadCheckpoint(op)
	cp.Step, cp.Failure, cp.RestoreConsistencyUncertain = "configure-target", "native restore failed", true
	op.Checkpoint, _ = json.Marshal(cp)
	op.Step, op.LastError = "failure-cleanup", cp.Failure
	agent.configError = `unable to authenticate using mechanism "SCRAM-SHA-256": EOF`
	res, err := RunRestore(t.Context(), env, op)
	if err == nil || res.Done || !strings.Contains(err.Error(), "EOF") {
		t.Fatal("transport compensation error became fast progress instead of a worker-backoff error", res, err)
	}
	op.Checkpoint = res.Checkpoint
	clock.at = clock.at.Add(time.Minute)
	res, err = RunRestore(t.Context(), env, op)
	starts := agent.startCount()
	if err == nil || !res.Done || !strings.Contains(err.Error(), "native restore failed") || starts != 1 {
		t.Fatal("compensation did not respect its durable deadline", res, err, starts)
	}
}

func TestLegacyPITRStartDoesNotChangeIdentityOrReplayNativeReceipt(t *testing.T) {
	env, agent, clock, op := prepareRestoreStartup(t, false)
	cp, _ := loadCheckpoint(op)
	cp.TargetTime = &clock.at
	op.Checkpoint, _ = json.Marshal(cp)
	hooks := &isolatedIdentityHooks{agent: agent, ready: true}
	env.Hooks = hooks
	res, err := RunRestore(t.Context(), env, op)
	starts := agent.startCount()
	if !res.Done || err == nil || !strings.Contains(err.Error(), "legacy PITR initiation outcome is unknown") || starts != 0 || hooks.prepared != 0 {
		t.Fatal("legacy uncertain initiation changed credentials or submitted native work", res, err, starts, hooks.prepared)
	}
}
