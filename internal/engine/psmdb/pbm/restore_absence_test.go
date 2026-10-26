// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package pbm

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
)

type absenceHooks struct {
	*fakeHooks
	absent    bool
	err       error
	calls     int
	commandTS int64
}

func (h *absenceHooks) ObserveRestoreAbsence(_ context.Context, _ string, env Env) (int64, bool, error) {
	h.calls++
	if env.Prefix != "karakoram/source-incarnation" {
		return 0, false, errors.New("wrong recorded storage prefix")
	}
	if h.commandTS != 0 {
		return h.commandTS, h.absent, h.err
	}
	return 1792568288, h.absent, h.err
}

func prepareAbsentRestore(t *testing.T) (Env, *fakeAgent, *routerRetryClock, *domain.Operation, *absenceHooks) {
	t.Helper()
	env, agent, clock, op := restoreStartupSetup(t, false)
	proof := &absenceHooks{fakeHooks: env.Hooks.(*fakeHooks), absent: true}
	env.Hooks = proof
	agent.restoreDescribeError = missingRestoreMetadata
	agent.pbmFenceCapability = true
	return env, agent, clock, op, proof
}

func advanceRestoreToFence(t *testing.T, env Env, clock *routerRetryClock, op *domain.Operation) {
	t.Helper()
	first, err := RunRestore(t.Context(), env, op)
	if err != nil || first.Step != "poll" {
		t.Fatal(first, err)
	}
	op.Checkpoint = first.Checkpoint
	cp, _ := loadCheckpoint(op)
	if cp.RestoreAbsentSince == nil {
		t.Fatal("absence window not durably recorded")
	}
	clock.at = clock.at.Add(restoreMetadataGrace - time.Nanosecond)
	pending, _ := RunRestore(t.Context(), env, op)
	if pending.Step != "poll" {
		t.Fatal("fenced before full absence window", pending)
	}
	op.Checkpoint = pending.Checkpoint
	clock.at = clock.at.Add(time.Nanosecond)
	fenced, err := RunRestore(t.Context(), env, op)
	if err != nil || fenced.Step != "fence-pbm" {
		t.Fatal("absence bypassed active fencing", fenced, err)
	}
	op.Checkpoint = fenced.Checkpoint
}

func TestRestoreAgentKillBeforeNativeStartupFailsWithoutResubmission(t *testing.T) {
	env, agent, clock, op, proof := prepareAbsentRestore(t)
	original := env.RC.Agents
	env.RC.Agents = &survivorDialer{testDialer: original.(testDialer)}
	interrupted, _ := RunRestore(t.Context(), env, op)
	op.Checkpoint = interrupted.Checkpoint
	cp, _ := loadCheckpoint(op)
	if cp.RestoreAbsentSince != nil || interrupted.Step != "poll" {
		t.Fatal("unavailable initiating agent became absence proof", interrupted, cp)
	}
	// Original fixture sequence: the native task recovers, but its consumed
	// initiation has no metadata and the healthy survivors cannot finish it.
	env.RC.Agents = original
	advanceRestoreToFence(t, env, clock, op)
	if len(agent.recordedFences()) != 0 {
		t.Fatal("restart occurred before the durable fence step")
	}
	drive(t, env, op, RunRestore, true)
	restore, err := env.RC.Store.GetRestore(t.Context(), env.RC.Cluster.Ref(), "clone")
	if err != nil || restore.Phase != domain.PhaseFailed || !strings.Contains(restore.Error, "data unchanged; re-run the restore") {
		t.Fatal(restore, err)
	}
	starts := agent.startCount()
	fences := agent.recordedFences()
	if starts != 1 || len(fences) != len(env.Nodes) || proof.calls < 4 {
		t.Fatal("missing every-member fence/post-check or destructive resubmission", starts, len(fences), proof.calls)
	}
	for _, node := range env.Nodes {
		if _, ok := fences[op.ID+"/pbm-fence/"+node.Member]; !ok {
			t.Fatal("member never actively fenced", node.Member)
		}
	}
}

func TestRestoreHalfSecondAgentKillWithEmptyStorageDescribe(t *testing.T) {
	for _, observation := range []string{"absent", "storage-present", "storage-unavailable", "previously-observed", "head-forbidden-absent", "head-forbidden-list-denied"} {
		t.Run(observation, func(t *testing.T) {
			env, agent, clock, op, proof := prepareAbsentRestore(t)
			start := time.Date(2026, 10, 21, 12, 34, 54, 959019416, time.UTC)
			clock.at = start.Add(529 * time.Millisecond)
			cp, _ := loadCheckpoint(op)
			cp.PBMName = start.Format(time.RFC3339Nano)
			deadline := start.Add(restoreMetadataGrace)
			cp.RestoreMetadataDeadline = &deadline
			op.Checkpoint, _ = json.Marshal(cp)
			proof.commandTS = start.Unix()
			// PBM --config succeeds with this shell even when .pbm.restore
			// contains zero objects. Member describe still returns not-found.
			agent.restoreStorageOutput = json.RawMessage(`{"name":"2026-10-21T12:34:54.959019416Z","type":"physical","status":"","opid":"","last_transition_ts":0}`)
			if strings.HasPrefix(observation, "head-forbidden") {
				agent.restoreStorageError = "Error: get restore meta: get S3 object header: StatusCode: 403 Forbidden"
			}
			// The first observation precedes command/heartbeat aging.
			proof.absent = false
			pending, err := RunRestore(t.Context(), env, op)
			if (err != nil && !strings.HasPrefix(observation, "head-forbidden")) || pending.Step != "poll" || pending.Done {
				t.Fatal("pre-publication kill released startup holds", pending, err)
			}
			op.Checkpoint = pending.Checkpoint
			clock.at = start.Add(2 * time.Minute)
			proof.absent = true
			switch observation {
			case "storage-present":
				proof.absent = false
			case "storage-unavailable":
				proof.err = errors.New("incomplete storage listing")
			case "head-forbidden-list-denied":
				proof.err = errors.New("storage ListObjects denied")
			case "previously-observed":
				cp.RestoreMetadataDeadline = nil
				op.Checkpoint, _ = json.Marshal(cp)
			}
			if observation != "absent" && observation != "head-forbidden-absent" {
				for range 2 {
					held, _ := RunRestore(t.Context(), env, op)
					op.Checkpoint = held.Checkpoint
					checkpoint, _ := loadCheckpoint(op)
					fences := agent.recordedFences()
					if held.Step != "poll" || held.Done || checkpoint.RestoreAbsentSince != nil || len(fences) != 0 {
						t.Fatal("unknown storage or observed native outcome entered absence fencing", held, checkpoint)
					}
					clock.at = clock.at.Add(2 * time.Minute)
				}
				return
			}
			advanceRestoreToFence(t, env, clock, op)
			drive(t, env, op, RunRestore, true)
			restore, err := env.RC.Store.GetRestore(t.Context(), env.RC.Cluster.Ref(), "clone")
			starts := agent.startCount()
			fences := agent.recordedFences()
			if err != nil || restore.Phase != domain.PhaseFailed || !strings.Contains(restore.Error, "RestoreNeverStarted") ||
				starts != 1 || len(fences) != len(env.Nodes) {
				t.Fatal("storage shell bypassed fenced failure or replayed restore", restore, err, starts, fences)
			}
		})
	}
}

func TestRestoreActiveFenceRejectsUnconfirmedExitAndClockSkew(t *testing.T) {
	for _, scenario := range []string{"old-exit-unconfirmed", "member-clock-behind", "post-fence-outcome-unknown"} {
		t.Run(scenario, func(t *testing.T) {
			env, agent, clock, op, proof := prepareAbsentRestore(t)
			advanceRestoreToFence(t, env, clock, op)
			switch scenario {
			case "old-exit-unconfirmed":
				agent.fenceNoExit = true
			case "member-clock-behind":
				agent.fenceClockBehind = true
			}
			if scenario != "post-fence-outcome-unknown" {
				held, err := RunRestore(t.Context(), env, op)
				cp := checkpoint{}
				_ = json.Unmarshal(held.Checkpoint, &cp)
				if err == nil || held.Step != "fence-pbm" || cp.Index != 0 || cp.Failure != "" || held.Done {
					t.Fatal("uncertain restart released fence", held, err)
				}
				if !cp.RestoreFenceUnknown {
					t.Fatal("uncertain restart was not durably fenced")
				}
				op.Checkpoint = held.Checkpoint
				agent.fenceNoExit, agent.fenceClockBehind = false, false
				retried, retryErr := RunRestore(t.Context(), env, op)
				if retryErr == nil || retried.Step != "fence-pbm" || retried.Done {
					t.Fatal("later healthy observation revived an unknown restart", retried, retryErr)
				}
			} else {
				for range len(env.Nodes) + 1 {
					r, err := RunRestore(t.Context(), env, op)
					if err != nil {
						t.Fatal(err)
					}
					op.Checkpoint = r.Checkpoint
				}
				proof.absent = false
				held, err := RunRestore(t.Context(), env, op)
				if err == nil || held.Step != "verify-fence" || held.Done {
					t.Fatal("missing post-restart absence proof released holds", held, err)
				}
			}
			starts := agent.startCount()
			if starts != 1 {
				t.Fatal("destructive restore resubmitted", starts)
			}
		})
	}
}

func TestRestoreAbsenceUnknownKeepsFenceAndRestartsWindow(t *testing.T) {
	for _, scenario := range []string{"transport", "storage-unknown", "storage-error", "permission-error", "unclassified-metadata", "already-observed", "legacy-agent"} {
		t.Run(scenario, func(t *testing.T) {
			env, agent, clock, op, proof := prepareAbsentRestore(t)
			original := env.RC.Agents
			first, _ := RunRestore(t.Context(), env, op)
			op.Checkpoint = first.Checkpoint
			clock.at = clock.at.Add(2 * restoreMetadataGrace)
			switch scenario {
			case "transport":
				env.RC.Agents = &survivorDialer{testDialer: original.(testDialer)}
			case "storage-unknown":
				proof.absent = false
			case "storage-error":
				proof.err = errors.New("storage listing incomplete")
			case "permission-error":
				agent.restoreDescribeError = "Error: access denied"
			case "unclassified-metadata":
				agent.restoreDescribeError = ""
				agent.restoreDescribeOutput = json.RawMessage(`{}`)
			case "already-observed":
				cp, _ := loadCheckpoint(op)
				cp.RestoreMetadataDeadline = nil
				cp.RestoreAbsentSince = nil
				op.Checkpoint, _ = json.Marshal(cp)
			case "legacy-agent":
				agent.pbmFenceCapability = false
			}
			held, _ := RunRestore(t.Context(), env, op)
			op.Checkpoint = held.Checkpoint
			cp, _ := loadCheckpoint(op)
			starts := agent.startCount()
			fences := agent.recordedFences()
			if held.Step != "poll" || held.Done || cp.RestoreAbsentSince != nil || starts != 1 || len(fences) != 0 {
				t.Fatal("unknown outcome released fence or retained old proof", held, cp)
			}
			if scenario != "already-observed" {
				env.RC.Agents = original
				proof.absent, proof.err = true, nil
				agent.pbmFenceCapability = true
				agent.restoreDescribeError = missingRestoreMetadata
				agent.restoreDescribeOutput = nil
				fresh, _ := RunRestore(t.Context(), env, op)
				op.Checkpoint = fresh.Checkpoint
				cp, _ = loadCheckpoint(op)
				if fresh.Step != "poll" || cp.RestoreAbsentSince == nil || !cp.RestoreAbsentSince.Equal(clock.at) {
					t.Fatal("renewed proof reused pre-error window", fresh, cp)
				}
			}
		})
	}
}

func TestLogicalRestoreObservesStoredOutcomeThroughSurvivor(t *testing.T) {
	env, agent, _, op := restoreStartupSetup(t, false)
	env.RC.Agents = &survivorDialer{testDialer: env.RC.Agents.(testDialer)}
	agent.restoreDescribeError = missingRestoreMetadata
	agent.restoreStorageOutput = json.RawMessage(`{"name":"2024-01-05T11:12:26.296561123Z","status":"done"}`)
	res, err := RunRestore(t.Context(), env, op)
	starts := agent.startCount()
	fences := agent.recordedFences()
	if err != nil || res.Step != "mongod-start" || starts != 1 || len(fences) != 0 {
		t.Fatal("stored native outcome was not observed safely", res, err)
	}
	op.Checkpoint = res.Checkpoint
	cp, _ := loadCheckpoint(op)
	if cp.Failure != "" || cp.RestoreMetadataDeadline != nil {
		t.Fatal(cp)
	}
}

func TestRestoreFenceConsumesPublishedNativeOutcome(t *testing.T) {
	for _, stage := range []string{"before-first", "between-members", "verify-fence"} {
		t.Run(stage, func(t *testing.T) {
			for _, outcome := range []string{"done", "error", "running", "unclassified", "invalid-status"} {
				t.Run(outcome, func(t *testing.T) {
					env, agent, clock, op, proof := prepareAbsentRestore(t)
					advanceRestoreToFence(t, env, clock, op)
					completed := 0
					if stage == "between-members" {
						completed = 1
					} else if stage == "verify-fence" {
						completed = len(env.Nodes) + 1
					}
					for range completed {
						res, err := RunRestore(t.Context(), env, op)
						if err != nil {
							t.Fatal(err)
						}
						op.Checkpoint = res.Checkpoint
					}
					fences := agent.recordedFences()
					starts, observations := len(fences), proof.calls
					agent.restoreDescribeError = ""
					switch outcome {
					case "error":
						agent.failRestore = true
					case "running":
						agent.restoreRunning = true
					case "unclassified":
						agent.restoreDescribeOutput = json.RawMessage(`{}`)
					case "invalid-status":
						agent.restoreDescribeOutput = json.RawMessage(`{"status":12}`)
					}
					res, err := RunRestore(t.Context(), env, op)
					if (err != nil) != (outcome == "invalid-status") {
						t.Fatal("native observation became an admission error", res, err)
					}
					op.Checkpoint = res.Checkpoint
					cp, _ := loadCheckpoint(op)
					currentStarts := agent.startCount()
					currentFences := agent.recordedFences()
					if res.Done || cp.RestoreMetadataDeadline != nil || cp.RestoreAbsentSince != nil ||
						currentStarts != 1 || len(currentFences) != starts || proof.calls != observations {
						t.Fatal("native observation resubmitted work or retained absence proof", res, cp, currentStarts, currentFences)
					}
					if outcome == "done" || outcome == "error" {
						if res.Step != "mongod-start" || (cp.Failure != "") != (outcome == "error") || cp.RestoreConsistencyUncertain != (outcome == "error") {
							t.Fatal("terminal native outcome was not consumed", res, cp)
						}
						if outcome == "error" && (!strings.Contains(cp.Failure, "PBM restore "+cp.PBMName+" failed: restore failed") || !strings.Contains(cp.Failure, "partial changes cannot be excluded")) {
							t.Fatal("native error lost", cp.Failure)
						}
						drive(t, env, op, RunRestore, outcome == "error")
						restore, err := env.RC.Store.GetRestore(t.Context(), env.RC.Cluster.Ref(), "clone")
						want := domain.PhaseSucceeded
						if outcome == "error" {
							want = domain.PhaseFailed
						}
						if err != nil || restore.Phase != want || strings.Contains(restore.Error, "RestoreNeverStarted") {
							t.Fatal("native terminal outcome was replaced by absence failure", restore, err)
						}
					} else {
						if res.Step != "poll" || cp.Failure != "" {
							t.Fatal("nonterminal native metadata released holds", res, cp)
						}
						// Even if metadata disappears again, neither another restart
						// nor the terminal absence path may release the restore holds.
						agent.restoreDescribeError = missingRestoreMetadata
						agent.restoreDescribeOutput = nil
						clock.at = clock.at.Add(2 * restoreMetadataGrace)
						for range 2 {
							held, err := RunRestore(t.Context(), env, op)
							if err == nil || held.Step != "poll" || held.Done {
								t.Fatal("unknown native outcome released holds", held, err)
							}
							op.Checkpoint = held.Checkpoint
						}
						restore, err := env.RC.Store.GetRestore(t.Context(), env.RC.Cluster.Ref(), "clone")
						if err != nil || restore.CompletedAt != nil || restore.Phase == domain.PhaseSucceeded ||
							restore.Phase == domain.PhaseFailed || len(proof.reset) != 0 {
							t.Fatal("nonterminal observation entered compensation", restore, err)
						}
					}
					currentStarts = agent.startCount()
					currentFences = agent.recordedFences()
					if currentStarts != 1 || len(currentFences) != starts || proof.calls != observations {
						t.Fatal("native outcome caused another initiation or absence fence", currentStarts, currentFences, proof.calls)
					}
				})
			}
		})
	}
}

func TestRestoreFenceRefreshesAdmissionBeforeEveryRestart(t *testing.T) {
	for _, completed := range []int{0, 1} {
		t.Run(map[int]string{0: "before-first", 1: "between-members"}[completed], func(t *testing.T) {
			env, agent, clock, op, proof := prepareAbsentRestore(t)
			advanceRestoreToFence(t, env, clock, op)
			for range completed {
				res, err := RunRestore(t.Context(), env, op)
				if err != nil {
					t.Fatal(err)
				}
				op.Checkpoint = res.Checkpoint
			}
			before := proof.calls
			// A backup/PITR/replay/resync lock appears after the sustained
			// absence observation or between two member restarts.
			proof.absent = false
			held, err := RunRestore(t.Context(), env, op)
			cp := checkpoint{}
			_ = json.Unmarshal(held.Checkpoint, &cp)
			fences := agent.recordedFences()
			if err == nil || held.Step != "fence-pbm" || cp.Index != completed ||
				len(fences) != completed || proof.calls <= before || cp.Failure != "" {
				t.Fatal("conflicting PBM work was not freshly admitted before restart", held, err, len(fences), proof.calls-before)
			}
			if cp.RestoreFenceUnknown {
				t.Fatal("an unsubmitted restart was classified as an unknown execution")
			}
		})
	}
}

func TestRestoreFinalFenceObservationRevalidatesAllReceipts(t *testing.T) {
	env, agent, clock, op, proof := prepareAbsentRestore(t)
	advanceRestoreToFence(t, env, clock, op)
	for range len(env.Nodes) + 1 {
		res, err := RunRestore(t.Context(), env, op)
		if err != nil {
			t.Fatal(err)
		}
		op.Checkpoint = res.Checkpoint
	}
	// Reproduce the previous release's persisted verification cursor. It must
	// never authorize skipping a member in a later complete observation.
	cp, _ := loadCheckpoint(op)
	cp.Index = len(env.Nodes)
	op.Checkpoint, _ = json.Marshal(cp)
	before := agent.fenceCallCount()
	proof.absent = false
	interrupted, err := RunRestore(t.Context(), env, op)
	if err == nil || interrupted.Step != "verify-fence" || interrupted.Done {
		t.Fatal("fixture did not interrupt the final absence observation", interrupted, err)
	}
	op.Checkpoint = interrupted.Checkpoint
	cp, _ = loadCheckpoint(op)
	calls := agent.fenceCallCount()
	if cp.Index != 0 || calls-before != len(env.Nodes) {
		t.Error("final observation retained verification progress or skipped receipts", cp.Index, calls-before)
	}
	// After that failed observation, a replacement/session becomes invalid.
	// Even though metadata is now absent again, the old receipt cannot release
	// holds. Every final observation must perform its own receipt validation.
	proof.absent = true
	agent.fenceRejectReplay = true
	before = proof.calls
	held, err := RunRestore(t.Context(), env, op)
	cp, _ = loadCheckpoint(&domain.Operation{Checkpoint: held.Checkpoint})
	if err == nil || held.Step != "verify-fence" || held.Done || !cp.RestoreFenceUnknown || cp.Failure != "" {
		t.Fatal("stale receipt released the restore fence", held, err)
	}
	if proof.calls != before {
		t.Fatal("final absence was accepted before current receipt validation")
	}
}
