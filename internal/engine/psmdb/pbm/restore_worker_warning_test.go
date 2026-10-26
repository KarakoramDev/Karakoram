// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package pbm

import (
	"strings"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/store"
)

func TestRestoreWorkerUnconfirmedWarnsRestoreAndCreateOnceAndClears(t *testing.T) {
	env, agent, _, op := restoreStartupSetup(t, false)
	st := env.RC.Store.(*store.Store)
	if err := st.CreateCluster(t.Context(), env.RC.Cluster); err != nil {
		t.Fatal(err)
	}
	env.RC.Cluster.Spec.DataSource = &domain.DataSource{Cluster: "source", Backup: "base"}
	owner := &domain.Operation{ID: "create-owner", Cluster: env.RC.Cluster.Name, Kind: domain.OperationCreate, Phase: domain.PhaseRunning}
	if err := st.EnqueueOperation(t.Context(), owner); err != nil {
		t.Fatal(err)
	}
	r, err := st.GetRestore(t.Context(), env.RC.Cluster.Ref(), "clone")
	if err != nil {
		t.Fatal(err)
	}
	r.OperationID = op.ID
	if err := st.UpdateRestore(t.Context(), r); err != nil {
		t.Fatal(err)
	}
	hooks := &workerFailureHooks{Hooks: env.Hooks, unconfirmed: true}
	env.Hooks, agent.restoreRunning = hooks, true
	for range 2 {
		result, err := RunRestore(t.Context(), env, op)
		if err != nil || result.Done || !strings.Contains(result.Message, "RestoreWorkerUnconfirmed") {
			t.Fatal("warning changed native outcome or disappeared", result, err)
		}
		op.Checkpoint = result.Checkpoint
		outcome, err := ReadRestoreOutcome(result.Checkpoint)
		if err != nil || !outcome.WorkerUnconfirmed || outcome.Failure != "" || outcome.ConsistencyUncertain {
			t.Fatal("nonterminal warning became abort evidence", outcome, err)
		}
	}
	counts := map[string]int{}
	for _, event := range env.RC.Events.(*fakeEvents).events {
		if event.Reason == "RestoreWorkerUnconfirmed" {
			counts[event.Operation]++
		}
	}
	if counts[op.ID] != 1 || counts[owner.ID] != 1 {
		t.Fatal("missing or repeated restore/create warning events", counts)
	}
	hooks.unconfirmed = false
	result, err := RunRestore(t.Context(), env, op)
	if err != nil || result.Done || strings.Contains(result.Message, "RestoreWorkerUnconfirmed") {
		t.Fatal("resumed native progress did not clear warning", result, err)
	}
	outcome, err := ReadRestoreOutcome(result.Checkpoint)
	starts := agent.startCount()
	if err != nil || outcome.WorkerUnconfirmed || outcome.Failure != "" || starts != 1 {
		t.Fatal(outcome, err, starts)
	}
}
