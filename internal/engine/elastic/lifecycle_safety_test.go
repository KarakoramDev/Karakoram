// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package elastic

import (
	"context"
	"errors"
	"net/http"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine/elastic/esapi"
)

func TestScaleUpCancelsUnfinishedDrain(t *testing.T) {
	for _, stage := range []string{stageJoined, stageDraining} {
		t.Run(stage, func(t *testing.T) {
			p, rc, f, es := coreSetup(t, 3)
			s := coreConverge(t, p, rc, f)
			m := s.find("demon2")
			m.Removing, m.Stage = true, stage
			if stage == stageDraining {
				m.AllocationExcluded = true
				es.persistent["cluster.routing.allocation.exclude._name"] = "operator,demon2"
			}
			if err := blockDrain(t.Context(), rc, s, m, "insufficient data nodes for index replicas"); err != nil {
				t.Fatal(err)
			}
			if err := save(rc.Cluster, s); err != nil {
				t.Fatal(err)
			}
			events := rc.Events.(*coreEvents)
			events.events = nil
			// Restoring the original count must reach cancellation before removeStep.
			rc.Cluster.Spec.Components.Engine.Replicas = 3
			if _, err := p.Sync(t.Context(), rc); err != nil {
				t.Fatal(err)
			}
			s, _ = load(rc.Cluster)
			m = s.find("demon2")
			if m.Removing || m.AllocationExcluded || m.Stage != stageJoined || s.DrainBlocked != "" || f.services[m.Name] == nil {
				t.Fatal("unfinished drain was not cancelled", m, s.DrainBlocked)
			}
			if stage == stageDraining && es.persistent["cluster.routing.allocation.exclude._name"] != "operator" {
				t.Fatal("cancellation did not preserve operator exclusions", es.persistent)
			}
			if len(events.events) != 1 || events.events[0].Type != "Normal" || events.events[0].Reason != "ShardDrainCancelled" {
				t.Fatal("cancellation event missing", events.events)
			}
			rc.Cluster.Spec.Components.Engine.Replicas = 4
			s = coreConverge(t, p, rc, f)
			if len(s.members(kindNode)) != 4 || s.find("demon3") == nil || s.find("demon2").Removing {
				t.Fatal("additional capacity did not converge", s.Members)
			}
		})
	}
}

func TestScaleUpDoesNotCancelCompletedDrainOrReplacement(t *testing.T) {
	p, rc, f, _ := coreSetup(t, 3)
	s := coreConverge(t, p, rc, f)
	m := s.find("demon2")
	m.Removing, m.Stage = true, stageExcluded
	if cancelled, err := p.cancelRemoval(t.Context(), rc, s, m); cancelled || err != nil {
		t.Fatal("completed drain was cancelled", cancelled, err)
	}
	m.Stage = stageDraining
	s.find("demon1").Replaces = m.Name
	if cancelled, err := p.cancelRemoval(t.Context(), rc, s, m); cancelled || err != nil {
		t.Fatal("replacement removal was cancelled", cancelled, err)
	}
}

func TestRollingRequiresCommittedVotingQuorum(t *testing.T) {
	p, rc, f, es := coreSetup(t, 3)
	s := coreConverge(t, p, rc, f)
	// Every planned master is present, but the committed configuration still
	// contains two unavailable old voters. Names alone cannot prove quorum.
	es.voters = []string{"demon0", "demon1", "demon2", "old-voter-a", "old-voter-b"}
	for _, step := range []string{"prepare", "apply"} {
		s.Rolling = &rolling{Reason: "restart", Current: "demon1", Order: []string{"demon1"}, Step: step}
		before, token := len(es.mutations), s.find("demon1").RestartToken
		if _, _, err := p.rollingStep(context.Background(), rc, s); !errors.Is(err, errWaiting) {
			t.Fatal("disruption did not wait for committed voting quorum", err)
		}
		if s.Rolling.Step != step || len(es.mutations) != before || s.find("demon1").RestartToken != token {
			t.Fatal("unsafe disruption advanced", s.Rolling)
		}
	}
	s.Retry = &pending{Action: "resources", Member: "demon1", Resources: domain.Resources{MinRAM: 4}}
	before := f.mutations
	if _, _, err := p.retryPending(context.Background(), rc, s); !errors.Is(err, errWaiting) || f.mutations != before {
		t.Fatal("platform retry bypassed committed quorum gate", err)
	}
	s.Retry = nil
	es.voters = []string{"demon0", "demon1", "demon2"}
	s.Rolling.Step = "prepare"
	if _, _, err := p.rollingStep(context.Background(), rc, s); err != nil || s.Rolling.Step != "flush" {
		t.Fatal("safe committed voting quorum did not release gate", err, s.Rolling)
	}
}

func TestRollingUpgradeRetryRecoversItsAbsentTarget(t *testing.T) {
	for _, scenario := range []string{"target-absent", "other-master-absent", "committed-quorum-missing", "different-target"} {
		t.Run(scenario, func(t *testing.T) {
			p, rc, f, es := coreSetup(t, 3)
			s := coreConverge(t, p, rc, f)
			s.Rolling = &rolling{Reason: "upgrade", Version: "9.5.5", Current: "demon1", Order: []string{"demon1"}, Step: "rejoin"}
			s.Retry = &pending{Action: "upgrade", Member: "demon1", Version: "9.5.5"}
			es.voters = []string{"demon0", "demon1", "demon2"}
			// Publishing the new version succeeded, but installing its product
			// home failed. The agent cannot rejoin until deployment is retried.
			delete(f.nodes, "demon1")
			switch scenario {
			case "other-master-absent":
				delete(f.nodes, "demon2")
			case "committed-quorum-missing":
				es.voters = append(es.voters, "old-voter-a", "old-voter-b")
			case "different-target":
				s.Rolling.Current = "demon2"
			}
			before := f.mutations
			busy, _, err := p.retryPending(context.Background(), rc, s)
			if scenario != "target-absent" {
				if !busy || !errors.Is(err, errWaiting) || f.mutations != before {
					t.Fatal("unsafe retry bypassed membership/quorum gate", busy, err, f.calls)
				}
				return
			}
			if !busy || err != nil || f.mutations != before+1 || s.Pending == nil || s.Retry != nil || f.nodes["demon1"] == "" {
				t.Fatal("failed upgrade target could not recover", busy, err, s.Pending, s.Retry, f.calls)
			}
		})
	}
}

func TestYellowRejectsPlaceableReplicaAmongUnplaceableShards(t *testing.T) {
	p, rc, f, es := coreSetup(t, 3)
	s := coreConverge(t, p, rc, f)
	es.health, es.replicas = "yellow", 4
	es.shardRows = []map[string]string{
		{"index": "oversubscribed", "shard": "0", "prirep": "p", "state": "STARTED", "node": "demon0"},
		{"index": "oversubscribed", "shard": "0", "prirep": "r", "state": "STARTED", "node": "demon1"},
		{"index": "oversubscribed", "shard": "0", "prirep": "r", "state": "STARTED", "node": "demon2"},
		{"index": "oversubscribed", "shard": "0", "prirep": "r", "state": "UNASSIGNED"},
		{"index": "recoverable", "shard": "0", "prirep": "p", "state": "STARTED", "node": "demon0"},
		{"index": "recoverable", "shard": "0", "prirep": "r", "state": "UNASSIGNED"},
	}
	for _, step := range []string{"prepare", "recover"} {
		s.Rolling = &rolling{Reason: "restart", Current: "demon1", Order: []string{"demon1"}, Step: step}
		if _, _, err := p.rollingStep(context.Background(), rc, s); err != nil || s.Rolling.Step != step || s.Rolling.Current != "demon1" {
			t.Fatal("placeable replica passed rolling health gate", err, s.Rolling)
		}
	}
	s.Rolling = nil
	s.Phase = "ready"
	if err := save(rc.Cluster, s); err != nil {
		t.Fatal(err)
	}
	status, err := p.Status(context.Background(), rc)
	if err != nil || status.Phase == domain.PhaseReady {
		t.Fatal("placeable replica reported ready", err, status)
	}
	if _, err := p.Sync(context.Background(), rc); err != nil {
		t.Fatal(err)
	}
	s, _ = load(rc.Cluster)
	if s.Phase == "ready" {
		t.Fatal("placeable replica converged")
	}
}

func TestRemovalRequiresAllocatedPrimariesAtEveryDestructiveStage(t *testing.T) {
	p, rc, f, es := coreSetup(t, 3)
	s := coreConverge(t, p, rc, f)
	m := s.find("demon2")
	m.Removing = true
	delete(f.nodes, m.Name)
	es.health = "red"
	for _, stage := range []string{stageDraining, stageExcluded, stageDeleting, "delete-volume"} {
		m.Stage = stage
		beforeES, beforeInfra := len(es.mutations), f.mutations
		if err := p.removeStep(context.Background(), rc, s, m); err != nil {
			t.Fatal(err)
		}
		if m.Stage != stage || s.Pending != nil || len(es.mutations) != beforeES || f.mutations != beforeInfra || s.DrainBlocked == "" {
			t.Fatalf("red cluster advanced removal at %s: %+v", stage, m)
		}
		if f.services[m.Name] == nil || f.services[m.Volume] == nil {
			t.Fatal("recoverable runtime or volume was deleted")
		}
	}
	// Yellow proves the missing copies are replicas rather than primaries.
	es.health, m.Stage = "yellow", stageDraining
	if err := p.removeStep(context.Background(), rc, s, m); err != nil || m.Stage != stageExcluded || s.DrainBlocked != "" {
		t.Fatal("safe absent member did not finish draining", err, m)
	}
}

func TestReplaceUnavailableMemberObservesReplacementJoin(t *testing.T) {
	p, rc, f, _ := coreSetup(t, 3)
	s := coreConverge(t, p, rc, f)
	delete(f.nodes, "demon0")
	op := &domain.Operation{ID: "replace-unavailable", Kind: domain.OperationReplaceMember, Payload: []byte(`{"member":"demon0"}`)}
	result, err := p.runOperation(context.Background(), rc, s, op)
	if err != nil || result.Done {
		t.Fatal("could not begin replacement", result, err)
	}
	op.Checkpoint = result.Checkpoint
	newName := opsCheckpoint(t, result).NewMember
	if err := save(rc.Cluster, s); err != nil {
		t.Fatal(err)
	}
	done := false
	for range 100 {
		if _, err := p.Sync(context.Background(), rc); err != nil {
			t.Fatal(err)
		}
		s, err = load(rc.Cluster)
		if err != nil {
			t.Fatal(err)
		}
		result, err = p.runOperation(context.Background(), rc, s, op)
		if err != nil {
			t.Fatal(err)
		}
		if old := s.find("demon0"); old != nil && old.Removing && s.find(newName).Stage != stageJoined {
			t.Fatal("old member removed before replacement joined")
		}
		if err := save(rc.Cluster, s); err != nil {
			t.Fatal(err)
		}
		op.Checkpoint = result.Checkpoint
		if result.Done {
			done = true
			break
		}
	}
	if !done || s.find("demon0") != nil || s.find(newName) == nil || s.find(newName).Stage != stageJoined || s.find(newName).Replaces != "" {
		t.Fatal("replacement wedged on absent old member", result, s)
	}
	if f.services["demon0"] != nil || f.services["demon0v"] != nil {
		t.Fatal("old member was not retired")
	}
}

func TestMissingAllocationRowRequiresAbsentNode(t *testing.T) {
	present, exclusions := true, 0
	p, rc, s, _ := opsFixture(t, func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/_cat/allocation":
			opsJSON(w, []any{})
		case "/_cluster/health":
			opsJSON(w, esapi.Health{Status: "green"})
		case "/_nodes":
			nodes := map[string]any{}
			if present {
				nodes["node"] = map[string]string{"name": "example.com"}
			}
			opsJSON(w, map[string]any{"nodes": nodes})
		case "/_cluster/voting_config_exclusions":
			exclusions++
			opsJSON(w, map[string]bool{"acknowledged": true})
		default:
			t.Error("unexpected removal request", r.URL)
			http.Error(w, "unexpected", http.StatusInternalServerError)
		}
	})
	m := s.find("example.com")
	m.Removing = true
	m.Stage = stageDraining
	if err := p.removeStep(context.Background(), rc, s, m); err != nil || m.Stage != stageDraining || exclusions != 0 {
		t.Fatal("present node with missing allocation row was treated as drained", err, m)
	}
	present = false
	if err := p.removeStep(context.Background(), rc, s, m); err != nil || m.Stage != stageExcluded || exclusions != 1 {
		t.Fatal("safe absent node was not treated as drained", err, m)
	}
}

func TestRemovalRetriesRequireAllocatedPrimaries(t *testing.T) {
	p, rc, f, es := coreSetup(t, 3)
	s := coreConverge(t, p, rc, f)
	m := s.find("demon2")
	m.Removing = true
	es.health = "red"
	for _, action := range []string{"stop-removal", "delete-runtime", "delete-volume"} {
		s.Retry = &pending{Action: action, Member: m.Name}
		before := f.mutations
		if _, _, err := p.retryPending(context.Background(), rc, s); !errors.Is(err, errWaiting) {
			t.Fatal("removal retry did not wait for primary safety", action, err)
		}
		if f.mutations != before || s.Retry == nil || s.Pending != nil {
			t.Fatal("unsafe removal retry advanced", action)
		}
	}
	es.health = "green"
	if _, _, err := p.retryPending(context.Background(), rc, s); err != nil || s.Retry != nil || s.Pending == nil {
		t.Fatal("safe removal retry did not resume", err)
	}
}

func TestRemovalPreflightUsesProjectedAutoExpandReplicas(t *testing.T) {
	p, rc, f, es := coreSetup(t, 3)
	s := coreConverge(t, p, rc, f)
	m := s.find("demon2")
	m.Removing = true
	es.replicas = 2
	for _, tc := range []struct {
		setting string
		blocked bool
	}{{"0-all", false}, {"0-5", false}, {"2-all", true}, {"2-5", true}, {"false", true}} {
		t.Run(tc.setting, func(t *testing.T) {
			es.autoExpand, m.Stage = tc.setting, stageJoined
			if err := p.removeStep(context.Background(), rc, s, m); err != nil {
				t.Fatal(err)
			}
			if got := s.DrainBlocked != ""; got != tc.blocked {
				t.Fatalf("blocked=%v, want %v", got, tc.blocked)
			}
			want := stageDraining
			if tc.blocked {
				want = stageJoined
			}
			if m.Stage != want {
				t.Fatalf("stage=%s, want %s", m.Stage, want)
			}
		})
	}
}
