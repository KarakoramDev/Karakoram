// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package elastic

import (
	"context"
	"errors"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/domain"
)

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
