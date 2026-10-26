// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package pbm

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
)

type inventoryProbeDialer struct {
	engine.AgentDialer
	mu          sync.Mutex
	seen        map[string]bool
	stalled     string
	unavailable bool
}

func (d *inventoryProbeDialer) Dial(ctx context.Context, ref domain.ClusterRef, member string) (*agentapi.Client, error) {
	d.mu.Lock()
	d.seen[member] = true
	d.mu.Unlock()
	if deadline, ok := ctx.Deadline(); !ok || time.Until(deadline) > engine.ObservationTimeout {
		return nil, errors.New("unbounded inventory probe")
	}
	if d.unavailable {
		return nil, errors.New("native inventory unavailable")
	}
	if member == d.stalled {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	return d.AgentDialer.Dial(ctx, ref, member)
}

func TestInventoryObservationRacesHangingMember(t *testing.T) {
	env, _, agent, _ := setup(t)
	d := &inventoryProbeDialer{AgentDialer: env.RC.Agents, stalled: "m0", seen: map[string]bool{}}
	env.RC.Agents = d
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	if _, err := inventory(ctx, env); err != nil {
		t.Fatal("healthy inventory peer was starved by first member", err)
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	starts := agent.startCount()
	if !d.seen["m0"] || !d.seen["m1"] || starts != 0 {
		t.Fatal("inventory did not isolate both reads without initiating native work", d.seen, starts)
	}
}

func TestInventoryObservationKeepsConfigSetBoundaryAndMemberErrors(t *testing.T) {
	env, _, _, _ := setup(t)
	env.Sharded = true
	env.Nodes = []Node{{Member: "shard", ReplSet: "rs0"}, {Member: "cfg0", ReplSet: "cfg"}, {Member: "cfg1", ReplSet: "cfg"}}
	d := &inventoryProbeDialer{AgentDialer: env.RC.Agents, unavailable: true, seen: map[string]bool{}}
	env.RC.Agents = d
	_, err := inventoryCommand(t.Context(), env, "list")
	if err == nil || !strings.Contains(err.Error(), "member cfg0") || !strings.Contains(err.Error(), "member cfg1") {
		t.Fatal("inventory errors lost their member attribution", err)
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.seen["shard"] || !d.seen["cfg0"] || !d.seen["cfg1"] {
		t.Fatal("sharded inventory escaped config replica set", d.seen)
	}
}
