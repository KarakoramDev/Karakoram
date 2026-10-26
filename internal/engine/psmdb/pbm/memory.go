// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package pbm

import (
	"context"
	"fmt"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
)

const mebibyte = uint64(1 << 20)

// MinimumLogicalRestoreGoBudget is an admission floor, not a peak-RSS promise.
// Native archive/BSON/bulk/driver buffers need live heap before GC can reclaim
// anything; a smaller target would invite GC thrashing without useful progress.
const MinimumLogicalRestoreGoBudget = 320 * mebibyte

// MemoryBudgetUnknownMessage describes capability, not a native failure.
const MemoryBudgetUnknownMessage = "PBMMemoryBudgetUnknown: PBM launch Go limit is unconfirmed (legacy, unavailable or pending-budget agent); no automatic restart"

// GoMemoryLimitBytes reserves configured WiredTiger cache, 256 MiB of mongod
// non-cache memory, 64 MiB for agents/CLI, and 64 MiB of allocation headroom.
// Only applied minimum RAM is usable. Missing applied RAM remains unknown (zero).
// A zero active-cache field uses the agent's historical 0.25 GiB default.
func GoMemoryLimitBytes(resources domain.Resources, cacheGB float64) uint64 {
	ram := resources.MinRAM
	if ram <= 0 {
		return 0
	}
	cacheGB = max(0.25, cacheGB)
	budgetMiB := (ram-cacheGB)*1024 - 384
	if budgetMiB <= 0 {
		return 0
	}
	return uint64(budgetMiB) * mebibyte
}

func minimumGoMemoryBudget(env Env) (uint64, *Node) {
	var budget uint64
	var limiting *Node
	for i := range env.Nodes {
		node := &env.Nodes[i]
		memberBudget := GoMemoryLimitBytes(node.Resources, node.CacheGB)
		if limiting == nil || memberBudget < budget {
			budget, limiting = memberBudget, node
		}
	}
	return budget, limiting
}

func logicalRestoreMemoryError(env Env) error {
	budget, node := minimumGoMemoryBudget(env)
	if node == nil {
		return fmt.Errorf("PBMRestoreMemoryBudgetUnknown: no participating mongod has applied memory sizing; reconcile the target allocation before submitting recovery; native restore was not initiated")
	}
	name := "engine"
	if node.ReplSet == "cfg" {
		name = "configServer"
	}
	if node.Resources.MinRAM <= 0 {
		return fmt.Errorf("PBMRestoreMemoryBudgetUnknown: member %s (%s) has no authoritative applied RAM bound; reconcile its backend allocation before submitting recovery; native restore was not initiated", node.Member, name)
	}
	if budget < MinimumLogicalRestoreGoBudget {
		return fmt.Errorf("PBMRestoreMemoryBudgetTooSmall: member %s (component %s) has a derived applied PBM Go memory budget of %d MiB; logical restore requires at least %d MiB after active WiredTiger, mongod and agent reserves; apply spec.components.%s.resources.minRam (and maxRam as needed) before submitting recovery; a failed initial clone must be deleted and recreated with larger resources; native restore was not initiated", node.Member, name, budget/mebibyte, MinimumLogicalRestoreGoBudget/mebibyte, name)
	}
	return nil
}

// observeGoMemoryBudget records the first restore-time capability observation,
// and never turns an unavailable/legacy diagnostic into a native abort.
func observeGoMemoryBudget(ctx context.Context, env Env) bool {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if len(env.Nodes) == 0 {
		return false
	}
	for _, node := range env.Nodes {
		client, err := env.RC.Agents.Dial(ctx, env.RC.Cluster.Ref(), node.Member)
		if err != nil {
			return false
		}
		status, err := engine.ObserveAgentStatus(ctx, client, "PSMDB", node.Member)
		client.Close()
		expected := GoMemoryLimitBytes(node.Resources, node.CacheGB)
		if err != nil || expected == 0 || status.Processes["pbm-agent"].GoMemoryLimitBytes != expected {
			return false
		}
	}
	return true
}
