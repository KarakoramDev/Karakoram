// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package mysql

import (
	"context"
	"sort"
	"sync"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/mysql/myspec"
)

type gtidObservationKey struct{ namespace, cluster string }

type gtidObservationBudgetKey struct{ namespace, cluster string }

// Accessed only while holding the same cluster observation/commit mutex.
type gtidObservationBudget struct{ remaining time.Duration }

type gtidObservationMember struct {
	name, service, volume, server, stage string
}

type gtidObservationIdentity struct {
	generation, operationGeneration           int64
	epoch                                     uint64
	group, fence, checkpoint                  string
	operationID, restoreID, restoreCheckpoint string
	restoreGeneration                         int64
	backupGeneration                          int
}

type gtidObservationSample struct {
	identity gtidObservationIdentity
	members  []gtidObservationMember
	statuses map[string]myspec.GaleraStatusResponse
	at       time.Time
}

// samplePXCQuorumGTIDs requires gtidObservationMutex(rc) to remain held through
// sampling AND the caller's journal commit. Fresh bypasses and replaces the pass
// cache: acknowledgement, incident append and restore certification/activation
// must never authorize a write using an earlier read-only observation. Returned
// maps are immutable. The timestamp belongs to the sample, not its cache lookup.
// Without a pass scope every invocation samples agents afresh.
// Read-only failed sweeps share at most ObservationTimeout of network time per
// cluster/pass. Exhaustion means comparison unavailable, not member unhealthy;
// fresh write-relevant probes are exempt and still use normal agent timeouts.
func samplePXCQuorumGTIDs(ctx context.Context, rc *engine.Context, s *state, fresh bool) (map[string]myspec.GaleraStatusResponse, time.Time) {
	members := s.data()
	sort.Slice(members, func(i, j int) bool { return members[i].Name < members[j].Name })
	identity := gtidObservationIdentity{
		generation: rc.Cluster.Generation, epoch: s.Formation.FormationEpoch,
		group: s.Formation.GroupName, fence: s.RestoreGTIDFence,
	}
	if rc.Operation != nil {
		identity.operationID = rc.Operation.ID
		identity.operationGeneration = rc.Operation.TargetGeneration
		identity.checkpoint = string(rc.Operation.Checkpoint)
	}
	key := gtidObservationKey{rc.Cluster.Namespace, rc.Cluster.Name}
	cache := engine.ObservationPassCache(ctx)
	backup, backupErr := loadBackupState(rc.Cluster)
	identity.backupGeneration = backup.Generation
	if op := backup.RestoreOperation; op != nil {
		identity.restoreID, identity.restoreCheckpoint = op.ID, string(op.Checkpoint)
		identity.restoreGeneration = op.TargetGeneration
	}
	var previous *gtidObservationSample
	if cache != nil {
		if value, ok := cache.Load(key); ok {
			previous = value.(*gtidObservationSample)
		}
	}
	matches := previous != nil && previous.identity == identity && len(previous.members) == len(members)
	if matches {
		for i, m := range members {
			if previous.members[i] != (gtidObservationMember{m.Name, m.ServiceID, m.VolumeID, m.ServerUUID, m.Stage}) {
				matches = false
				break
			}
		}
	}
	if !fresh && matches && ctx.Err() == nil && backupErr == nil {
		return previous.statuses, previous.at
	}
	// Invalidate before a fresh attempt so failed writes cannot leave an older
	// successful sample available to a later read in this pass.
	if cache != nil {
		cache.Delete(key)
	}
	if cache != nil && !fresh {
		budgetKey := gtidObservationBudgetKey{rc.Cluster.Namespace, rc.Cluster.Name}
		value, ok := cache.Load(budgetKey)
		if !ok {
			value = &gtidObservationBudget{remaining: engine.ObservationTimeout}
			cache.Store(budgetKey, value)
		}
		budget := value.(*gtidObservationBudget)
		if budget.remaining <= 0 {
			unavailable := make(map[string]myspec.GaleraStatusResponse, len(members))
			for _, m := range members {
				unavailable[m.Name] = myspec.GaleraStatusResponse{}
			}
			return unavailable, now(rc)
		}
		started := time.Now()
		probe, cancel := context.WithTimeout(ctx, budget.remaining)
		defer cancel()
		ctx = probe
		defer func() {
			budget.remaining -= time.Since(started)
		}()
	}
	statuses := make([]myspec.GaleraStatusResponse, len(members))
	succeeded := make([]bool, len(members))
	// A completed sweep proves no later boundary than when its probes began.
	at := now(rc)
	var wg sync.WaitGroup
	for i, m := range members {
		if ctx.Err() != nil || rc.Agents == nil || m.ServiceID == "" || m.Stage == "planned" || m.Stage == "imported" {
			continue
		}
		wg.Add(1)
		go func(i int, name string) {
			defer wg.Done()
			if ctx.Err() != nil {
				return
			}
			status, err := galeraStatus(ctx, rc, name)
			if err == nil {
				statuses[i], succeeded[i] = status, true
			}
		}(i, m.Name)
	}
	wg.Wait()
	observations := make(map[string]myspec.GaleraStatusResponse, len(members))
	allSucceeded := len(members) > 0 && ctx.Err() == nil
	for i, m := range members {
		observations[m.Name] = statuses[i]
		allSucceeded = allSucceeded && succeeded[i]
	}
	if cache != nil && allSucceeded && backupErr == nil {
		identities := make([]gtidObservationMember, len(members))
		for i, m := range members {
			identities[i] = gtidObservationMember{m.Name, m.ServiceID, m.VolumeID, m.ServerUUID, m.Stage}
		}
		cache.Store(key, &gtidObservationSample{identity: identity, members: identities, statuses: observations, at: at})
	}
	return observations, at
}
