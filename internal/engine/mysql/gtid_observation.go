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

type gtidObservationIdentity struct {
	generation, operationGeneration           int64
	config                                    int64
	epoch                                     uint64
	group, fence, checkpoint                  string
	uid, formation                            string
	operationID, restoreID, restoreCheckpoint string
	restoreGeneration                         int64
	backupGeneration                          int
}

type gtidObservationSample struct {
	identity gtidObservationIdentity
	members  []mysqlReadMember
	statuses map[string]myspec.GaleraStatusResponse
	at       time.Time
}

// samplePXCQuorumGTIDs requires gtidObservationMutex(rc) to remain held through
// sampling AND the caller's journal commit. Fresh bypasses and replaces the pass
// cache: acknowledgement, incident append and restore certification/activation
// must never authorize a write using an earlier read-only observation. Returned
// maps are immutable. The timestamp belongs to the sample, not its cache lookup.
// Without a pass scope every invocation samples agents afresh.
// All read-only RPCs share one absolute pass deadline, including failed sweeps
// and lock waits. Fresh write-relevant probes are exempt from that read budget.
func samplePXCQuorumGTIDs(ctx context.Context, rc *engine.Context, s *state, fresh bool) (map[string]myspec.GaleraStatusResponse, time.Time) {
	members := s.data()
	sort.Slice(members, func(i, j int) bool { return members[i].Name < members[j].Name })
	identity := gtidObservationIdentity{
		generation: rc.Cluster.Generation, epoch: s.Formation.FormationEpoch,
		group: s.Formation.GroupName, fence: s.RestoreGTIDFence,
		config: s.ConfigVersion, uid: s.UID, formation: s.Formation.State,
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
			if previous.members[i] != mysqlReadMemberIdentity(m) {
				matches = false
				break
			}
		}
	}
	if !fresh && matches && backupErr == nil {
		return previous.statuses, previous.at
	}
	// Unsaved caller changes require discarding raw RPC snapshots too. A saved
	// identity change does not: Status may already have populated fresh native
	// samples for that identity before reaching this optional comparison.
	if previous != nil && !matches {
		persisted, loadErr := load(rc.Cluster)
		unsaved := loadErr != nil || persisted.UID != s.UID || persisted.ConfigVersion != s.ConfigVersion ||
			persisted.Formation.FormationEpoch != s.Formation.FormationEpoch ||
			persisted.Formation.GroupName != s.Formation.GroupName || persisted.Formation.State != s.Formation.State ||
			persisted.RestoreGTIDFence != s.RestoreGTIDFence || len(persisted.data()) != len(members)
		if !unsaved {
			for _, m := range members {
				original := persisted.find(m.Name)
				if original == nil || mysqlReadMemberIdentity(original) != mysqlReadMemberIdentity(m) {
					unsaved = true
					break
				}
			}
		}
		operationChanged := previous.identity.operationID != identity.operationID ||
			previous.identity.operationGeneration != identity.operationGeneration || previous.identity.checkpoint != identity.checkpoint
		if unsaved || operationChanged {
			invalidateMySQLReadObservation(ctx, rc)
		}
	}
	// Invalidate before a fresh attempt so failed writes cannot leave an older
	// successful sample available to a later read in this pass.
	if cache != nil {
		cache.Delete(key)
	}
	if fresh {
		invalidateMySQLReadObservation(ctx, rc)
		ctx = freshMySQLReads(ctx)
	}
	probe, cancel := mysqlReadContext(ctx, rc)
	defer cancel()
	ctx = probe
	statuses := make([]myspec.GaleraStatusResponse, len(members))
	succeeded := make([]bool, len(members))
	sampleTimes := make([]time.Time, len(members))
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
			status, sampledAt, err := galeraStatusAt(ctx, rc, name)
			if err == nil {
				statuses[i], sampleTimes[i], succeeded[i] = status, sampledAt, true
			}
		}(i, m.Name)
	}
	wg.Wait()
	observations := make(map[string]myspec.GaleraStatusResponse, len(members))
	allSucceeded := len(members) > 0 && ctx.Err() == nil
	for i, m := range members {
		observations[m.Name] = statuses[i]
		allSucceeded = allSucceeded && succeeded[i]
		// A retry may reuse individual successful responses from an earlier
		// incomplete sweep. Never advance the safety boundary past those reads.
		if succeeded[i] && sampleTimes[i].Before(at) {
			at = sampleTimes[i]
		}
	}
	if cache != nil && allSucceeded && backupErr == nil {
		identities := make([]mysqlReadMember, len(members))
		for i, m := range members {
			identities[i] = mysqlReadMemberIdentity(m)
		}
		cache.Store(key, &gtidObservationSample{identity: identity, members: identities, statuses: observations, at: at})
	}
	return observations, at
}
