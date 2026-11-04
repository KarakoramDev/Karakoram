// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package postgres

import (
	"context"
	"encoding/json"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/postgres/pgspec"
	"github.com/KarakoramDev/Karakoram/internal/secrets"
)

func (p *Core) ensureCitusStanzas(ctx context.Context, rc *engine.Context, s *state, upgrade bool) error {
	bs, err := loadBackupState(rc.Cluster)
	if err != nil {
		return err
	}
	names := append([]string{rc.Cluster.Spec.Backup.Storage}, bs.RepositoryStorages...)
	action := "stanza-create"
	if upgrade {
		action = "stanza-upgrade"
	}
	seen := map[string]bool{}
	for _, name := range names {
		if name == "" || seen[name] {
			continue
		}
		seen[name] = true
		storage, err := p.backupStorage(ctx, rc, name, rc.Cluster.Ref().Namespace)
		if err != nil {
			return err
		}
		for _, g := range s.groups() {
			leader, _, err := g.primary(ctx, rc)
			if err != nil {
				return err
			}
			repo, err := repository(storage, groupRepoPrefix(storage, rc.Cluster, s, g.id))
			if err != nil {
				return err
			}
			if err = pgAction(ctx, rc, leader.Name, action, pgspec.RepositoryRequest{Repository: &repo}, nil); err != nil {
				return err
			}
		}
	}
	return nil
}

// Scheduling uses a deterministic quarter-hour key and the existing transactional
// backup/operation enqueue, so repeated Sync calls cannot create duplicate points.
func (p *Core) scheduleCitusPoint(ctx context.Context, rc *engine.Context, s *state) (bool, error) {
	if !rc.Cluster.Spec.Backup.PITR.Enabled {
		return false, nil
	}
	bs, err := loadBackupState(rc.Cluster)
	if err != nil {
		return false, err
	}
	if bs.ManagedRolesHeld || s.CitusOperation != "" || s.Rebalance != nil {
		return false, nil
	}
	items, err := rc.Store.ListBackups(ctx, rc.Cluster.Ref())
	if err != nil {
		return false, err
	}
	storage, err := p.backupStorage(ctx, rc, rc.Cluster.Spec.Backup.Storage, rc.Cluster.Namespace)
	if err != nil {
		return false, err
	}
	var base *domain.Backup
	for i := range items {
		b := &items[i]
		if b.Phase != domain.PhaseSucceeded || b.Postgres == nil || len(b.Postgres.Groups) == 0 || b.CompletedAt == nil || b.Storage != rc.Cluster.Spec.Backup.Storage {
			continue
		}
		if bs.LineageStart != nil && b.CompletedAt.Before(*bs.LineageStart) {
			continue
		}
		if !matchingCitusBases(storage, rc.Cluster, s, b.Postgres) {
			continue
		}
		if base == nil || b.CompletedAt.After(*base.CompletedAt) {
			base = b
		}
	}
	if base == nil {
		return false, nil
	}
	bucket := now(rc).UTC().Truncate(15 * time.Minute)
	if !base.CompletedAt.Before(bucket) {
		return false, nil
	}
	name := "point-" + secrets.Hash(s.UID + "/" + bucket.Format(time.RFC3339))[:24]
	if _, err = rc.Store.GetBackup(ctx, rc.Cluster.Ref(), name); err == nil {
		return false, nil
	} else if !missing(err) {
		return false, err
	}
	b := *base
	profiles, err := p.selectedProfileRequirements(rc.Cluster.Spec.EngineOptions)
	if err != nil {
		return false, err
	}
	metadata := *base.Postgres
	metadata.Profiles = normalizedProfileRequirements(append(profiles, metadata.Profiles...))
	metadata.Groups = append([]domain.PostgresBackupGroup(nil), metadata.Groups...)
	for i := range metadata.Groups {
		metadata.Groups[i].Backup.Profiles = normalizedProfileRequirements(append(append([]string(nil), metadata.Profiles...), metadata.Groups[i].Backup.Profiles...))
	}
	b.Postgres = &metadata
	b.Name = name
	b.OperationID = ""
	b.Trigger = "pitr"
	b.Schedule = ""
	b.Phase = domain.PhasePending
	b.CreatedAt = now(rc)
	b.StartedAt = nil
	b.CompletedAt = nil
	b.LastWriteTime = nil
	b.Error = ""
	payload, _ := json.Marshal(map[string]string{"backup": name})
	op := &domain.Operation{Namespace: b.Namespace, Cluster: b.Cluster, Kind: domain.OperationBackup, Payload: payload}
	return false, rc.Store.EnqueueBackup(ctx, &b, op)
}

// A point may only reuse bases from exactly this formation and repository
// incarnation. Group IDs alone survive destructive worker retirement.
func matchingCitusBases(storage *domain.BackupStorage, cluster *domain.Cluster, s *state, bases *domain.PostgresBackup) bool {
	groups := s.groups()
	if bases == nil || len(bases.Groups) != len(groups) {
		return false
	}
	seen := make(map[int]bool, len(groups))
	for _, base := range bases.Groups {
		if seen[base.Group] || base.Backup.Label == "" {
			return false
		}
		found := false
		for _, g := range groups {
			if g.id == base.Group && base.Backup.Repository == groupRepoPrefix(storage, cluster, s, g.id) {
				found = true
				break
			}
		}
		if !found {
			return false
		}
		seen[base.Group] = true
	}
	return true
}
