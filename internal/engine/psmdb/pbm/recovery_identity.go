// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package pbm

import (
	"context"
	"errors"
	"fmt"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/secrets"
	"github.com/KarakoramDev/Karakoram/internal/store"
)

// ensureRecoveryIdentity resolves physical snapshot authentication before any
// restore side effects. Legacy snapshots can use the still-existing source's
// userAdmin credential; protect it independently before replacing target data.
func ensureRecoveryIdentity(ctx context.Context, env Env, b *domain.Backup) error {
	if b.Type != "physical" {
		return nil
	}
	source := domain.ClusterRef{Namespace: b.Namespace, Name: b.Cluster}
	if b.RecoveryIdentity != "" {
		pw, err := env.RC.Secrets.GetClusterSecret(ctx, domain.ClusterRef{Namespace: b.Namespace, Name: b.RecoveryIdentity}, "users/userAdmin")
		if err != nil {
			return fmt.Errorf("physical backup %s/%s recovery identity is unavailable: %w", b.Cluster, b.Name, err)
		}
		if len(pw) == 0 {
			return fmt.Errorf("%w: physical backup %s/%s recovery identity has no userAdmin credential", engine.ErrConflict, b.Cluster, b.Name)
		}
		return nil
	}

	// Keep a prior protected value if persistence of the backup record failed.
	// A retry must not replace it with a subsequently rotated source password.
	identity := "backup-" + secrets.Hash(b.Cluster + "/" + b.Storage + "/" + b.PBMPrefix + "/" + b.Name)[:32]
	recovery := domain.ClusterRef{Namespace: b.Namespace, Name: identity}
	pw, err := env.RC.Secrets.GetClusterSecret(ctx, recovery, "users/userAdmin")
	if errors.Is(err, engine.ErrNotFound) || errors.Is(err, store.ErrNotFound) {
		if source.Key() != env.RC.Cluster.Ref().Key() {
			if _, err := env.RC.Store.GetCluster(ctx, source); err != nil {
				return fmt.Errorf("legacy physical backup %s/%s has no recovery identity and its source cluster is unavailable; restore rejected before modifying the target: %w", b.Cluster, b.Name, err)
			}
		}
		pw, err = env.RC.Secrets.GetClusterSecret(ctx, source, "users/userAdmin")
		if err != nil {
			return fmt.Errorf("legacy physical backup %s/%s has no recovery identity and its source userAdmin credential is unavailable; restore rejected before modifying the target: %w", b.Cluster, b.Name, err)
		}
		if len(pw) > 0 {
			if err := env.RC.Secrets.SetClusterSecret(ctx, recovery, "users/userAdmin", pw); err != nil {
				return fmt.Errorf("protect legacy physical backup %s/%s recovery identity: %w", b.Cluster, b.Name, err)
			}
		}
	} else if err != nil {
		return fmt.Errorf("read legacy physical backup %s/%s recovery identity: %w", b.Cluster, b.Name, err)
	}
	if len(pw) == 0 {
		return fmt.Errorf("%w: legacy physical backup %s/%s has no recoverable userAdmin credential; restore rejected before modifying the target", engine.ErrConflict, b.Cluster, b.Name)
	}
	b.RecoveryIdentity = identity
	if err := env.RC.Store.UpdateBackup(ctx, b); err != nil {
		return fmt.Errorf("persist legacy physical backup %s/%s recovery identity: %w", b.Cluster, b.Name, err)
	}
	return nil
}
