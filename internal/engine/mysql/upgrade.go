// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package mysql

import (
	"context"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"strings"
)

func mysqlMajor(version string) string {
	fields := strings.Split(version, ".")
	if len(fields) < 2 {
		return version
	}
	return strings.Join(fields[:2], ".")
}

func requireUpgradeBackup(ctx context.Context, rc *engine.Context, u *rollingState) error {
	if rc.Store == nil || rc.Cluster.Spec.Backup.Storage == "" {
		return engine.Invalid("major MySQL upgrade requires a successful current-version backup taken since the upgrade request")
	}
	backups, err := rc.Store.ListBackups(ctx, rc.Cluster.Ref())
	if err != nil {
		return err
	}
	for _, b := range backups {
		if b.ProviderMatches(rc.Cluster.Spec.Provider) && b.Phase == domain.PhaseSucceeded && b.SourceVersion == u.From && b.MySQL != nil && b.MySQL.Label != "" && b.StartedAt != nil && !b.StartedAt.Before(u.RequestedAt) && b.CompletedAt != nil {
			return nil
		}
	}
	return engine.Invalid("major MySQL upgrade requires a successful current-version backup taken since the upgrade request")
}
