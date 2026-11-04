// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package mysql

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
)

// UpgradeBackupReady admits the prerequisite backup while a major upgrade is
// parked before its first side effect. The last published configuration digest
// must match exactly after reverting only the desired version; this is not an
// exception for arbitrary unobserved changes or other maintenance.
func (p *Core) UpgradeBackupReady(ctx context.Context, c *domain.Cluster, secrets engine.SecretStore) bool {
	if c.Deleting || secrets == nil || c.Spec.Provider != p.Name() {
		return false
	}
	s, err := load(c)
	if err != nil || s.Rolling == nil || s.ConfigHash == "" || s.Suspended || s.Pending != nil || s.Replacement != nil || s.TLSRotation != nil || s.GaleraTLS != nil || s.Formation.State != "formed" || s.Formation.reforming() {
		return false
	}
	u := s.Rolling
	if !u.Major || u.Stage != "backup" || u.Index != 0 || u.Op != nil || u.RequestedAt.IsZero() || u.To != c.Spec.Version || !upgradeAllowed(u.From, u.To) {
		return false
	}
	if len(s.data()) == 0 {
		return false
	}
	for _, m := range s.Members {
		if m.Stage != "joined" || m.Removing || m.Offline || m.Retirement != "" || m.Version != u.From {
			return false
		}
	}
	spec := c.Spec
	spec.Version = u.From
	raw, err := json.Marshal(spec)
	if err != nil {
		return false
	}
	managed, err := secrets.GetClusterSecret(ctx, c.Ref(), "mysql/access/proxy-users")
	if err != nil && !missing(err) {
		return false
	}
	digest := sha256.New()
	digest.Write(raw)
	digest.Write(managed)
	return hex.EncodeToString(digest.Sum(nil)) == s.ConfigHash
}
