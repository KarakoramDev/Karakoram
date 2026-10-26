// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package postgres

import (
	"context"
	"fmt"

	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/postgres/pgspec"
)

// Replica rebuilds are independent once a new-major writer is available. A
// failed first replica must not keep every later replica stopped on the old
// major. Version and Offline are each member's durable publication receipt;
// the journal's old serial Index/Waiting fields are not recovery ownership.
func (p *Core) rebuildUpgradeReplicas(ctx context.Context, rc *engine.Context, s *state, request pgspec.UpgradeRequest) error {
	u := s.Upgrade
	writer, st, err := primary(ctx, rc, s)
	if err != nil {
		return fmt.Errorf("upgrade replica rebuild requires an available upgraded primary: %w", err)
	}
	if s.find(writer).Version != u.To || st.Version != "" && major(st.Version) != major(u.To) {
		return engine.Invalid("upgrade replica rebuild requires the new-major primary")
	}
	request.Replica = true
	allReady := true
	var firstErr error
	for _, m := range s.data() {
		if m.Name == u.Primary {
			continue
		}
		if m.Version != u.To || m.Offline {
			if err := upgradeAction(ctx, rc, m.Name, "upgrade-run", request); err != nil {
				allReady = false
				if firstErr == nil {
					firstErr = fmt.Errorf("upgrade replica %s requires rebuild recovery: %w", m.Name, err)
				}
				continue
			}
			m.Version, m.Offline, m.Stage = u.To, false, "deployed"
			m.ConfigVersion = s.ConfigVersion
			return nil // Persist the new-major fence before a configuration pull.
		}
		ok, err := ready(ctx, rc, m)
		if err != nil || !ok {
			allReady = false
			if err == nil {
				err = reloadPublished(ctx, rc, s, m)
			}
			if err != nil && firstErr == nil {
				firstErr = fmt.Errorf("upgrade replica %s is waiting for new-major readiness: %w", m.Name, err)
			}
			continue
		}
		m.Stage = "joined"
	}
	if allReady {
		u.Stage, u.Index, u.Waiting = "stanza", len(s.data()), false
	}
	return firstErr
}
