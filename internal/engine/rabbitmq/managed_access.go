// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package rabbitmq

import (
	"context"
	"fmt"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/engine"
)

const managedAccessReconcileInterval = time.Minute

// Native credentials are mutable independently of sealed desired secrets. Use
// the local authenticated control channel so repair does not depend on a working
// management account. Only the reserved users and their default-vhost permissions
// are authoritative; customer accounts and other vhosts are not reconciled.
func (p *Provider) reconcileManagedAccess(ctx context.Context, rc *engine.Context, s *internalState) error {
	if !s.Bootstrapped {
		return nil
	}
	// A rotation deliberately publishes a staged credential before committing
	// it cluster-wide. Do not overwrite that in-flight native change.
	for _, m := range s.Members {
		if m.ChangeAction == "password" {
			return nil
		}
	}
	if !s.SystemUsersReconciledAt.IsZero() && now(rc).Before(s.SystemUsersReconciledAt.Add(managedAccessReconcileInterval)) {
		return nil
	}
	members := s.runningMembers()
	if len(members) == 0 {
		return nil
	}
	// Do not use brokerStatus here: management authentication is precisely
	// what this path must repair. A minority outage must not pin repair to the
	// first member either. The CLI itself enforces native metadata quorum.
	for _, selected := range members {
		st, err := p.agentStatus(ctx, rc, selected.Name)
		if err != nil || !running(st) || st.Maintenance {
			continue
		}
		// Rotation staging advances the global epoch, but only its owner
		// receives that version. Publish a stale peer's epoch in a separate
		// persisted Sync step, fencing delayed pre-rotation pulls.
		if selected.ConfigVersion < s.ConfigVersion {
			selected.ConfigVersion = s.ConfigVersion
			return nil
		}
		memberName := selected.Name
		// Bootstrap reads the agent's local config. Reload committed desired
		// credentials and require acknowledgement before allowing native writes.
		a, repairErr := p.agent(ctx, rc, memberName)
		refreshed := false
		if repairErr == nil {
			repairErr = a.Reload(ctx)
			if repairErr == nil {
				probeContext, cancel := context.WithTimeout(ctx, engine.ObservationTimeout)
				status, err := a.Status(probeContext)
				cancel()
				repairErr = err
				refreshed = err == nil && status.ConfigVersion >= selected.ConfigVersion
			}
			a.Close()
		}
		if repairErr == nil && !refreshed {
			// A reachable peer can remain stale indefinitely. Keep its safety
			// fence, but do not let it starve repair through another broker.
			continue
		}
		if repairErr == nil {
			repairErr = p.action(ctx, rc, memberName, "bootstrap", nil)
		}
		if repairErr != nil {
			s.SystemUsersError = fmt.Sprintf("Managed RabbitMQ users karakoram/admin or permissions on vhost / could not be restored through %s: %v. Restore member/quorum reachability; reconciliation will retry without changing customer accounts.", memberName, repairErr)
			return fmt.Errorf("RabbitMQ managed-access repair through %s: %w", memberName, repairErr)
		}
		s.SystemUsersReconciledAt = now(rc)
		s.SystemUsersError = ""
		return nil
	}
	// No safe native repair was possible. Runtime/rolling recovery can proceed;
	// do not advance the success timestamp or discard an existing repair error.
	return nil
}
