// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package psmdb

import (
	"context"
	"fmt"

	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/psmdb/mongoadmin"
)

func retainedPrimary(s *internalState, lost *member, status mongoadmin.Status) bool {
	if !status.HasWritablePrimary() {
		return false
	}
	for _, native := range status.Members {
		if native.State != "PRIMARY" || native.Health != 1 || native.Name == lost.Name+":27017" {
			continue
		}
		for _, candidate := range s.setMembers(lost.Set) {
			if !candidate.Removing && candidate.Name+":27017" == native.Name && !candidate.RuntimeRecovery.VolumeLost {
				return true
			}
		}
	}
	return false
}

// volumeRecoverySafe authorizes initial sync only from a freshly observed,
// retained writable primary. Desired replica count is not evidence of a copy.
func (p *Provider) volumeRecoverySafe(ctx context.Context, rc *engine.Context, s *internalState, lost *member) error {
	status, err := p.replication(ctx, rc, s, lost)
	if err != nil {
		return fmt.Errorf("%w: member %s lost its durable volume; no surviving writable primary can be verified; return retained members and quorum or restore a verified backup into a new cluster: %v", engine.ErrConflict, lost.Name, err)
	}
	if !retainedPrimary(s, lost, status) {
		return fmt.Errorf("%w: member %s lost its durable volume; no retained writable primary is available for lossless initial sync; return retained members and quorum or restore a verified backup into a new cluster", engine.ErrConflict, lost.Name)
	}
	return nil
}
