// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package psmdb

import (
	"context"
	"fmt"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/psmdb/pbm"
	"github.com/KarakoramDev/Karakoram/internal/engine/psmdb/users"
)

var _ engine.PITRRestoreValidator = (*Provider)(nil)
var _ engine.FCVValidator = (*Provider)(nil)
var _ engine.DatabasePasswordValidator = (*Provider)(nil)

// ValidatePITRRestore checks live source coverage without accepting recovery work.
func (p *Provider) ValidatePITRRestore(ctx context.Context, rc *engine.Context, target time.Time) ([]domain.TimeWindow, error) {
	windows, err := p.PITRWindow(ctx, rc)
	if err != nil {
		return windows, err
	}
	if len(windows) == 0 {
		return windows, domain.Conflict("cluster", rc.Cluster.Name, domain.ReasonInvalidState, engine.ErrConflict,
			"source.pitrTime cannot be restored: cluster %q has no PITR coverage; wait for initialization, a successful base snapshot and continuous oplog history", rc.Cluster.Name)
	}
	return windows, pbm.ValidatePITRTarget(windows, target, now(rc))
}

// ValidateFCV requires a major-form version and a completed binary rollout.
func (p *Provider) ValidateFCV(ctx context.Context, rc *engine.Context, version string) error {
	accepted := major(rc.Cluster.Spec.Version)
	if version != accepted {
		err := domain.InvalidField("version", domain.ReasonInvalid, "version must equal the deployed engine major in major form (%q), not a package version", accepted)
		err.Err = engine.ErrInvalid
		return err
	}
	s, err := load(rc.Cluster)
	if err != nil {
		return err
	}
	blocked := func() error {
		return domain.Conflict("cluster", rc.Cluster.Name, domain.ReasonInvalidState, engine.ErrConflict,
			"version %q cannot be promoted until the binary rollout finishes and all members report deployed version %s", version, rc.Cluster.Spec.Version)
	}
	if s.Phase != "ready" || s.Pending != nil || s.Retry != nil || len(s.Members) == 0 {
		return blocked()
	}
	for _, m := range s.Members {
		if m.Removing || m.Stage != "active" || m.Version != rc.Cluster.Spec.Version {
			return blocked()
		}
		st, err := agentStatus(ctx, rc, m.Name)
		if err != nil {
			return fmt.Errorf("observe member %s before FCV promotion: %w: %w", m.Name, engine.ErrUnavailable, err)
		}
		if st.EngineVersion != rc.Cluster.Spec.Version {
			return blocked()
		}
	}
	return nil
}

// ValidateDatabasePassword checks MongoDB SCRAM's SASLprep password profile.
func (*Provider) ValidateDatabasePassword(password string) error {
	return users.ValidatePassword(password)
}
