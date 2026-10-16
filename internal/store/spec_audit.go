// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package store

import (
	"context"
	"database/sql"
	"encoding/json"

	"github.com/KarakoramDev/Karakoram/internal/domain"
)

type specAuditKey struct{}

// SpecSecretChange records only presence transitions, never secret material.
type SpecSecretChange struct {
	Key    string
	WasSet bool
}

// SpecUpdateAudit builds and persists the audit alongside the desired-state write.
// Event is populated only after commit, for publication to subscribers.
type SpecUpdateAudit struct {
	Build func(domain.ClusterSpec, domain.ClusterSpec, int64, []SpecSecretChange) domain.Event
	Event *domain.Event
}

func WithSpecUpdateAudit(ctx context.Context, audit *SpecUpdateAudit) context.Context {
	return context.WithValue(ctx, specAuditKey{}, audit)
}

func specAuditBefore(ctx context.Context, tx *sql.Tx, ref domain.ClusterRef) (domain.ClusterSpec, error) {
	var before domain.ClusterSpec
	if ctx.Value(specAuditKey{}) == nil {
		return before, nil
	}
	var data []byte
	err := tx.QueryRowContext(ctx, "SELECT spec FROM clusters WHERE namespace=? AND name=?", domain.NormalizedNamespace(ref.Namespace), ref.Name).Scan(&data)
	if err != nil {
		return before, typed(err)
	}
	err = json.Unmarshal(data, &before)
	return before, err
}

func commitSpecAudit(ctx context.Context, tx *sql.Tx, ref domain.ClusterRef, before, after domain.ClusterSpec, secrets []SpecSecretChange) error {
	audit, _ := ctx.Value(specAuditKey{}).(*SpecUpdateAudit)
	var event *domain.Event
	if audit != nil {
		var generation int64
		if err := tx.QueryRowContext(ctx, "SELECT generation FROM clusters WHERE namespace=? AND name=?", domain.NormalizedNamespace(ref.Namespace), ref.Name).Scan(&generation); err != nil {
			return err
		}
		built := audit.Build(before, after, generation, secrets)
		event = &built
		if err := appendEvent(ctx, tx, event); err != nil {
			return err
		}
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	if audit != nil {
		audit.Event = event
	}
	return nil
}
