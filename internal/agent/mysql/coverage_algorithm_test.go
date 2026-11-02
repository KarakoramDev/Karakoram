// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package mysql

import (
	"context"

	"github.com/KarakoramDev/Karakoram/internal/engine/mysql/myspec"
	"github.com/minio/minio-go/v7"
)

// Synchronous driver for shared planning/lineage unit tests. Production HTTP
// observations use only the persisted agent-owned refresh service.
type coverageAlgorithmCache struct{ previous *replayCoverageCache }

func (e *coverageAlgorithmCache) cachedReplayCoverage(ctx context.Context, c *minio.Client, source myspec.Repository, baseline myspec.BackupResult) (myspec.PITRCoverageResponse, error) {
	if err := ctx.Err(); err != nil {
		return myspec.PITRCoverageResponse{}, err
	}
	key := requestDigest([]any{source, baseline})
	previous := e.previous
	if previous != nil && previous.key != key {
		previous = nil
	}
	plan, err := planReplayCoverage(ctx, c, source, baseline, previous)
	if err != nil {
		return myspec.PITRCoverageResponse{}, err
	}
	for _, ref := range plan.refs {
		if err = ctx.Err(); err != nil {
			return myspec.PITRCoverageResponse{}, err
		}
		log, err := readReplaySidecar(ctx, c, source, ref.key)
		if err != nil {
			return myspec.PITRCoverageResponse{}, err
		}
		if err = plan.cache.state.consume(log, baseline); err != nil {
			return myspec.PITRCoverageResponse{}, err
		}
	}
	plan.cache.key = key
	e.previous = plan.cache
	return plan.cache.state.response, nil
}
