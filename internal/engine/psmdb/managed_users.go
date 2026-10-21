// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package psmdb

import (
	"context"
	"errors"
	"fmt"

	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/psmdb/users"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

// reconcileManagedUsers repairs only provider-reserved accounts and the PBM
// role. Bootstrap completion and desired-secret hashes are not native evidence.
func (p *Provider) reconcileManagedUsers(ctx context.Context, rc *engine.Context, s *internalState) error {
	passwords, err := passwords(ctx, rc)
	if err != nil {
		return err
	}
	for set, state := range s.Sets {
		if state.Stage != "ready" {
			continue
		}
		readCtx, cancel := context.WithTimeout(ctx, observationReadBudget)
		client, err := p.dial(readCtx, rc, s, set, "userAdmin")
		if err != nil && (errors.Is(err, context.DeadlineExceeded) || errors.Is(readCtx.Err(), context.DeadlineExceeded)) {
			err = fmt.Errorf("PSMDB managed-user connection read exceeded %s budget: inspect set %s member responsiveness/connectivity: %w", observationReadBudget, set, context.DeadlineExceeded)
		}
		cancel()
		if err != nil {
			if managedUsersTransient(err) {
				return fmt.Errorf("managed system users in %s await replica-set connectivity and election recovery: %w", set, err)
			}
			return fmt.Errorf("managed system users in %s cannot be reconciled: verify the retained userAdmin credential, privileges, quorum and connectivity; restore userAdmin access using a surviving authorized administrator: %w", set, err)
		}
		err = users.EnsureSystemUsers(ctx, client, passwords)
		_ = client.Disconnect(ctx)
		if err != nil {
			if managedUsersTransient(err) {
				return fmt.Errorf("managed system users in %s await replica-set connectivity and election recovery: %w", set, err)
			}
			return fmt.Errorf("managed system users in %s require repair; verify userAdmin privileges: %w", set, err)
		}
	}
	return p.reconcileDataAccessUsers(ctx, rc, s)
}

// Inspect typed server errors, including write-concern errors nested in a
// WriteException. A stepdown does not establish broken credentials or grants.
func managedUsersTransient(err error) bool {
	var server mongo.ServerError
	if errors.As(err, &server) {
		// Authentication/authorization failures must stay actionable even if
		// accompanied by a retry label.
		if server.HasErrorCode(13) || server.HasErrorCode(18) {
			return false
		}
		for _, code := range []int{6, 7, 89, 91, 189, 9001, 10107, 11600, 11602, 13435, 13436} {
			if server.HasErrorCode(code) {
				return true
			}
		}
		if server.HasErrorLabel("RetryableWriteError") {
			return true
		}
	}
	return mongo.IsNetworkError(err) || mongo.IsTimeout(err) ||
		errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
}
