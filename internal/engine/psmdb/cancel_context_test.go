// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package psmdb

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/store"
)

type cancellationInventory struct {
	engine.ClusterStore
	ctx context.Context
	err error
}

func (s *cancellationInventory) GetBackup(ctx context.Context, _ domain.ClusterRef, _ string) (*domain.Backup, error) {
	s.ctx = ctx
	return nil, s.err
}

func TestCancellationInventoryUsesRequestContextAndSafeErrors(t *testing.T) {
	for _, cause := range []error{store.ErrNotFound, errors.New("private SQLite diagnostic"), context.Canceled} {
		t.Run(cause.Error(), func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if cause == context.Canceled {
				cancel()
			}
			inventory := &cancellationInventory{err: cause}
			p := &Provider{options: Options{Store: inventory}}
			op := &domain.Operation{ID: "task", Kind: domain.OperationBackup, Payload: json.RawMessage(`{"backup":"legacy-base"}`)}
			err := p.CanCancelOperation(ctx, op)
			if inventory.ctx != ctx {
				t.Error("inventory lookup ignored the request context")
			}
			if err == nil || !strings.Contains(err.Error(), "cannot verify backup inventory") || strings.Contains(err.Error(), cause.Error()) {
				t.Fatal("inventory failure did not produce a safe conflict", err)
			}
		})
	}
}
