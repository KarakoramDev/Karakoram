// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package psmdb

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/infra"
)

type placementMoveBackend struct {
	*fakeInfra
	reject   bool
	attempts int
}

func (b *placementMoveBackend) Capabilities() infra.Capabilities {
	c := b.fakeInfra.Capabilities()
	c.Placement = true
	return c
}
func (b *placementMoveBackend) ImportServices(ctx context.Context, specs []infra.ServiceSpec) (infra.Op, error) {
	b.attempts++
	if b.reject {
		return infra.Op{}, fmt.Errorf("scheduler: %w", infra.ErrPlacementUnsatisfiable)
	}
	return b.fakeInfra.ImportServices(ctx, specs)
}

func TestMovePlacementRejectedCleansOnlyReplacement(t *testing.T) {
	p, rc, base, _ := setup(t)
	b := &placementMoveBackend{fakeInfra: base, reject: true}
	rc.Backend = b
	s, err := load(rc.Cluster)
	if err != nil {
		t.Fatal(err)
	}
	plan(rc.Cluster, s)
	for _, m := range s.Members {
		m.Stage = "active"
		m.Version = rc.Cluster.Spec.Version
	}
	old := s.Members[0]
	if err := save(rc.Cluster, s); err != nil {
		t.Fatal(err)
	}
	op := &domain.Operation{Kind: domain.OperationReplaceMember, Payload: json.RawMessage(`{"member":"` + old.Name + `","relocate":true,"avoidGroup":"node-a"}`)}
	result, err := p.RunOperation(t.Context(), rc, op)
	if err != nil {
		t.Fatal(err)
	}
	op.Checkpoint = result.Checkpoint
	s, _ = load(rc.Cluster)
	var cp operationState
	_ = json.Unmarshal(op.Checkpoint, &cp)
	next := s.find(cp.NewMember)
	if err := p.ensureSecrets(t.Context(), rc, s); err != nil {
		t.Fatal(err)
	}
	if err := p.importMember(t.Context(), rc, s, next); err != nil {
		t.Fatal(err)
	}
	// Persist and reload the frozen batch before replaying it.
	if err := save(rc.Cluster, s); err != nil {
		t.Fatal(err)
	}
	s, _ = load(rc.Cluster)
	next = s.find(cp.NewMember)
	err = p.importMember(t.Context(), rc, s, next)
	if !errors.Is(err, infra.ErrPlacementUnsatisfiable) || !strings.Contains(err.Error(), "no other eligible node for "+old.Name) {
		t.Fatal(err)
	}
	if err := save(rc.Cluster, s); err != nil {
		t.Fatal(err)
	}
	if _, err := p.CancelOperation(t.Context(), rc, op); err != nil {
		t.Fatal(err)
	}
	s, _ = load(rc.Cluster)
	if s.find(old.Name).Removing || s.find(old.Name).Stage != "active" || !s.find(cp.NewMember).Removing || s.find(cp.NewMember).Stage != "delete-volume" || s.Retry != nil || b.attempts != 1 {
		t.Fatal("placement rejection touched predecessor or retained retry", s)
	}
}
