// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package rabbitmq

import (
	"context"
	"errors"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/infra"
	"github.com/KarakoramDev/Karakoram/internal/infra/fake"
	"slices"
	"testing"
)

type relocationBackend struct {
	infra.Backend
	placement *fake.Backend
	specs     []infra.ServiceSpec
}

func (b *relocationBackend) Capabilities() infra.Capabilities {
	c := b.Backend.Capabilities()
	c.Placement = true
	return c
}
func (b *relocationBackend) ImportServices(ctx context.Context, specs []infra.ServiceSpec) (infra.Op, error) {
	b.specs = append(b.specs, specs...)
	if _, err := b.placement.ImportServices(ctx, specs); err != nil {
		return infra.Op{}, err
	}
	return b.Backend.ImportServices(ctx, specs)
}
func TestReplacementRelocation(t *testing.T) {
	for _, unsatisfiable := range []bool{false, true} {
		t.Run(map[bool]string{false: "avoid", true: "unsatisfiable"}[unsatisfiable], func(t *testing.T) {
			p, rc, _, _ := setup(t)
			ready(t, p, rc)
			groups := []string{"old-node", "new-node"}
			if unsatisfiable {
				groups = groups[:1]
			}
			b := &relocationBackend{Backend: rc.Backend, placement: fake.New(fake.Options{Capabilities: infra.Capabilities{Placement: true}, PlacementGroups: groups})}
			rc.Backend = b
			op := &domain.Operation{ID: "move", Kind: domain.OperationReplaceMember, Payload: []byte(`{"member":"demom1","relocate":true,"avoidGroup":"old-node"}`)}
			failed := false
			complete := false
			for range 120 {
				result, err := p.RunOperation(t.Context(), rc, op)
				op.Checkpoint = result.Checkpoint
				if err != nil {
					if !unsatisfiable || !errors.Is(err, infra.ErrPlacementUnsatisfiable) {
						t.Fatal(err)
					}
					if !result.Done {
						t.Fatal("failure reported before rollback completed")
					}
					failed = true
					break
				}
				if result.Done {
					complete = true
					break
				}
				_, err = p.Sync(t.Context(), rc)
				if err != nil && (!unsatisfiable || !errors.Is(err, infra.ErrPlacementUnsatisfiable)) {
					t.Fatal(err)
				}
				s := state(t, rc)
				if unsatisfiable {
					old := s.find("demom1")
					if old == nil || old.Removing {
						t.Fatal("old member retired on unsatisfiable placement")
					}
					if s.Retry != nil {
						t.Fatal("definitive placement failure retried")
					}
				}
			}
			if unsatisfiable != failed || (!unsatisfiable && !complete) {
				t.Fatalf("failed=%v complete=%v state=%s", failed, complete, rc.Cluster.Internal)
			}
			found := false
			for _, spec := range b.specs {
				if spec.Kind == infra.KindVolume {
					found = true
					if !slices.Equal(spec.SpreadAvoid, []string{"old-node"}) {
						t.Fatalf("avoid=%v", spec.SpreadAvoid)
					}
				}
			}
			if !found {
				t.Fatal("replacement volume never submitted")
			}
			if unsatisfiable {
				s := state(t, rc)
				if len(s.Members) != 3 || s.ReplaceActive || s.find("demom1") == nil {
					t.Fatalf("rollback incomplete: %+v", s)
				}
			}
		})
	}
}
