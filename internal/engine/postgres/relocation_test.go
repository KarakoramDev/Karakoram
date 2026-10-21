// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package postgres

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

type moveBackend struct {
	*fakeBackend
	reject   bool
	attempts int
}

func (b *moveBackend) ImportServices(ctx context.Context, specs []infra.ServiceSpec) (infra.Op, error) {
	b.attempts++
	if b.reject {
		return infra.Op{}, fmt.Errorf("scheduler: %w", infra.ErrPlacementUnsatisfiable)
	}
	return b.fakeBackend.ImportServices(ctx, specs)
}
func TestReplacementRelocationJournalAndPlacement(t *testing.T) {
	for _, relocate := range []bool{false, true} {
		t.Run(fmt.Sprint(relocate), func(t *testing.T) {
			p, rc, b := setup(t, "ha")
			converge(t, p, rc)
			payload, _ := json.Marshal(map[string]any{"member": "demop1", "relocate": relocate, "avoidGroup": "node-a"})
			op := &domain.Operation{ID: "move", Kind: domain.OperationReplaceMember, Payload: payload}
			result, err := p.RunOperation(t.Context(), rc, op)
			if err != nil {
				t.Fatal(err)
			}
			op.Checkpoint = result.Checkpoint
			var cp replacementState
			if err := json.Unmarshal(result.Checkpoint, &cp); err != nil {
				t.Fatal(err)
			}
			if cp.Relocate != relocate || cp.AvoidGroup != "node-a" {
				t.Fatal("lost checkpoint", cp)
			}
			// Resume with a fresh provider and no payload: durable state owns the intent.
			op.Payload = nil
			p = New(p.options)
			if _, err := p.RunOperation(t.Context(), rc, op); err != nil {
				t.Fatal(err)
			}
			for range 15 {
				if _, err := p.Sync(t.Context(), rc); err != nil {
					t.Fatal(err)
				}
				if b.services[cp.New+"v"] != nil {
					break
				}
			}
			found := false
			for _, spec := range b.imports {
				if spec.Hostname == cp.New+"v" {
					found = true
					if relocate && (len(spec.SpreadAvoid) != 1 || spec.SpreadAvoid[0] != "node-a") || !relocate && len(spec.SpreadAvoid) != 0 {
						t.Fatal("wrong exclusion", spec.SpreadAvoid)
					}
				}
			}
			if !found {
				t.Fatal("replacement volume not imported")
			}
		})
	}
}
func TestMovePlacementRejectedRollsBackWithoutFencingOld(t *testing.T) {
	p, rc, base := setup(t, "ha")
	converge(t, p, rc)
	b := &moveBackend{fakeBackend: base, reject: true}
	rc.Backend = b
	op := &domain.Operation{ID: "move", Kind: domain.OperationReplaceMember, Payload: json.RawMessage(`{"member":"demop1","relocate":true,"avoidGroup":"node-a"}`)}
	result, err := p.RunOperation(t.Context(), rc, op)
	if err != nil {
		t.Fatal(err)
	}
	op.Checkpoint = result.Checkpoint
	rejected := false
	for range 15 {
		_, err = p.Sync(t.Context(), rc)
		if err != nil {
			rejected = true
			break
		}
	}
	if !rejected || !errors.Is(err, infra.ErrPlacementUnsatisfiable) || !strings.Contains(err.Error(), "no other eligible node for demop1") {
		t.Fatal(err)
	}
	if _, err := p.CancelOperation(t.Context(), rc, op); err != nil {
		t.Fatal(err)
	}
	for range 30 {
		if _, err := p.Sync(t.Context(), rc); err != nil {
			t.Fatal(err)
		}
		s, _ := load(rc.Cluster)
		if s.Replacement == nil {
			break
		}
	}
	s, _ := load(rc.Cluster)
	if s.Replacement != nil || s.find("demop1") == nil || s.find("demop1").Retirement != "" || s.find("demop3") != nil || base.services["demop1"] == nil || base.held["demop1"] || b.attempts != 1 {
		t.Fatal("failed move did not roll back safely", s.Replacement, base.actions)
	}
}

func TestDistributedRelocationKeepsWorkerReplicationGroup(t *testing.T) {
	p, rc, _, _ := setupDistributed(t, 3)
	s := converge(t, p, rc)
	old := s.group(1).data()[1]
	if err := p.ValidateMemberReplacement(rc.Cluster, old.Name); err != nil {
		t.Fatal(err)
	}
	op := &domain.Operation{ID: "worker-move", Kind: domain.OperationReplaceMember, Payload: mustJSON(map[string]any{
		"member": old.Name, "relocate": true, "avoidGroup": "node-a",
	})}
	result, err := p.RunOperation(t.Context(), rc, op)
	if err != nil {
		t.Fatal(err)
	}
	var checkpoint replacementState
	if err := json.Unmarshal(result.Checkpoint, &checkpoint); err != nil {
		t.Fatal(err)
	}
	current, err := load(rc.Cluster)
	if err != nil {
		t.Fatal(err)
	}
	replacement := current.find(checkpoint.New)
	if replacement == nil || replacement.Group != old.Group || !checkpoint.Relocate || checkpoint.AvoidGroup != "node-a" {
		t.Fatalf("worker relocation lost group or placement intent: %+v %+v", replacement, checkpoint)
	}
	if len(current.group(0).data()) != len(s.group(0).data()) {
		t.Fatal("worker relocation allocated a coordinator")
	}
}
