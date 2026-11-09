// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package mysql

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/infra"
)

type safetyPlacementBackend struct {
	*fakeBackend
	eligible []string
	groups   map[string]string
	imports  []infra.ServiceSpec
}

func (b *safetyPlacementBackend) PlacementSnapshot(context.Context) (infra.PlacementSnapshot, error) {
	return infra.PlacementSnapshot{Eligible: b.eligible}, nil
}
func (b *safetyPlacementBackend) Containers(ctx context.Context, name string) ([]infra.Container, error) {
	out, err := b.fakeBackend.Containers(ctx, name)
	if group := b.groups[name]; group != "" {
		for i := range out {
			out[i].PlacementGroup = group
		}
	}
	return out, err
}
func (b *safetyPlacementBackend) ImportServices(ctx context.Context, specs []infra.ServiceSpec) (infra.Op, error) {
	for _, spec := range specs {
		if spec.Kind != infra.KindVolume {
			continue
		}
		node := ""
		for _, candidate := range b.eligible {
			if !slices.Contains(spec.SpreadAvoid, candidate) {
				node = candidate
				break
			}
		}
		if node == "" {
			return infra.Op{}, infra.ErrPlacementUnsatisfiable
		}
		for _, runtime := range specs {
			if runtime.Volume == spec.Hostname {
				b.groups[runtime.Hostname] = node
			}
		}
	}
	b.imports = append(b.imports, specs...)
	return b.fakeBackend.ImportServices(ctx, specs)
}

type placementSafetyEvents struct{ events []domain.Event }

func (e *placementSafetyEvents) Record(_ context.Context, event domain.Event) error {
	e.events = append(e.events, event)
	return nil
}

func TestVotingFailureDomainsDetectAndRepair(t *testing.T) {
	for _, provider := range []string{"mysql", "pxc"} {
		t.Run(provider, func(t *testing.T) {
			var p *Core
			var rc *engine.Context
			var base *fakeBackend
			if provider == "pxc" {
				p, rc, base = setupPXC(t)
			} else {
				p, rc, base = setup(t, "group")
			}
			converge(t, p, rc, base)
			b := &safetyPlacementBackend{fakeBackend: base, eligible: []string{"manager", "worker-a", "worker-b"}, groups: map[string]string{"demom0": "worker-a", "demom1": "worker-b", "demom2": "worker-b"}}
			events := &placementSafetyEvents{}
			rc.Backend, rc.Events = b, events
			status, err := p.Status(t.Context(), rc)
			if err != nil {
				t.Fatal(err)
			}
			found := false
			for _, condition := range status.Conditions {
				if condition.Type == "FailureDomainsSeparated" && condition.Status == "False" && condition.Reason == "SharedNode" {
					found = true
				}
			}
			if !found || status.Phase == domain.PhaseReady || len(events.events) != 0 {
				t.Fatalf("missing co-location condition or uncommitted event: %+v, events=%+v", status, events.events)
			}
			rc.Cluster.Status = status
			if _, err := p.Status(t.Context(), rc); err != nil || len(events.events) != 0 {
				t.Fatalf("observation emitted an uncommitted event: %v, %+v", err, events.events)
			}
			op := &domain.Operation{ID: "repair", Kind: domain.OperationReplaceMember, Payload: json.RawMessage(`{"member":"demom2"}`)}
			runDay2Operation(t, rc, base, op)
			imported := false
			for _, spec := range b.imports {
				if spec.Kind == infra.KindVolume {
					imported = true
					if !slices.Contains(spec.SpreadAvoid, "worker-a") || !slices.Contains(spec.SpreadAvoid, "worker-b") || spec.SpreadReplaces != "demom2v" {
						t.Fatalf("unsafe repair import: %+v", spec)
					}
				}
			}
			if !imported || b.groups["demom3"] != "manager" {
				t.Fatalf("repair did not use free node: %v", b.groups)
			}
			status, err = p.Status(t.Context(), rc)
			if err != nil {
				t.Fatal(err)
			}
			found = false
			for _, condition := range status.Conditions {
				if condition.Type == "FailureDomainsSeparated" && condition.Status == "True" {
					found = true
				}
			}
			if !found {
				t.Fatalf("completed repair not observed: %+v", status.Conditions)
			}
		})
	}
}

func TestReplacementPlacementLegacyAllocationAndRefusal(t *testing.T) {
	for _, legacy := range []bool{false, true} {
		t.Run(fmt.Sprintf("legacy=%v", legacy), func(t *testing.T) {
			p, rc, base := setup(t, "group")
			converge(t, p, rc, base)
			b := &safetyPlacementBackend{fakeBackend: base, eligible: []string{"manager", "worker-a", "worker-b"}, groups: map[string]string{"demom0": "worker-a", "demom1": "worker-b", "demom2": "worker-b"}}
			rc.Backend = b
			op := &domain.Operation{ID: "repair", Kind: domain.OperationReplaceMember, Payload: json.RawMessage(`{"member":"demom2"}`)}
			if !legacy {
				b.eligible = []string{"worker-a", "worker-b"}
				before, _ := load(rc.Cluster)
				if _, err := p.RunOperation(t.Context(), rc, op); !errors.Is(err, infra.ErrPlacementUnsatisfiable) {
					t.Fatalf("exhausted nodes did not refuse replacement: %v", err)
				}
				after, _ := load(rc.Cluster)
				if after.Replacement != nil || after.NextIndex != before.NextIndex {
					t.Fatal("refused replacement allocated an identity")
				}
				return
			}
			if _, err := p.RunOperation(t.Context(), rc, op); err != nil {
				t.Fatal(err)
			}
			s, _ := load(rc.Cluster)
			next := s.find(s.Replacement.New)
			raw, _ := json.Marshal(next)
			var fields map[string]json.RawMessage
			if err := json.Unmarshal(raw, &fields); err != nil {
				t.Fatal(err)
			}
			delete(fields, "replacesVolume")
			delete(fields, "placementAvoid")
			raw, _ = json.Marshal(fields)
			var oldSchema member
			if err := json.Unmarshal(raw, &oldSchema); err != nil {
				t.Fatal(err)
			}
			*next = oldSchema
			if err := save(rc.Cluster, s); err != nil {
				t.Fatal(err)
			}
			// Reconstruct the control plane with only the legacy durable allocation.
			p = New(Options{})
			for range 30 {
				if _, err := p.Sync(t.Context(), rc); err != nil {
					t.Fatal(err)
				}
				s, _ = load(rc.Cluster)
				next = s.find(next.Name)
				if next.ImportBatch != nil {
					break
				}
			}
			if next.ImportBatch == nil {
				t.Fatal("legacy replacement did not freeze")
			}
			for _, spec := range next.ImportBatch.Specs {
				if spec.Kind == infra.KindVolume && (spec.SpreadReplaces != "demom2v" || !slices.Contains(spec.SpreadAvoid, "worker-a") || !slices.Contains(spec.SpreadAvoid, "worker-b")) {
					t.Fatalf("legacy allocation froze unsafe placement: %+v", spec)
				}
			}
		})
	}
}
