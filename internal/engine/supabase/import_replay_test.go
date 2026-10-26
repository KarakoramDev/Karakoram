// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package supabase

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/postgres"
	"github.com/KarakoramDev/Karakoram/internal/infra"
	"github.com/KarakoramDev/Karakoram/internal/secrets"
)

type importReplayClock struct{ at time.Time }

func (c importReplayClock) Now() time.Time { return c.at }

type componentInterruptedImportBackend struct {
	*testBackend
	batches [][]infra.ServiceSpec
	fail    bool
	accept  bool
}

func (b *componentInterruptedImportBackend) ImportServices(ctx context.Context, specs []infra.ServiceSpec) (infra.Op, error) {
	raw, err := json.Marshal(specs)
	if err != nil {
		return infra.Op{}, err
	}
	var snapshot []infra.ServiceSpec
	if err := json.Unmarshal(raw, &snapshot); err != nil {
		return infra.Op{}, err
	}
	b.batches = append(b.batches, snapshot)
	op, err := b.testBackend.ImportServices(ctx, specs)
	if err != nil {
		return op, err
	}
	if b.fail {
		b.fail = false
		for _, spec := range specs {
			b.services[spec.Hostname].Importing = true
		}
		if b.accept {
			op.Kind = "incomplete-component-import"
			return op, nil
		}
		return infra.Op{}, errors.New("component setup interrupted after reservation")
	}
	return op, nil
}

func (b *componentInterruptedImportBackend) OpStatus(ctx context.Context, op infra.Op) (infra.OpState, error) {
	if op.Kind == "incomplete-component-import" {
		for name := range op.Services {
			if service := b.services[name]; service != nil && service.Importing {
				return infra.OpState{}, nil
			}
		}
	}
	return b.testBackend.OpStatus(ctx, op)
}

func (b *componentInterruptedImportBackend) DeleteServiceByID(ctx context.Context, name, id string) (infra.Op, error) {
	if service := b.services[name]; service != nil && service.Importing {
		return infra.Op{}, infra.ErrConflict
	}
	return b.testBackend.DeleteServiceByID(ctx, name, id)
}

func componentImportFixture(t *testing.T) (*Provider, *engine.Context, *componentInterruptedImportBackend, string) {
	t.Helper()
	p, rc, base := setupProvider(t)
	backend := &componentInterruptedImportBackend{testBackend: base, fail: true}
	rc.Backend = backend
	for name, desired := range rc.Cluster.Spec.Components.Extra {
		desired.Replicas = 0
		rc.Cluster.Spec.Components.Extra[name] = desired
	}
	desired := rc.Cluster.Spec.Components.Extra["rest"]
	desired.Replicas = 1
	rc.Cluster.Spec.Components.Extra["rest"] = desired
	name := rc.Cluster.Name + codes["rest"]
	token := "sealed-component-import-token"
	if err := rc.Secrets.SetAgentToken(t.Context(), &domain.AgentToken{Namespace: rc.Cluster.Namespace, Cluster: rc.Cluster.Name, Member: name, Token: token, Hash: secrets.Hash(token)}); err != nil {
		t.Fatal(err)
	}
	s := &componentState{Members: map[string]*componentMember{"rest": {Name: name, Component: "rest", Stage: engine.ComponentPlanned, Replicas: desired.Replicas, Resources: desired.Resources}}}
	if err := saveState(rc.Cluster, s); err != nil {
		t.Fatal(err)
	}
	return p, rc, backend, name
}

func TestComponentImportReplaysFrozenOriginalRequest(t *testing.T) {
	for _, disable := range []bool{false, true} {
		name := "resource-and-count-edit"
		if disable {
			name = "disable-during-import"
		}
		t.Run(name, func(t *testing.T) {
			p, rc, backend, member := componentImportFixture(t)
			if _, err := p.syncComponents(t.Context(), rc); err != nil {
				t.Fatal(err)
			}
			s, _ := loadState(rc.Cluster)
			plan := s.Members["rest"].ImportBatch
			if plan == nil || len(plan.Specs) != 1 || plan.Specs[0].Hostname != member || len(backend.batches) != 0 {
				t.Fatal("component import did not persist intent before submission")
			}
			if strings.Contains(string(rc.Cluster.Internal), "sealed-component-import-token") {
				t.Fatal("component import journal exposed its sealed agent token")
			}
			if _, err := p.syncComponents(t.Context(), rc); err == nil {
				t.Fatal("fixture did not leave an incomplete reservation")
			}
			if len(backend.batches) != 1 || !backend.services[member].Importing {
				t.Fatal("missing durable import reservation")
			}
			original := backend.batches[0]
			if _, err := p.syncComponents(t.Context(), rc); err == nil || len(backend.batches) != 1 {
				t.Fatal("failed component import retried before its durable cooldown")
			}
			rc.Clock = importReplayClock{at: time.Now().UTC().Add(2 * time.Minute)}
			desired := rc.Cluster.Spec.Components.Extra["rest"]
			desired.Replicas = 2
			desired.Resources.MinRAM += 1
			desired.Resources.MaxRAM += 1
			if disable {
				desired.Replicas = 0
			}
			rc.Cluster.Spec.Components.Extra["rest"] = desired
			p = New(postgres.Options{})
			if disable {
				// The core has not formed: disabled cleanup must still replay
				// its frozen import, without deploying the optional service.
				if _, err := p.Sync(t.Context(), rc); err != nil {
					t.Fatal(err)
				}
			} else if _, err := p.syncComponents(t.Context(), rc); err != nil {
				t.Fatal(err)
			}
			if len(backend.batches) != 2 || !reflect.DeepEqual(original, backend.batches[1]) || backend.deployed[member] {
				t.Fatalf("reservation advanced without exact original replay: %#v => %#v", original, backend.batches)
			}
			if disable {
				if _, err := p.Sync(t.Context(), rc); err != nil {
					t.Fatal(err)
				}
			} else if _, err := p.syncComponents(t.Context(), rc); err != nil {
				t.Fatal(err)
			}
			s, _ = loadState(rc.Cluster)
			m := s.Members["rest"]
			if m.Stage != engine.ComponentImported || m.ImportBatch != nil || s.Pending != nil {
				t.Fatalf("confirmed setup did not release import journal: %+v", m)
			}
		})
	}
}

func TestComponentImportDoesNotAdoptUnjournaledReservation(t *testing.T) {
	p, rc, backend, member := componentImportFixture(t)
	if _, err := p.syncComponents(t.Context(), rc); err != nil {
		t.Fatal(err)
	}
	if _, err := p.syncComponents(t.Context(), rc); err == nil {
		t.Fatal("fixture did not leave an incomplete reservation")
	}
	s, _ := loadState(rc.Cluster)
	s.Members["rest"].ImportBatch = nil
	if err := saveState(rc.Cluster, s); err != nil {
		t.Fatal(err)
	}
	if _, err := p.syncComponents(t.Context(), rc); err == nil || !strings.Contains(err.Error(), "no frozen batch") {
		t.Fatalf("incomplete component reservation was adopted or guessed: %v", err)
	}
	s, _ = loadState(rc.Cluster)
	if s.Members["rest"].Stage != engine.ComponentPlanned || backend.deployed[member] || len(backend.batches) != 1 {
		t.Fatal("unfinished reservation advanced into deployment or a changed import")
	}
}

func TestComponentCleanupResumesFrozenImportWithoutDeployment(t *testing.T) {
	for _, accepted := range []bool{false, true} {
		name := "lost-response"
		if accepted {
			name = "accepted-pending"
		}
		t.Run(name, func(t *testing.T) {
			p, rc, backend, member := componentImportFixture(t)
			backend.accept = accepted
			if _, err := p.syncComponents(t.Context(), rc); err != nil {
				t.Fatal(err)
			}
			_, err := p.syncComponents(t.Context(), rc)
			if (err == nil) != accepted {
				t.Fatalf("unexpected component import outcome: %v", err)
			}
			original := backend.batches[0]
			rc.Cluster.Deleting = true
			rc.Clock = importReplayClock{at: time.Now().UTC().Add(2 * time.Minute)}
			done := false
			for range 10 {
				done, err = p.Cleanup(t.Context(), rc, domain.DeleteOptions{})
				if err != nil {
					t.Fatal(err)
				}
				if done {
					break
				}
			}
			if !done || len(backend.batches) != 2 || !reflect.DeepEqual(original, backend.batches[1]) {
				t.Fatalf("component cleanup failed to drain original import: done=%t batches=%#v", done, backend.batches)
			}
			if backend.services[member] != nil || backend.deployed[member] {
				t.Fatal("cleanup deployed or retained the incomplete optional runtime")
			}
		})
	}
}

func TestAcceptedComponentImportResumesSetupBeforeWaitingOnHandle(t *testing.T) {
	p, rc, backend, _ := componentImportFixture(t)
	backend.accept = true
	for range 3 {
		if _, err := p.syncComponents(t.Context(), rc); err != nil {
			t.Fatal(err)
		}
	}
	if len(backend.batches) != 2 || !reflect.DeepEqual(backend.batches[0], backend.batches[1]) {
		t.Fatal("accepted component import waited forever instead of resuming setup")
	}
	if _, err := p.syncComponents(t.Context(), rc); err != nil {
		t.Fatal(err)
	}
	s, _ := loadState(rc.Cluster)
	if m := s.Members["rest"]; m.Stage != engine.ComponentImported || m.ImportBatch != nil {
		t.Fatalf("resumed accepted component import did not complete: %+v", m)
	}
}

func TestComponentCleanupDrainsRuntimeRecoveryImportPlan(t *testing.T) {
	for _, accepted := range []bool{false, true} {
		name := "persisted-plan"
		if accepted {
			name = "accepted-setup-reservation"
		}
		t.Run(name, func(t *testing.T) {
			p, rc, backend, member := componentImportFixture(t)
			backend.fail, backend.accept = accepted, accepted
			s, err := loadState(rc.Cluster)
			if err != nil {
				t.Fatal(err)
			}
			m := s.Members["rest"]
			m.Stage = engine.ComponentJoined
			token, err := rc.Secrets.GetAgentToken(t.Context(), rc.Cluster.Ref(), member)
			if err != nil {
				t.Fatal(err)
			}
			m.RuntimeRecovery.ImportPlan = engine.NewImportPlan([]infra.ServiceSpec{engine.RuntimeServiceSpec(backend.Layout(), engine.MemberService{Cluster: rc.Cluster.Name, Member: member, Token: token.Token, Resources: m.Resources, Containers: m.Replicas})})
			if accepted {
				op, err := backend.ImportServices(t.Context(), m.RuntimeRecovery.ImportPlan.Services(backend.Layout(), rc.Cluster.Name, member, token.Token))
				if err != nil {
					t.Fatal(err)
				}
				m.RuntimeRecovery.Op, m.RuntimeRecovery.Action = &op, "import"
			}
			if err := saveState(rc.Cluster, s); err != nil {
				t.Fatal(err)
			}
			rc.Cluster.Deleting = true
			done := false
			for range 10 {
				done, err = p.Cleanup(t.Context(), rc, domain.DeleteOptions{})
				if err != nil {
					t.Fatal(err)
				}
				if done {
					break
				}
			}
			if !done || backend.services[member] != nil || backend.deployed[member] {
				t.Fatal("cleanup deployed or stranded runtime-only component recovery")
			}
			for _, batch := range backend.batches {
				if len(batch) != 1 || batch[0].Hostname != member || batch[0].Kind != infra.KindRuntime {
					t.Fatal("component recovery cleanup changed its original batch")
				}
			}
		})
	}
}
