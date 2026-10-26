// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package postgres

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
	"github.com/KarakoramDev/Karakoram/internal/infra"
)

type importReplayClock struct{ at time.Time }

func (c importReplayClock) Now() time.Time { return c.at }

type interruptedImportBackend struct {
	*fakeBackend
	batches [][]infra.ServiceSpec
	fail    bool
	accept  bool
}

func (b *interruptedImportBackend) ImportServices(ctx context.Context, specs []infra.ServiceSpec) (infra.Op, error) {
	raw, err := json.Marshal(specs)
	if err != nil {
		return infra.Op{}, err
	}
	var snapshot []infra.ServiceSpec
	if err := json.Unmarshal(raw, &snapshot); err != nil {
		return infra.Op{}, err
	}
	b.batches = append(b.batches, snapshot)
	op, err := b.fakeBackend.ImportServices(ctx, specs)
	if err != nil {
		return op, err
	}
	if b.fail {
		b.fail = false
		// The volume sibling finished but runtime setup is only reserved. A
		// nonnil owned observation must not replace the original batch replay.
		for _, spec := range specs {
			b.services[spec.Hostname].Importing = spec.Kind == infra.KindRuntime
		}
		if b.accept {
			op.Kind = "incomplete-import"
			return op, nil
		}
		return infra.Op{}, errors.New("import response lost after durable reservation")
	}
	return op, nil
}

func (b *interruptedImportBackend) OpStatus(ctx context.Context, op infra.Op) (infra.OpState, error) {
	if op.Kind == "incomplete-import" {
		for name := range op.Services {
			if service := b.services[name]; service != nil && service.Importing {
				return infra.OpState{}, nil
			}
		}
	}
	return b.fakeBackend.OpStatus(ctx, op)
}

func (b *interruptedImportBackend) DeleteServiceByID(ctx context.Context, name, id string) (infra.Op, error) {
	if service := b.services[name]; service != nil && service.Importing {
		return infra.Op{}, infra.ErrConflict
	}
	return b.fakeBackend.DeleteServiceByID(ctx, name, id)
}

func TestImportReplaysFrozenOriginalBatch(t *testing.T) {
	for _, retained := range []bool{false, true} {
		name := "volume-and-runtime"
		if retained {
			name = "runtime-only-retained-volume"
		}
		t.Run(name, func(t *testing.T) {
			p, rc, base := setup(t, "single")
			backend := &interruptedImportBackend{fakeBackend: base, fail: true}
			rc.Backend = backend
			if _, err := p.Sync(t.Context(), rc); err != nil {
				t.Fatal(err)
			}
			s, err := load(rc.Cluster)
			if err != nil {
				t.Fatal(err)
			}
			m := s.data()[0]
			token, err := rc.Secrets.GetAgentToken(t.Context(), rc.Cluster.Ref(), m.Name)
			if err != nil {
				t.Fatal(err)
			}
			if retained {
				// Model an original retained volume before the first import. Only
				// the runtime belongs to this new import request.
				if _, err := base.ImportServices(t.Context(), []infra.ServiceSpec{engine.VolumeServiceSpec(rc.Cluster.Name, m.Name, m.Volume, token.Token, m.Resources)}); err != nil {
					t.Fatal(err)
				}
				m.ImportBatch = nil
				if err := save(rc.Cluster, s); err != nil {
					t.Fatal(err)
				}
				if _, err := p.Sync(t.Context(), rc); err != nil {
					t.Fatal(err)
				}
				s, _ = load(rc.Cluster)
				m = s.find(m.Name)
			}
			wantNames := []string{m.Volume, m.Name}
			if retained {
				wantNames = []string{m.Name}
			}
			var plannedNames []string
			if m.ImportBatch != nil {
				for _, spec := range m.ImportBatch.Specs {
					plannedNames = append(plannedNames, spec.Hostname)
				}
			}
			if !reflect.DeepEqual(plannedNames, wantNames) || len(backend.batches) != 0 {
				t.Fatalf("import did not first persist its exact subset: %v, calls=%d", plannedNames, len(backend.batches))
			}
			if strings.Contains(string(rc.Cluster.Internal), token.Token) {
				t.Fatal("frozen import exposed the sealed agent token in internal state")
			}
			if _, err := p.Sync(t.Context(), rc); err == nil {
				t.Fatal("ambiguous first submission unexpectedly succeeded")
			}
			if len(backend.batches) != 1 || backend.services[m.Name] == nil || !backend.services[m.Name].Importing {
				t.Fatal("fixture did not retain an incomplete owned runtime reservation")
			}
			original := backend.batches[0]
			oldVolume := backend.services[m.Volume].ID
			if _, err := p.Sync(t.Context(), rc); err == nil || len(backend.batches) != 1 {
				t.Fatal("failed import retried before its durable cooldown")
			}
			rc.Clock = importReplayClock{at: time.Now().UTC().Add(2 * time.Minute)}
			component := rc.Cluster.Spec.Components.Engine
			component.Resources.MinRAM += 1
			component.Resources.MaxRAM += 1
			component.Resources.MinDisk += 10
			component.Resources.MaxDisk += 10
			rc.Cluster.Spec.Components.Engine = component
			// Reconstruct the provider after both a response loss and desired
			// resource changes. The complete original request must be replayed.
			p = New(Options{})
			if _, err := p.Sync(t.Context(), rc); err != nil {
				t.Fatal(err)
			}
			if len(backend.batches) != 2 || !reflect.DeepEqual(original, backend.batches[1]) {
				t.Fatalf("retry changed original batch or its inputs: %#v => %#v", original, backend.batches)
			}
			if retained && backend.services[m.Volume].ID != oldVolume {
				t.Fatal("runtime-only replay replaced its retained volume")
			}
			if _, err := p.Sync(t.Context(), rc); err != nil {
				t.Fatal(err)
			}
			s, _ = load(rc.Cluster)
			m = s.find(m.Name)
			if m.ImportBatch != nil || m.Stage != "imported" {
				t.Fatalf("confirmed import did not release its frozen journal: %+v", m)
			}
		})
	}
}

func TestImportDoesNotAdoptUnjournaledReservation(t *testing.T) {
	p, rc, base := setup(t, "single")
	backend := &interruptedImportBackend{fakeBackend: base, fail: true}
	rc.Backend = backend
	if _, err := p.Sync(t.Context(), rc); err != nil {
		t.Fatal(err)
	}
	if _, err := p.Sync(t.Context(), rc); err == nil {
		t.Fatal("fixture import did not fail")
	}
	s, _ := load(rc.Cluster)
	m := s.data()[0]
	m.ImportBatch = nil
	if err := save(rc.Cluster, s); err != nil {
		t.Fatal(err)
	}
	if _, err := p.Sync(t.Context(), rc); err == nil || !strings.Contains(err.Error(), "no frozen batch") {
		t.Fatalf("incomplete reservation was adopted or guessed: %v", err)
	}
	s, _ = load(rc.Cluster)
	if s.find(m.Name).Stage != "planned" || base.deployed[m.Name] || len(backend.batches) != 1 {
		t.Fatal("incomplete reservation advanced into deployment or a changed import")
	}
}

func TestCleanupResumesFrozenImportWithoutDeployment(t *testing.T) {
	for _, accepted := range []bool{false, true} {
		for _, retain := range []bool{false, true} {
			name := "lost-response"
			if accepted {
				name = "accepted-pending"
			}
			if retain {
				name += "-retained-volume"
			}
			t.Run(name, func(t *testing.T) {
				p, rc, base := setup(t, "single")
				backend := &interruptedImportBackend{fakeBackend: base, fail: true, accept: accepted}
				rc.Backend = backend
				if _, err := p.Sync(t.Context(), rc); err != nil {
					t.Fatal(err)
				}
				_, err := p.Sync(t.Context(), rc)
				if (err == nil) != accepted {
					t.Fatalf("unexpected import submission outcome: %v", err)
				}
				original := backend.batches[0]
				rc.Cluster.Deleting = true
				rc.Clock = importReplayClock{at: time.Now().UTC().Add(2 * time.Minute)}
				done := false
				for range 12 {
					done, err = p.Cleanup(t.Context(), rc, domain.DeleteOptions{KeepVolumes: retain})
					if err != nil {
						t.Fatal(err)
					}
					if done {
						break
					}
				}
				if !done || len(backend.batches) != 2 || !reflect.DeepEqual(original, backend.batches[1]) {
					t.Fatalf("cleanup did not drain original import: done=%t batches=%#v", done, backend.batches)
				}
				if base.services["demop0"] != nil || (base.services["demop0v"] != nil) != retain {
					t.Fatal("cleanup did not preserve the requested volume disposition")
				}
				for _, action := range base.actions {
					if strings.HasPrefix(action, "deploy:") {
						t.Fatal("cleanup deployed a partially imported member")
					}
				}
			})
		}
	}
}

func TestAcceptedImportResumesSetupBeforeWaitingOnHandle(t *testing.T) {
	p, rc, base := setup(t, "single")
	backend := &interruptedImportBackend{fakeBackend: base, fail: true, accept: true}
	rc.Backend = backend
	for range 3 {
		if _, err := p.Sync(t.Context(), rc); err != nil {
			t.Fatal(err)
		}
	}
	if len(backend.batches) != 2 || !reflect.DeepEqual(backend.batches[0], backend.batches[1]) {
		t.Fatal("accepted import waited forever instead of resuming its frozen setup")
	}
	if _, err := p.Sync(t.Context(), rc); err != nil {
		t.Fatal(err)
	}
	s, _ := load(rc.Cluster)
	if m := s.data()[0]; m.Stage != "imported" || m.ImportBatch != nil {
		t.Fatalf("resumed accepted import did not complete: %+v", m)
	}
}

func TestCleanupDrainsRuntimeRecoveryImportPlan(t *testing.T) {
	for _, accepted := range []bool{false, true} {
		name := "persisted-plan"
		if accepted {
			name = "accepted-setup-reservation"
		}
		t.Run(name, func(t *testing.T) {
			p, rc, base := setup(t, "single")
			backend := &interruptedImportBackend{fakeBackend: base, fail: accepted, accept: accepted}
			rc.Backend = backend
			m := &member{Name: "demop0", Volume: "demop0v", Role: "data", Stage: "joined", Replicas: 1}
			token := &domain.AgentToken{Namespace: rc.Cluster.Namespace, Cluster: rc.Cluster.Name, Member: m.Name, Token: "runtime-recovery-token"}
			if err := rc.Secrets.SetAgentToken(t.Context(), token); err != nil {
				t.Fatal(err)
			}
			op, err := base.ImportServices(t.Context(), []infra.ServiceSpec{engine.VolumeServiceSpec(rc.Cluster.Name, m.Name, m.Volume, token.Token, m.Resources)})
			if err != nil {
				t.Fatal(err)
			}
			m.VolumeID = op.Services[m.Volume]
			m.RuntimeRecovery.ImportPlan = engine.NewImportPlan([]infra.ServiceSpec{engine.RuntimeServiceSpec(backend.Layout(), engine.MemberService{Cluster: rc.Cluster.Name, Member: m.Name, Token: token.Token, Volume: m.Volume, Containers: 1})})
			if accepted {
				op, err := backend.ImportServices(t.Context(), m.RuntimeRecovery.ImportPlan.Services(backend.Layout(), rc.Cluster.Name, m.Name, token.Token))
				if err != nil {
					t.Fatal(err)
				}
				m.RuntimeRecovery.Op, m.RuntimeRecovery.Action = &op, "import"
			}
			s := &state{UID: "runtime-recovery", Topology: "single", Members: []*member{m}}
			if err := save(rc.Cluster, s); err != nil {
				t.Fatal(err)
			}
			rc.Cluster.Deleting = true
			done := false
			for range 12 {
				done, err = p.Cleanup(t.Context(), rc, domain.DeleteOptions{KeepVolumes: true})
				if err != nil {
					t.Fatal(err)
				}
				if done {
					break
				}
			}
			if !done || base.services[m.Name] != nil || base.services[m.Volume] == nil || base.services[m.Volume].ID != m.VolumeID {
				t.Fatal("cleanup did not drain runtime recovery before retaining the original volume")
			}
			for _, batch := range backend.batches {
				if len(batch) != 1 || batch[0].Hostname != m.Name || batch[0].Kind != infra.KindRuntime {
					t.Fatal("cleanup changed a runtime-only recovery into volume creation")
				}
			}
			for _, action := range base.actions {
				if strings.HasPrefix(action, "deploy:") {
					t.Fatal("runtime-recovery cleanup deployed a database")
				}
			}
		})
	}
}
