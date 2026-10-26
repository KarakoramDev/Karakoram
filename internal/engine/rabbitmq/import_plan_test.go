// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package rabbitmq

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/infra"
	"reflect"
	"strings"
	"testing"
	"time"
)

type incompleteImportInfra struct {
	*fakeInfra
	expected  []infra.ServiceSpec
	op        infra.Op
	calls     int
	ambiguous bool
}

func (b *incompleteImportInfra) ImportServices(ctx context.Context, specs []infra.ServiceSpec) (infra.Op, error) {
	b.calls++
	if b.expected == nil {
		raw, _ := json.Marshal(specs)
		_ = json.Unmarshal(raw, &b.expected)
		op, err := b.fakeInfra.ImportServices(ctx, specs)
		if err != nil {
			return op, err
		}
		b.op = op
		for _, spec := range specs {
			b.services[spec.Hostname].Importing = true
		}
		if b.ambiguous {
			return infra.Op{}, errors.New("setup reply lost")
		}
		return op, nil
	}
	if !reflect.DeepEqual(specs, b.expected) {
		return infra.Op{}, errors.New("resumed a changed or partial import batch")
	}
	for _, spec := range specs {
		b.services[spec.Hostname].Importing = false
	}
	return b.op, nil
}
func (b *incompleteImportInfra) OpStatus(ctx context.Context, op infra.Op) (infra.OpState, error) {
	for _, s := range b.services {
		if s.Importing {
			return infra.OpState{}, nil
		}
	}
	return b.fakeInfra.OpStatus(ctx, op)
}

func TestSyncAndCleanupResumeFrozenImportReservation(t *testing.T) {
	for _, cleanup := range []bool{false, true} {
		for _, ambiguous := range []bool{false, true} {
			t.Run(map[bool]string{false: "sync", true: "cleanup"}[cleanup]+map[bool]string{false: "/accepted", true: "/ambiguous"}[ambiguous], func(t *testing.T) {
				p, rc, f, _ := setup(t)
				rc.Cluster.Spec.Components.Engine.Replicas = 1
				b := &incompleteImportInfra{fakeInfra: f, ambiguous: ambiguous}
				rc.Backend = b
				if _, err := p.Sync(t.Context(), rc); err != nil {
					t.Fatal(err)
				}
				s := state(t, rc)
				m := s.Members[0]
				token, err := rc.Secrets.GetAgentToken(t.Context(), rc.Cluster.Ref(), m.Name)
				if err != nil {
					t.Fatal(err)
				}
				if m.ImportPlan == nil || b.calls != 0 || strings.Contains(string(rc.Cluster.Internal), token.Token) {
					t.Fatal("missing secret-free pre-submit plan")
				}
				_, err = p.Sync(t.Context(), rc)
				if (err != nil) != ambiguous || b.calls != 1 {
					t.Fatal("initial setup did not reach reserved state", err, b.calls)
				}
				ids := map[string]string{}
				for name, service := range f.services {
					ids[name] = service.ID
				}
				rc.Cluster.Spec.Components.Engine.Resources.MaxRAM += 2
				rc.Clock.(*testClock).at = rc.Clock.Now().Add(2 * time.Minute)
				rc.Cluster.Spec.Components.Engine.Resources.MinRAM += 2
				p = New(p.options)
				rc.Cluster = cloneCluster(t, rc.Cluster)
				if cleanup {
					rc.Cluster.Deleting = true
					if done, err := p.Cleanup(t.Context(), rc, domain.DeleteOptions{KeepBackups: true, KeepVolumes: true}); err != nil || done {
						t.Fatal(done, err)
					}
				} else {
					if _, err := p.Sync(t.Context(), rc); err != nil {
						t.Fatal(err)
					}
				}
				if b.calls != 2 {
					t.Fatal("ordinary lifecycle did not resume original import", b.calls)
				}
				for name, id := range ids {
					if f.services[name].ID != id || f.services[name].Importing {
						t.Fatal("reservation identity replaced or setup unfinished", name)
					}
				}
				if len(f.deployments) != 0 {
					t.Fatal("deployed before import completion checkpoint")
				}
			})
		}
	}
}
