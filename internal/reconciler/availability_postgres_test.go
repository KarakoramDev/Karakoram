// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package reconciler

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/postgres"
	"github.com/KarakoramDev/Karakoram/internal/infra"
	"github.com/KarakoramDev/Karakoram/internal/infra/fake"
)

type postgresSweepProvider struct {
	*fakeProvider
	native  *postgres.Core
	failing atomic.Bool
	samples atomic.Int32
	syncs   atomic.Int32
	started chan struct{}
	once    sync.Once
}

func (p *postgresSweepProvider) Sync(ctx context.Context, _ *engine.Context) (engine.Result, error) {
	p.syncs.Add(1)
	p.once.Do(func() { close(p.started) })
	select {
	case <-ctx.Done():
		return engine.Result{}, ctx.Err()
	case <-time.After(20 * time.Second):
		return engine.Result{RequeueAfter: time.Minute}, nil
	}
}
func (p *postgresSweepProvider) Status(ctx context.Context, rc *engine.Context) (domain.ClusterStatus, error) {
	p.samples.Add(1)
	if p.failing.Load() {
		return p.native.Status(ctx, rc)
	}
	return domain.ClusterStatus{Phase: domain.PhaseReady, Members: []domain.MemberStatus{{Name: "demop0", Health: true, Agent: domain.AgentStatus{Healthy: true, LastSeen: rc.Clock.Now()}}}}, nil
}

type unavailableRuntimeBackend struct{ infra.Backend }

func (b unavailableRuntimeBackend) GetService(context.Context, string) (*infra.Service, error) {
	return nil, errors.New("platform runtime read unavailable")
}

func TestPostgresFailedRenewalDuringSyncDoesNotRestartLifecycle(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		p := &postgresSweepProvider{fakeProvider: &fakeProvider{}, native: postgres.New(postgres.Options{}), started: make(chan struct{})}
		m, st := newTestManager(t, p)
		m.opts.Context.ObservationEpoch = "current-process"
		m.opts.Namespaces = engine.StaticNamespaces{DefaultBackend: unavailableRuntimeBackend{fake.New(fake.Options{})}}
		c := &domain.Cluster{Name: "demo", Spec: domain.ClusterSpec{Provider: "fake"},
			Internal: json.RawMessage(`{"postgres":{"topology":"single","formed":true,"members":[{"name":"demop0","role":"data","stage":"joined"}]}}`),
			Status:   domain.ClusterStatus{Phase: domain.PhaseReady, ObservedGeneration: 1, Members: []domain.MemberStatus{{Name: "demop0", Health: true}}}}
		p.native.ApplyDefaults(&c.Spec)
		c.Spec.Provider = "fake"
		if err := st.CreateCluster(t.Context(), c); err != nil {
			t.Fatal(err)
		}
		original := string(c.Internal)
		ctx, cancel := context.WithCancel(t.Context())
		w := &worker{trigger: make(chan struct{}, 1)}
		m.wg.Add(1)
		go m.work(ctx, c.Ref(), w)
		defer func() { cancel(); m.wg.Wait() }()
		<-p.started
		p.failing.Store(true)
		for second := 1; second <= 12; second++ {
			time.Sleep(time.Second)
			synctest.Wait()
			stored, err := st.GetCluster(t.Context(), c.Ref())
			if err != nil {
				t.Fatal(err)
			}
			if len(stored.Status.Members) != 1 || stored.Status.Phase == domain.PhaseCreating || stored.Status.ObservedGeneration != 1 || string(stored.Internal) != original {
				t.Fatalf("renewal reset established lifecycle at %ds: %+v", second, stored.Status)
			}
			if second >= 5 && (stored.Status.Phase != domain.PhaseDegraded || stored.Status.Members[0].Health || !stored.Status.ObservedAt.IsZero()) {
				t.Fatalf("failure did not withdraw availability: %+v", stored.Status)
			}
		}
		p.failing.Store(false)
		time.Sleep(4 * time.Second)
		synctest.Wait()
		stored, err := st.GetCluster(t.Context(), c.Ref())
		if err != nil || stored.Status.Phase != domain.PhaseReady || len(stored.Status.Members) != 1 || !stored.Status.Members[0].Health {
			t.Fatalf("renewal did not recover independently of Sync: %+v %v", stored, err)
		}
		if p.syncs.Load() != 1 || p.samples.Load() < 4 {
			t.Fatal("unexpected lifecycle reentry or missing concurrent renewals", p.syncs.Load(), p.samples.Load())
		}
	})
}
