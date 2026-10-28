// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package server

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/infra"
	"github.com/KarakoramDev/Karakoram/internal/infra/fake"
	"github.com/KarakoramDev/Karakoram/internal/pgbinding"
	"github.com/KarakoramDev/Karakoram/internal/secrets"
	"github.com/KarakoramDev/Karakoram/internal/store"
)

var errPlatformObservation = errors.New("platform observation unavailable")

type failedObservationPlatform struct {
	infra.Backend
	reads atomic.Int32
}

func (b *failedObservationPlatform) GetService(context.Context, string) (*infra.Service, error) {
	b.reads.Add(1)
	return nil, errPlatformObservation
}
func (b *failedObservationPlatform) Containers(context.Context, string) ([]infra.Container, error) {
	b.reads.Add(1)
	return nil, errPlatformObservation
}
func (b *failedObservationPlatform) ProjectInfo(context.Context) (infra.Project, error) {
	b.reads.Add(1)
	return infra.Project{}, errPlatformObservation
}
func (b *failedObservationPlatform) ListServices(context.Context, string) ([]infra.Service, error) {
	b.reads.Add(1)
	return nil, errPlatformObservation
}

type unavailableObservationAgents struct{}

func (unavailableObservationAgents) Dial(context.Context, domain.ClusterRef, string) (*agentapi.Client, error) {
	return nil, engine.ErrUnavailable
}

func TestRegisteredProvidersFailedObservationRetainsLifecycleAndMembers(t *testing.T) {
	key, err := secrets.New(strings.Repeat("x", 32))
	if err != nil {
		t.Fatal(err)
	}
	st, err := store.Open(filepath.Join(t.TempDir(), "state.db"), key)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	// Non-nil binding service enables Keycloak in the production inventory. Status
	// only observes its deployed runtime; it does not call the binding manager.
	providers := engineProviders(engineOptions{Store: st, Bindings: &pgbinding.Manager{}})
	states := map[string]string{
		"psmdb":         `{"phase":"ready","members":[{"name":"member","set":"rs0","role":"mongod","stage":"ready"}]}`,
		"rabbitmq":      `{"phase":"ready","members":[{"name":"member","stage":"ready"}]}`,
		"openbao":       `{"phase":"ready","initialized":true,"members":[{"name":"member","stage":"ready"}]}`,
		"elasticsearch": `{"elasticsearch":{"phase":"ready","members":[{"name":"member","kind":"node","stage":"ready"}]}}`,
		"postgres":      `{"postgres":{"topology":"single","formed":true,"members":[{"name":"member","role":"data","stage":"joined"}]}}`,
		"mysql":         `{"mysql":{"topology":"single","members":[{"name":"member","role":"data","stage":"joined"}]}}`,
		"pxc":           `{"mysql":{"topology":"pxc","members":[{"name":"member","role":"data","stage":"joined"}]}}`,
		"supabase":      `{"postgres":{"topology":"single","formed":true,"members":[{"name":"member","role":"data","stage":"joined"}]}}`,
		"temporal":      `{"temporal":{"bindingId":"binding","members":{"server":{"name":"member","stage":"ready"}}}}`,
		"neo4j":         `{"neo4j":{"member":{"name":"member","stage":"ready"}}}`,
		"weaviate":      `{"phase":"ready","members":[{"name":"member","stage":"ready"}]}`,
		"keycloak":      `{"member":"member","deployed":true}`,
	}
	if len(providers) != len(states) {
		t.Fatalf("provider inventory changed: %d providers, %d fixtures", len(providers), len(states))
	}
	for _, p := range providers {
		t.Run(p.Name(), func(t *testing.T) {
			internal, ok := states[p.Name()]
			if !ok {
				t.Fatal("missing production provider fixture")
			}
			for _, availability := range []bool{false, true} {
				t.Run(map[bool]string{false: "ordinary", true: "renewal"}[availability], func(t *testing.T) {
					at := time.Now()
					c := &domain.Cluster{Name: "demo", Generation: 1, Internal: json.RawMessage(internal), Status: domain.ClusterStatus{Phase: domain.PhaseReady, ObservedGeneration: 1, Members: []domain.MemberStatus{{Name: "member", Health: true, Agent: domain.AgentStatus{Healthy: true, LastSeen: at}}, {Name: "retained-peer", Health: true, Agent: domain.AgentStatus{Healthy: true, LastSeen: at}}}}}
					p.ApplyDefaults(&c.Spec)
					backend := &failedObservationPlatform{Backend: fake.New(fake.Options{})}
					rc := &engine.Context{Cluster: c, Store: st, Secrets: st, Backend: backend, Agents: unavailableObservationAgents{}, Clock: engine.RealClock{}, AvailabilityOnly: availability}
					got, err := engine.ObserveStatus(t.Context(), p, rc)
					if backend.reads.Load() == 0 {
						t.Fatal("fixture did not exercise platform read failure")
					}
					if err == nil {
						t.Fatal("provider concealed platform observation error")
					}
					if got.Phase != domain.PhaseDegraded || len(got.Members) != 2 || got.ObservedGeneration != 1 {
						t.Fatalf("failed observation replaced durable lifecycle/inventory: %+v (%v)", got, err)
					}
					for i, m := range got.Members {
						if m.Name != c.Status.Members[i].Name || m.Health || m.Agent.Healthy {
							t.Fatalf("lost identity or retained health: %+v", m)
						}
					}
					if !got.ObservedAt.IsZero() || got.ObservationEpoch != "" {
						t.Fatal("failed observation renewed lease")
					}
				})
			}
		})
	}
}
