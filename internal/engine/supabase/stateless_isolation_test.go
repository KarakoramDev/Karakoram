// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package supabase

import (
	"context"
	"encoding/json"
	"reflect"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine/postgres/pgspec"
	"github.com/KarakoramDev/Karakoram/internal/infra"
)

type dataPathMutationBackend struct {
	*testBackend
	mutations []string
}

func (b *dataPathMutationBackend) record(name string) {
	if name == "demop0" || name == "demopx" || name == "demop0v" {
		b.mutations = append(b.mutations, name)
	}
}

func (b *dataPathMutationBackend) Deploy(ctx context.Context, name string, d infra.Deployment) (infra.Op, error) {
	b.record(name)
	return b.testBackend.Deploy(ctx, name, d)
}

func (b *dataPathMutationBackend) SetResources(ctx context.Context, name string, r domain.Resources) (infra.Op, error) {
	b.record(name)
	return b.testBackend.SetResources(ctx, name, r)
}

func (b *dataPathMutationBackend) SetContainerCount(ctx context.Context, name string, min, max int) (infra.Op, error) {
	b.record(name)
	return b.testBackend.SetContainerCount(ctx, name, min, max)
}

func TestStatelessComponentResourcesDoNotReconfigureDataPath(t *testing.T) {
	for _, change := range []string{"resources", "replicas"} {
		t.Run(change, func(t *testing.T) {
			p, rc, backend := setupProvider(t)
			convergeProvider(t, p, rc)
			before := map[string]agentapi.MemberConfig{}
			for _, name := range []string{"demop0", "demopx"} {
				config, err := p.MemberConfig(t.Context(), rc, name)
				if err != nil {
					t.Fatal(err)
				}
				before[name] = config
			}
			tracked := &dataPathMutationBackend{testBackend: backend}
			rc.Backend = tracked
			rest := rc.Cluster.Spec.Components.Extra["rest"]
			if change == "resources" {
				rest.Resources.MaxCPU++
			} else {
				rest.Replicas++
			}
			rc.Cluster.Spec.Components.Extra["rest"] = rest
			rc.Cluster.Generation++
			convergeProvider(t, p, rc)
			for name, previous := range before {
				config, err := p.MemberConfig(t.Context(), rc, name)
				if err != nil {
					t.Fatal(err)
				}
				if !reflect.DeepEqual(previous, config) {
					t.Errorf("REST %s change reconfigured %s (revision %d -> %d)", change, name, previous.ConfigVersion, config.ConfigVersion)
				}
			}
			if len(tracked.mutations) != 0 {
				t.Fatalf("REST change mutated data-path infrastructure: %v", tracked.mutations)
			}
			service := backend.services["demors"]
			if service.Resources != rest.Resources || service.MinContainers != rest.Replicas {
				t.Fatalf("REST desired bounds were not applied: resources=%+v replicas=%d", service.Resources, service.MinContainers)
			}
		})
	}
}

func TestDataPathOptionsStillPublishNewConfiguration(t *testing.T) {
	for _, options := range []string{
		`{"parameters":{"work_mem":"8MB"}}`,
		`{"proxy":{"readWriteSplit":true}}`,
		`{"extensions":["pg_cron"]}`,
	} {
		t.Run(options, func(t *testing.T) {
			p, rc, _ := setupProvider(t)
			convergeProvider(t, p, rc)
			before, err := p.MemberConfig(t.Context(), rc, "demop0")
			if err != nil {
				t.Fatal(err)
			}
			rc.Cluster.Spec.EngineOptions = json.RawMessage(options)
			rc.Cluster.Generation++
			convergeProvider(t, p, rc)
			for _, name := range []string{"demop0", "demopx"} {
				config, err := p.MemberConfig(t.Context(), rc, name)
				if err != nil {
					t.Fatal(err)
				}
				if config.ConfigVersion <= before.ConfigVersion {
					t.Fatalf("data-path option change did not advance %s revision", name)
				}
				var spec pgspec.Spec
				if err := json.Unmarshal(config.EngineSpec, &spec); err != nil {
					t.Fatal(err)
				}
				if name == "demop0" && options == `{"parameters":{"work_mem":"8MB"}}` && spec.Postgres.Parameters["work_mem"] != "8MB" {
					t.Fatal("PostgreSQL parameter was not published")
				}
				if name == "demopx" && options == `{"proxy":{"readWriteSplit":true}}` && !spec.Proxy.ReadWriteSplit {
					t.Fatal("proxy routing option was not published")
				}
				if name == "demop0" && options == `{"extensions":["pg_cron"]}` {
					found := false
					for _, preload := range spec.Postgres.SharedPreloadLibraries {
						found = found || preload == "pg_cron"
					}
					if !found {
						t.Fatal("extension preload was not published")
					}
				}
			}
		})
	}
}
