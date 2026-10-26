// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package psmdb

import (
	"context"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
)

func TestReplicaCountDoesNotInvalidateMemberConfiguration(t *testing.T) {
	for _, field := range []string{"engine", "configServer", "router", "extra"} {
		t.Run(field, func(t *testing.T) {
			rc := &engine.Context{Cluster: &domain.Cluster{}}
			rc.Cluster.Spec.Components.Engine.Replicas = 3
			rc.Cluster.Spec.Components.Extra = map[string]domain.ComponentSpec{"other": {Replicas: 3}}
			s := &internalState{ConfigVersion: 4, TLSMode: rc.Cluster.Spec.TLS.Mode}
			if _, err := reconcileConfig(context.Background(), rc, s); err != nil {
				t.Fatal(err)
			}
			version := s.ConfigVersion
			switch field {
			case "engine":
				rc.Cluster.Spec.Components.Engine.Replicas = 5
			case "configServer":
				rc.Cluster.Spec.Components.ConfigServer.Replicas = 5
			case "router":
				rc.Cluster.Spec.Components.Router.Replicas = 5
			case "extra":
				rc.Cluster.Spec.Components.Extra["other"] = domain.ComponentSpec{Replicas: 5}
			}
			changed, err := reconcileConfig(context.Background(), rc, s)
			if err != nil || changed || s.ConfigVersion != version {
				t.Fatalf("replica-only change schedules rolling reload: changed=%v version=%d want=%d err=%v", changed, s.ConfigVersion, version, err)
			}
			if rc.Cluster.Spec.Components.Extra["other"].Replicas == 0 {
				t.Fatal("hash normalization mutated desired spec")
			}
			rc.Cluster.Spec.Components.Engine.Resources.MaxRAM = 2
			if changed, err := reconcileConfig(context.Background(), rc, s); err != nil || !changed || s.ConfigVersion != version+1 {
				t.Fatalf("resource change lost reload: changed=%v version=%d err=%v", changed, s.ConfigVersion, err)
			}
		})
	}
}

func TestLegacyConfigHashMigratesWithoutReload(t *testing.T) {
	rc := &engine.Context{Cluster: &domain.Cluster{}}
	rc.Cluster.Spec.Components.Engine.Replicas = 3
	s := &internalState{ConfigVersion: 4}
	s.ConfigHash = hash(struct {
		Engine          string
		TLS             domain.TLSConfig
		RAM             domain.Components
		CertificateHash string
		ConfigDB        string
	}{rc.Cluster.Spec.EngineConfig, rc.Cluster.Spec.TLS, rc.Cluster.Spec.Components, "", ""})
	changed, err := reconcileConfig(context.Background(), rc, s)
	if err != nil || changed || s.ConfigVersion != 4 {
		t.Fatalf("migration triggers reload: changed=%v version=%d err=%v", changed, s.ConfigVersion, err)
	}
	rc.Cluster.Spec.Components.Engine.Replicas = 5
	if changed, err = reconcileConfig(context.Background(), rc, s); err != nil || changed || s.ConfigVersion != 4 {
		t.Fatalf("migrated scale triggers reload: changed=%v version=%d err=%v", changed, s.ConfigVersion, err)
	}
}

func TestMaterialConfigChangesStillInvalidateMemberConfiguration(t *testing.T) {
	for _, field := range []string{"engine", "tls", "configDB"} {
		t.Run(field, func(t *testing.T) {
			rc := &engine.Context{Cluster: &domain.Cluster{}}
			s := &internalState{ConfigVersion: 4}
			if _, err := reconcileConfig(context.Background(), rc, s); err != nil {
				t.Fatal(err)
			}
			version := s.ConfigVersion
			switch field {
			case "engine":
				rc.Cluster.Spec.EngineConfig = "net:\n  maxIncomingConnections: 100"
			case "tls":
				rc.Cluster.Spec.TLS.Mode = "requireTLS"
			case "configDB":
				s.Members = append(s.Members, &member{Name: "cfg0", Set: "cfg"})
			}
			changed, err := reconcileConfig(context.Background(), rc, s)
			if err != nil || !changed || s.ConfigVersion != version+1 {
				t.Fatalf("material change lost reload: changed=%v version=%d err=%v", changed, s.ConfigVersion, err)
			}
		})
	}
}

func TestPostScaleConvergesWithoutEmptyRollingReload(t *testing.T) {
	p, rc, agents, _ := renewalCluster(t, 5)
	ctx := context.Background()
	s, err := load(rc.Cluster)
	if err != nil {
		t.Fatal(err)
	}
	// Model the pre-scale fingerprint, with all five members now safely joined
	// and promoted. Membership mutations themselves remain on the serial path.
	rc.Cluster.Spec.Components.Engine.Replicas = 3
	if _, err := reconcileConfig(ctx, rc, s); err != nil {
		t.Fatal(err)
	}
	for _, m := range s.Members {
		activateConfig(rc.Cluster, s, m)
		agents[m.Name].version.Store(m.ConfigVersion)
	}
	if err := save(rc.Cluster, s); err != nil {
		t.Fatal(err)
	}
	rc.Cluster.Spec.Components.Engine.Replicas = 5
	prepare := func(context.Context, *engine.Context, *internalState, *member) (bool, error) {
		t.Fatal("replica-only scale entered disruptive rolling configuration")
		return false, nil
	}
	for range 20 {
		if _, err := p.sync(ctx, rc, prepare, healthyTestReplication); err != nil {
			t.Fatal(err)
		}
		s, err = load(rc.Cluster)
		if err != nil {
			t.Fatal(err)
		}
		if s.Phase == "ready" {
			for name, a := range agents {
				if a.reloads.Load() != 0 {
					t.Fatalf("%s reloaded after replica-only scale", name)
				}
			}
			return
		}
	}
	t.Fatal("post-scale convergence did not reach Ready")
}
