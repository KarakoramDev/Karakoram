// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package neo4j

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/neo4j/neo4jspec"
)

func TestVersionCatalogAndBackupRequiredPaths(t *testing.T) {
	p := New(Options{})
	versions := []string{"2026.09.0", "2026.10.0", "2026.10.1"}
	info := p.Describe()
	if len(info.Versions) != len(versions) || len(info.UpgradePaths) != 3 || defaultVersion != versions[2] {
		t.Fatal(info)
	}
	for i, v := range info.Versions {
		if v.Version != versions[i] || v.Default != (i == 2) {
			t.Fatal(v)
		}
	}
	for i, from := range versions {
		for j, to := range versions {
			old := domain.Cluster{Name: "graphdb", Spec: domain.ClusterSpec{Version: from}}
			p.ApplyDefaults(&old.Spec)
			next := old
			next.Spec.Version = to
			err := p.Validate(t.Context(), &old, &next)
			if (err == nil) != (j >= i) {
				t.Fatalf("%s -> %s: %v", from, to, err)
			}
			if j <= i {
				continue
			}
			found := false
			for _, path := range info.UpgradePaths {
				if path.From == from && path.To == to {
					found = path.RequiresBackup
				}
			}
			if !found {
				t.Fatalf("missing backup-required path %s -> %s", from, to)
			}
		}
	}
}

func TestLegacyJREChangesOnlyAfterAgentPreparation(t *testing.T) {
	for _, version := range []string{"2026.09.0", "2026.10.0"} {
		t.Run(version, func(t *testing.T) {
			p, rc, b, a := setup(t)
			rc.Cluster.Spec.Version = version
			converge(t, p, rc)
			s, _ := load(rc.Cluster)
			s.Member.JavaVersion = "" // State written before shared JRE tracking.
			if err := save(rc.Cluster, s); err != nil {
				t.Fatal(err)
			}
			config := func() neo4jspec.Spec {
				t.Helper()
				cfg, err := p.MemberConfig(t.Context(), rc, s.Member.Name)
				if err != nil {
					t.Fatal(err)
				}
				a.config = cfg
				if err := json.Unmarshal(cfg.EngineSpec, &a.spec); err != nil {
					t.Fatal(err)
				}
				return a.spec
			}
			before := config()
			if !strings.HasSuffix(before.Install.JavaHome, "/jdk-21.0.12.1+1-jre") {
				t.Fatal(before.Install)
			}
			legacyToken := digest(struct {
				Version     string
				Heap, Page  int
				Settings    map[string]string
				Certificate string
			}{before.Version, before.HeapMB, before.PageCacheMB, before.Settings, digest(before.Server.CertPEM)})
			if before.RestartToken != legacyToken {
				t.Fatal("control-plane-only update changed legacy restart token")
			}
			build, _ := upgradeAgent(t, rc, a)
			wrapped := &upgradeBackend{Backend: b}
			rc.Backend = wrapped
			op := upgradeOperation(true)
			done := false
			for range 16 {
				r, err := p.RunOperation(t.Context(), rc, op)
				if err != nil {
					t.Fatal(err)
				}
				op.Checkpoint = r.Checkpoint
				var cp agentUpgradeCheckpoint
				if err := json.Unmarshal(r.Checkpoint, &cp); err != nil {
					t.Fatal(err)
				}
				after := config()
				if cp.Deploy != nil && after.Install.JavaHome != before.Install.JavaHome {
					t.Fatal("published unprepared JRE")
				}
				if len(wrapped.names) > 0 {
					*build = engine.CurrentAgentBuild().Build
				}
				if r.Done {
					done = true
					break
				}
			}
			after := config()
			if !done || len(wrapped.specs) != 1 || after.Version != version || !strings.HasSuffix(after.Install.JavaHome, "/jdk-21.0.13+9-jre") || after.RestartToken == before.RestartToken {
				t.Fatalf("done=%v deploys=%d install=%+v", done, len(wrapped.specs), after.Install)
			}
			if !strings.Contains(strings.Join(wrapped.specs[0].Prepare, "\n"), "OpenJDK21U-jre_x64_linux_hotspot_21.0.13_9.tar.gz") {
				t.Fatal("new JRE not prepared")
			}
		})
	}
}
