// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package neo4j

import (
	"encoding/json"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/enginetest"
	"github.com/KarakoramDev/Karakoram/internal/engine/nativeartifacts"
	"github.com/KarakoramDev/Karakoram/internal/engine/neo4j/neo4jspec"
	"github.com/KarakoramDev/Karakoram/internal/infra"
)

type cpUpgradeClock struct{ at time.Time }

func (c cpUpgradeClock) Now() time.Time { return c.at }

// The fixture was emitted by setup/converge at a50af33 (before 4289cf58),
// with its genuine embedded old lock, not by provisioning with today's provider
// and deleting the installed-version receipt. All credentials are test-only.
func TestCPUpgradePreservesExistingNeo4jRuntime(t *testing.T) {
	raw, err := os.ReadFile("testdata/cp-upgrade-a50af33.json")
	if err != nil {
		t.Fatal(err)
	}
	var fixture struct {
		Cluster  domain.Cluster
		Secrets  map[string][]byte
		Token    *domain.AgentToken
		Internal json.RawMessage
	}
	if err = json.Unmarshal(enginetest.RestoreFixtureKeys(t, raw), &fixture); err != nil {
		t.Fatal(err)
	}
	p, rc, b, a := setup(t)
	*rc.Cluster = fixture.Cluster
	rc.Cluster.Internal = fixture.Internal
	rc.Clock = cpUpgradeClock{at: fixture.Cluster.Status.ObservedAt}
	for key, value := range fixture.Secrets {
		if err = rc.Secrets.SetClusterSecret(t.Context(), rc.Cluster.Ref(), key, value); err != nil {
			t.Fatal(err)
		}
	}
	if err = rc.Secrets.SetAgentToken(t.Context(), fixture.Token); err != nil {
		t.Fatal(err)
	}
	s, err := load(rc.Cluster)
	if err != nil {
		t.Fatal(err)
	}
	m := s.Member
	if m == nil {
		t.Fatal("legacy fixture has no member")
	}
	if m.JavaVersion != "" || m.Version != "2026.10.0" || m.Stage != "ready" {
		t.Fatalf("not legacy installed state: %+v", m)
	}
	specs := []infra.ServiceSpec{
		engine.VolumeServiceSpec(rc.Cluster.Name, m.Name, m.Volume, fixture.Token.Token, m.Disk),
		engine.RuntimeServiceSpec(b.Layout(), engine.MemberService{Cluster: rc.Cluster.Name, Member: m.Name, Token: fixture.Token.Token, Volume: m.Volume, Resources: m.Resources, Containers: 1}),
	}
	op, err := b.ImportServices(t.Context(), specs)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = b.OpStatus(t.Context(), op); err != nil {
		t.Fatal(err)
	}
	op, err = b.Start(t.Context(), m.Name)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = b.OpStatus(t.Context(), op); err != nil {
		t.Fatal(err)
	}
	jre, err := nativeartifacts.Get("temurin-jre-21")
	if err != nil {
		t.Fatal(err)
	}
	neo, err := nativeartifacts.Get("neo4j-community-2026.10.1")
	if err != nil {
		t.Fatal(err)
	}
	if jre.Version != "21.0.13+9" || neo.Version != "2026.10.1" {
		t.Fatalf("unexpected upgraded lock: JRE=%s Neo4j=%s", jre.Version, neo.Version)
	}
	cfg, err := p.MemberConfig(t.Context(), rc, m.Name)
	if err != nil {
		t.Fatal(err)
	}
	a.config = cfg
	if err = json.Unmarshal(cfg.EngineSpec, &a.spec); err != nil {
		t.Fatal(err)
	}
	a.password = a.spec.AdminPassword
	before := a.spec
	legacyToken := digest(struct {
		Version     string
		Heap, Page  int
		Settings    map[string]string
		Certificate string
	}{before.Version, before.HeapMB, before.PageCacheMB, before.Settings, digest(before.Server.CertPEM)})
	if !strings.HasSuffix(before.Install.JavaHome, "/jdk-21.0.12.1+1-jre") || before.RestartToken != legacyToken {
		t.Fatalf("legacy runtime activated new JRE: %+v", before.Install)
	}
	calls := len(b.Calls())
	for range 12 {
		// A new provider on every pass models CP process restarts over persisted state.
		p = New(p.options)
		result, err := p.Sync(t.Context(), rc)
		if err != nil || result.ConvergenceError != nil {
			t.Fatalf("CP reconcile: %v %v", err, result.ConvergenceError)
		}
		cfg, err = p.MemberConfig(t.Context(), rc, m.Name)
		if err != nil {
			t.Fatal(err)
		}
		var after neo4jspec.Spec
		if err = json.Unmarshal(cfg.EngineSpec, &after); err != nil {
			t.Fatal(err)
		}
		if after.Version != before.Version || after.Install.JavaHome != before.Install.JavaHome || after.RestartToken != before.RestartToken {
			t.Fatalf("CP-only reconcile changed active runtime: before=%+v after=%+v", before.Install, after.Install)
		}
	}
	for _, call := range b.Calls()[calls:] {
		if call == "Deploy" || call == "Restart" || call == "ImportServices" {
			t.Fatalf("CP-only reconcile submitted %s", call)
		}
	}
	if a.restarts != 0 {
		t.Fatalf("CP-only reconcile restarted Neo4j %d times", a.restarts)
	}
	after, err := load(rc.Cluster)
	if err != nil {
		t.Fatal(err)
	}
	if after.Member.JavaVersion != "" || after.Member.PendingJavaVersion != "" || after.Member.Pending != nil {
		t.Fatal("CP-only reconcile staged or activated new JRE")
	}
}
