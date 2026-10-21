// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package weaviate

import (
	"context"
	"encoding/json"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/nativeartifacts"
	"github.com/KarakoramDev/Karakoram/internal/engine/weaviate/wvspec"
	"github.com/KarakoramDev/Karakoram/internal/infra"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"
)

func TestNamingDisjoint(t *testing.T) {
	for length := 2; length <= 12; length++ {
		cluster := strings.Repeat("a", length)
		names := make([]string, 1000)
		for i := range names {
			names[i] = NodeHostname(cluster, i)
			if !ValidHostname(names[i]) || !ValidHostname(VolumeHostname(names[i])) {
				t.Fatal("backend hostname limit", names[i])
			}
		}
		for i, name := range names {
			for j, other := range names {
				if i != j && strings.Contains(other+":8301", name) {
					t.Fatalf("%s is contained in %s", name, other)
				}
			}
		}
		for _, i := range []int{999999, 1000000, 999999999} {
			name := NodeHostname(cluster, i)
			if !ValidHostname(VolumeHostname(name)) {
				t.Fatal(name)
			}
			for _, other := range names {
				if strings.Contains(name+":8301", other) || strings.Contains(other+":8301", name) {
					t.Fatal("width overflow collision")
				}
			}
		}
	}
	if ValidClusterName("a") || ValidClusterName(strings.Repeat("a", 13)) || ValidHostname(strings.Repeat("a", 26)) {
		t.Fatal("invalid length accepted")
	}
	if NextMemberIndex(12, []int{0, 2, 7}) != 12 || NextMemberIndex(0, []int{0, 7, 2}) != 8 {
		t.Fatal("retired index reused")
	}
}
func TestOptionsStrict(t *testing.T) {
	for _, raw := range []string{`null`, `[]`, `{} {}`, `{"unknown":true}`, `{"modules":["backup-s3"]}`, `{"modules":["text2vec-openai","text2vec-openai"]}`, `{"modules":["text2vec-unknown"]}`, `{"asyncReplication":1}`, `{"asyncReplication":null}`, `{"queryDefaultsLimit":0}`, `{"queryDefaultsLimit":10001}`, `{"diskWarningPercent":90}`, `{"diskReadonlyPercent":96}`, `{"replicationMinimumFactor":0}`, `{"inference":{"text2vec-ollama":{"url":"http://host"}}}`, `{"inference":{"text2vec-transformers":{"url":"http://user:pass@host"}}}`, `{"inference":{"text2vec-transformers":{"url":"http://host?"}}}`, `{"inference":{"text2vec-transformers":{"url":"http://host#"}}}`, `{"inference":{"text2vec-transformers":{"url":"file:///tmp/x"}}}`, `{"inference":{"text2vec-transformers":{"url":"http://host","env":"x"}}}`} {
		if _, err := parseOptions(json.RawMessage(raw)); err == nil {
			t.Errorf("accepted %s", raw)
		}
	}
	o, err := parseOptionsWithDefaults(json.RawMessage(`{"asyncReplication":false,"modules":["text2vec-openai"],"inference":{"text2vec-transformers":{"url":"https://inference:8080/v1"}}}`), "cluster")
	if err != nil || o.AsyncReplication || o.ReplicationMinimumFactor != 3 || o.QueryDefaultsLimit != 25 || o.DiskWarningPercent != 80 || o.DiskReadonlyPercent != 90 {
		t.Fatalf("%+v %v", o, err)
	}
	if validateOptions(o, 2) == nil {
		t.Fatal("replication above members accepted")
	}
	if InferenceEnv("text2vec-transformers") != "TRANSFORMERS_INFERENCE_API" {
		t.Fatal("missing inference mapping")
	}
}
func TestCatalogArtifactsAndUpgrade(t *testing.T) {
	for _, v := range catalog {
		a, err := nativeartifacts.Get(ArtifactName(v.Version))
		if err != nil || a.Version != v.Version {
			t.Fatalf("%s: %+v %v", v.Version, a, err)
		}
		commands, err := PrepareCommands(v.Version, infra.Layout{User: "app"})
		if err != nil || len(commands) < 2 || !strings.Contains(commands[0], "python3") || !strings.Contains(strings.Join(commands, "\n"), a.SHA256) {
			t.Fatalf("prepare %s: %v %v", v.Version, commands, err)
		}
	}
	for _, tc := range []struct {
		a, b string
		ok   bool
	}{{"1.40.1", "1.40.1", true}, {"1.40.1", "1.40.2", true}, {"1.39.12", "1.40.1", true}, {"1.39.12", "1.41.1", false}, {"1.40.1", "1.39.12", false}, {"1.40.2", "1.40.1", false}, {"1.40.1", "2.0.0", false}, {"x", "1.40.1", false}} {
		if UpgradeAllowed(tc.a, tc.b) != tc.ok {
			t.Fatal(tc)
		}
	}
}
func surfaceCluster(topology string) *domain.Cluster {
	c := &domain.Cluster{Name: "vectors", Spec: domain.ClusterSpec{Topology: domain.Topology{Type: topology}}}
	New(Options{}).ApplyDefaults(&c.Spec)
	return c
}
func TestDescribeAndValidateDefaults(t *testing.T) {
	p := New(Options{})
	info := p.Describe()
	if info.Name != "weaviate" || info.DisplayName != "Weaviate" || len(info.Topologies) != 2 || !slices.Contains(info.Capabilities, "weaviateAccess") || !reflect.DeepEqual(info.BackupTypes, []string{"full"}) {
		t.Fatalf("%+v", info)
	}
	for _, topology := range []string{"single", "cluster"} {
		c := surfaceCluster(topology)
		if err := p.Validate(t.Context(), nil, c); err != nil {
			t.Fatal(err)
		}
		before := c.Spec
		p.ApplyDefaults(&c.Spec)
		if !reflect.DeepEqual(before, c.Spec) {
			t.Fatal("defaults are not idempotent")
		}
		o, err := parseOptions(c.Spec.EngineOptions)
		if err != nil || o.ReplicationMinimumFactor != c.Spec.Components.Engine.Replicas {
			t.Fatal(o, err)
		}
		if c.Spec.Components.Engine.Resources.MinRAM != 2 || c.Spec.TLS.Mode != "requireTLS" {
			t.Fatal(c.Spec)
		}
	}
	cases := []struct {
		name   string
		mutate func(*domain.Cluster)
	}{
		{"name", func(c *domain.Cluster) { c.Name = "Invalid" }},
		{"version", func(c *domain.Cluster) { c.Spec.Version = "1.40.2" }},
		{"replicas", func(c *domain.Cluster) { c.Spec.Components.Engine.Replicas = 2 }},
		{"RAM", func(c *domain.Cluster) { c.Spec.Components.Engine.Resources.MinRAM = 0.5 }},
		{"config", func(c *domain.Cluster) { c.Spec.EngineConfig = "AUTHENTICATION_ANONYMOUS_ACCESS_ENABLED=true" }},
		{"TLS", func(c *domain.Cluster) { c.Spec.TLS.Mode = "preferTLS" }},
		{"CA", func(c *domain.Cluster) { c.Spec.TLS.CA = "unknown" }},
		{"public plaintext", func(c *domain.Cluster) { c.Spec.TLS.Mode = "disabled"; c.Spec.Exposure.Public = true }},
		{"missing storage", func(c *domain.Cluster) { c.Spec.Backup.Storage = "absent" }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := surfaceCluster("single")
			tc.mutate(c)
			if p.Validate(t.Context(), nil, c) == nil {
				t.Fatal("accepted invalid spec")
			}
		})
	}
	old := surfaceCluster("cluster")
	old.Spec.Components.Engine.Replicas = 4
	n := surfaceCluster("cluster")
	if err := p.Validate(t.Context(), old, n); err != nil {
		t.Fatal(err)
	}
	old = surfaceCluster("single")
	n = surfaceCluster("cluster")
	if p.Validate(t.Context(), old, n) == nil {
		t.Fatal("topology mutation")
	}
}
func TestRelayAndMetricsCatalog(t *testing.T) {
	p := New(Options{})
	c := surfaceCluster("single")
	s := &state{Members: []*member{{Name: NodeHostname(c.Name, 0)}}}
	if err := s.save(c); err != nil {
		t.Fatal(err)
	}
	name := s.Members[0].Name
	if got := p.RelayTargets(c); !reflect.DeepEqual(got, []string{name + ":7947", name + ":8080", name + ":9443"}) {
		t.Fatal(got)
	}
	got := p.Metrics()
	if len(got) != len(wvspec.MetricNames) {
		t.Fatal(got)
	}
	for i, d := range got {
		if d.Exposition != wvspec.MetricNames[i] || d.Scale != 1 || d.Unit == "" || !strings.HasPrefix(d.Name, "weaviate.") {
			t.Fatal(d)
		}
	}
	if got := p.RequiredCapabilities(domain.ClusterSpec{Exposure: domain.Exposure{Public: true, Allowlist: []string{"192.0.2.0/24"}}}); !reflect.DeepEqual(got, []string{"publicTcp", "tcpAllowlist"}) {
		t.Fatal(got)
	}
}
func TestStatusConnectionAndFreshness(t *testing.T) {
	c := surfaceCluster("single")
	s := &state{Phase: "ready", Members: []*member{{Name: NodeHostname(c.Name, 0), PublicHTTPPort: 18080, PublicGRPCPort: 15051}}}
	v := connection(c, s)
	if v.URI != "https://vectorsn000000x:8080" || len(v.Endpoints) != 2 || v.Endpoints[1].URL != "vectorsn000000x:50051" {
		t.Fatal(v)
	}
	c.Spec.TLS.Mode = "disabled"
	if !strings.HasPrefix(connection(c, s).URI, "http://") {
		t.Fatal("plaintext scheme")
	}
	c.Spec.Exposure.Public = true
	c.Spec.Exposure.IPType = "IPV4"
	v = connectionWithProject(c, s, infra.Project{PublicIPv4: "192.0.2.1"})
	if len(v.PublicHosts) != 1 || v.PublicHosts[0] != "192.0.2.1:18080" {
		t.Fatal(v)
	}
	at := time.Now()
	status := engine.FreshStatus(domain.ClusterStatus{Phase: domain.PhaseReady, ObservedAt: at.Add(-engine.ReadinessMaxAge - time.Second), Members: []domain.MemberStatus{{Health: true, Agent: domain.AgentStatus{Healthy: true}}}}, at)
	if status.Phase != domain.PhaseDegraded || status.Members[0].Health || status.Members[0].Agent.Healthy {
		t.Fatal("stale readiness retained")
	}
	p := New(Options{})
	users, err := p.SystemUsers(context.Background(), nil)
	if err != nil || len(users) != 2 || users[0].Name != AdminUser {
		t.Fatal(users, err)
	}
	if _, err := p.PITRWindow(context.Background(), nil); err != engine.ErrNotSupported {
		t.Fatal(err)
	}
}
