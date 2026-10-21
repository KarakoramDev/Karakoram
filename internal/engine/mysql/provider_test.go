// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package mysql

import (
	"context"
	"encoding/json"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine/nativeartifacts"
	"testing"
)

func defaultCluster() *domain.Cluster {
	c := &domain.Cluster{Name: "sample"}
	New(Options{}).ApplyDefaults(&c.Spec)
	return c
}
func TestCatalogAndDefaults(t *testing.T) {
	p := New(Options{})
	if p.Name() != "mysql" {
		t.Fatal(p.Name())
	}
	info := p.Describe()
	if len(info.Topologies) != 2 || len(info.Presets) != 2 || len(info.BackupTypes) != 2 {
		t.Fatalf("incomplete catalog: %+v", info)
	}
	for _, v := range info.Versions {
		s, b, err := ArtifactPins(v.Version)
		if err != nil {
			t.Fatal(err)
		}
		server, err := nativeartifacts.Get(s)
		if err != nil || server.Version != v.Version {
			t.Fatalf("server pin %s: %v", s, err)
		}
		backup, err := nativeartifacts.Get(b)
		if err != nil || major(backup.Version) != v.Major {
			t.Fatalf("backup pin %s: %v", b, err)
		}
	}
	c := defaultCluster()
	if c.Spec.Version != "8.4.11-11" || c.Spec.Components.Extra["proxy"].Replicas != 1 {
		t.Fatal(c.Spec)
	}
	if err := p.Validate(context.Background(), nil, c); err != nil {
		t.Fatal(err)
	}
	group := &domain.Cluster{Name: "sample", Spec: domain.ClusterSpec{Topology: domain.Topology{Type: "group"}}}
	p.ApplyDefaults(&group.Spec)
	if group.Spec.Components.Engine.Replicas != 3 {
		t.Fatal("group default")
	}
	if err := p.Validate(context.Background(), nil, group); err != nil {
		t.Fatal(err)
	}
	proxy := c.Spec.Components.Extra["proxy"]
	proxy.Replicas = 0
	c.Spec.Components.Extra["proxy"] = proxy
	if err := p.Validate(context.Background(), nil, c); err != nil {
		t.Fatal(err)
	}
}
func TestAdmissionRejects(t *testing.T) {
	for name, mutate := range map[string]func(*domain.Cluster){"name": func(c *domain.Cluster) { c.Name = "bad-name" }, "version": func(c *domain.Cluster) { c.Spec.Version = "8.4.99-1" }, "replicas": func(c *domain.Cluster) { c.Spec.Components.Engine.Replicas = 2 }, "topology": func(c *domain.Cluster) { c.Spec.Topology.Type = "ha" }, "group proxy": func(c *domain.Cluster) {
		c.Spec.Topology.Type = "group"
		c.Spec.Components.Engine.Replicas = 3
		p := c.Spec.Components.Extra["proxy"]
		p.Replicas = 0
		c.Spec.Components.Extra["proxy"] = p
	}, "tls": func(c *domain.Cluster) { c.Spec.TLS.Mode = "disabled" }, "options": func(c *domain.Cluster) { c.Spec.EngineOptions = json.RawMessage(`{"parameters":{"server_id":"1"}}`) }, "config": func(c *domain.Cluster) { c.Spec.EngineConfig = "arbitrary" }} {
		t.Run(name, func(t *testing.T) {
			c := defaultCluster()
			mutate(c)
			if New(Options{}).Validate(context.Background(), nil, c) == nil {
				t.Fatal("accepted invalid cluster")
			}
		})
	}
}
func TestUpgradeAndTopologyChanges(t *testing.T) {
	p := New(Options{})
	old, n := defaultCluster(), defaultCluster()
	old.Spec.Version = "8.0.46-37"
	if err := p.Validate(context.Background(), old, n); err != nil {
		t.Fatal(err)
	}
	old.Spec.Version = "8.4.11-11"
	n.Spec.Version = "8.0.46-37"
	if p.Validate(context.Background(), old, n) == nil {
		t.Fatal("downgrade accepted")
	}
	n = defaultCluster()
	n.Spec.Topology.Type = "group"
	n.Spec.Components.Engine.Replicas = 3
	if err := p.Validate(context.Background(), old, n); err != nil {
		t.Fatal(err)
	}
	old, n = n, defaultCluster()
	if p.Validate(context.Background(), old, n) == nil {
		t.Fatal("group to single accepted")
	}
	if upgradeAllowed("8.4.11-11", "9.0.1-1") {
		t.Fatal("skip major accepted")
	}
	if upgradeAllowed("8.4.11-11", "8.4.10-10") {
		t.Fatal("minor downgrade accepted")
	}
	if !upgradeAllowed("8.4.10-10", "8.4.11-11") {
		t.Fatal("minor upgrade rejected")
	}
}
func TestOptions(t *testing.T) {
	o, err := ParseOptions(nil)
	if err != nil || o.GroupReplication.Consistency != "BEFORE_ON_PRIMARY_FAILOVER" || o.Binlog.ArchiveInterval != 300 {
		t.Fatalf("defaults: %+v %v", o, err)
	}
	for _, raw := range []string{`null`, `[]`, `{} {}`, `{"extra":true}`, `{"proxy":{"unknown":true}}`, `{"proxy":null}`, `{"groupReplication":{"consistency":"bad"}}`, `{"groupReplication":{"unreachableMajorityTimeout":0}}`, `{"binlog":{"archiveInterval":300,"expireSeconds":200}}`, `{"parameters":{"require_secure_transport":"OFF"}}`, `{"parameters":{"max_connections":"bad"}}`} {
		if _, err := ParseOptions(json.RawMessage(raw)); err == nil {
			t.Fatalf("accepted %s", raw)
		}
	}
	if _, err := ParseOptions(json.RawMessage(`{"proxy":{"readWriteSplit":true,"readerPort":true},"parameters":{"max_connections":"200"}}`)); err != nil {
		t.Fatal(err)
	}
}
