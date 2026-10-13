// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package rabbitmq

import (
	"context"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"net"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/rabbitmq/rmqspec"
	"github.com/KarakoramDev/Karakoram/internal/infra"
	"github.com/KarakoramDev/Karakoram/internal/store"
	"gopkg.in/yaml.v3"
)

func TestCatalogAndValidation(t *testing.T) {
	p, rc, _, _ := setup(t)
	ctx := context.Background()
	info := p.Describe()
	if info.Name != "rabbitmq" || info.DisplayName != "RabbitMQ" || len(info.Versions) != 2 || info.Versions[0].Version != "4.3.5" || info.Versions[1].Version != "4.3.6" || !info.Versions[1].Default {
		t.Fatalf("catalog: %+v", info)
	}
	if len(info.UpgradePaths) != 1 || info.UpgradePaths[0].From != "4.3.5" || info.UpgradePaths[0].To != "4.3.6" {
		t.Fatal(info.UpgradePaths)
	}
	if len(info.Topologies) != 1 || info.Topologies[0].Type != "cluster" || !slices.Equal(info.Topologies[0].Components["engine"].AllowedReplicas, []int{1, 3, 5, 7}) {
		t.Fatal(info.Topologies)
	}
	if !slices.Equal(info.BackupTypes, []string{"definitions"}) || !slices.Equal(info.Processes, []string{"rabbitmq-server"}) {
		t.Fatal(info)
	}
	for _, capability := range []string{"systemUsers", "backups", "restores", "publicExposure", "tls", "monitoring", "engineConfig", "rabbitmqAccess"} {
		if !slices.Contains(info.Capabilities, capability) {
			t.Errorf("missing capability %s", capability)
		}
	}
	for _, operation := range []string{"backup", "delete-backup", "restore", "restart", "replace-member", "rotate-system-user", "rotate-tls"} {
		if !slices.Contains(info.Operations, operation) {
			t.Errorf("missing operation %s", operation)
		}
	}
	if rc.Cluster.Spec.Version != "4.3.6" || rc.Cluster.Spec.Components.Engine.Replicas != 3 || rc.Cluster.Spec.Components.Engine.Resources.MinDisk != 5 || rc.Cluster.Spec.TLS.Mode != "requireTLS" {
		t.Fatal(rc.Cluster.Spec)
	}
	for _, version := range []string{"4.3.5", "4.3.6"} {
		for _, replicas := range []int{1, 3, 5, 7} {
			for _, mode := range []string{"disabled", "preferTLS", "requireTLS"} {
				c := cloneCluster(t, rc.Cluster)
				c.Spec.Version = version
				c.Spec.Components.Engine.Replicas = replicas
				c.Spec.TLS.Mode = mode
				if err := p.Validate(ctx, nil, c); err != nil {
					t.Errorf("%s/%d/%s: %v", version, replicas, mode, err)
				}
			}
		}
	}
	invalidCases := []struct {
		name   string
		change func(*domain.Cluster)
	}{
		{"name", func(c *domain.Cluster) { c.Name = "bad-name" }}, {"provider", func(c *domain.Cluster) { c.Spec.Provider = "psmdb" }}, {"version", func(c *domain.Cluster) { c.Spec.Version = "4.4.0" }}, {"topology", func(c *domain.Cluster) { c.Spec.Topology.Type = "replicaSet" }}, {"shards", func(c *domain.Cluster) { c.Spec.Topology.Shards = 1 }},
		{"config server", func(c *domain.Cluster) { c.Spec.Components.ConfigServer.Replicas = 1 }}, {"router", func(c *domain.Cluster) { c.Spec.Components.Router.Replicas = 1 }}, {"extra", func(c *domain.Cluster) {
			c.Spec.Components.Extra = map[string]domain.ComponentSpec{"other": {Replicas: 1}}
		}},
		{"cpu mode", func(c *domain.Cluster) { c.Spec.Components.Engine.Resources.CPUMode = "INVALID" }}, {"fractional CPU", func(c *domain.Cluster) { c.Spec.Components.Engine.Resources.MinCPU = 0.5 }}, {"ram bounds", func(c *domain.Cluster) { c.Spec.Components.Engine.Resources.MaxRAM = 0.1 }}, {"disk floor", func(c *domain.Cluster) { c.Spec.Components.Engine.Resources.MinDisk = 4 }},
		{"tls mode", func(c *domain.Cluster) { c.Spec.TLS.Mode = "optional" }}, {"tls CA", func(c *domain.Cluster) { c.Spec.TLS.CA = "unknown" }}, {"public allowlist", func(c *domain.Cluster) { c.Spec.Exposure.Public = true }}, {"CIDR", func(c *domain.Cluster) { c.Spec.Exposure.Allowlist = []string{"not-a-cidr"} }}, {"ip type", func(c *domain.Cluster) { c.Spec.Exposure.IPType = "both" }},
		{"PITR", func(c *domain.Cluster) { c.Spec.Backup.PITR.Enabled = true }}, {"missing storage", func(c *domain.Cluster) { c.Spec.Backup.Storage = "missing" }},
	}
	for _, replicas := range []int{-1, 0, 2, 4, 6, 8, 9} {
		n := replicas
		invalidCases = append(invalidCases, struct {
			name   string
			change func(*domain.Cluster)
		}{fmt.Sprintf("replicas %d", n), func(c *domain.Cluster) { c.Spec.Components.Engine.Replicas = n }})
	}
	for _, test := range invalidCases {
		t.Run(test.name, func(t *testing.T) {
			c := cloneCluster(t, rc.Cluster)
			test.change(c)
			if err := p.Validate(ctx, nil, c); !errors.Is(err, engine.ErrInvalid) {
				t.Fatalf("expected invalid: %v", err)
			}
		})
	}
	for _, test := range []struct {
		name   string
		change func(*domain.Cluster)
	}{{"identity", func(c *domain.Cluster) { c.Name = "other" }}, {"namespace", func(c *domain.Cluster) { c.Namespace = "other" }}, {"CA source", func(c *domain.Cluster) { c.Spec.TLS.CA = "userProvided" }}, {"disk shrink", func(c *domain.Cluster) { c.Spec.Components.Engine.Resources.MaxDisk = 5 }}, {"downgrade", func(c *domain.Cluster) { c.Spec.Version = "4.3.5" }}, {"dataSource", func(c *domain.Cluster) { c.Spec.DataSource = &domain.DataSource{Cluster: "source", Backup: "backup"} }}} {
		t.Run("immutable/"+test.name, func(t *testing.T) {
			old := cloneCluster(t, rc.Cluster)
			old.Spec.Components.Engine.Resources.MaxDisk = 10
			c := cloneCluster(t, old)
			test.change(c)
			if err := p.Validate(ctx, old, c); !errors.Is(err, engine.ErrInvalid) {
				t.Fatalf("accepted update: %v", err)
			}
		})
	}
	old := cloneCluster(t, rc.Cluster)
	old.Spec.Version = "4.3.5"
	if err := p.Validate(ctx, old, rc.Cluster); err != nil {
		t.Fatal("patch upgrade rejected", err)
	}
	c := cloneCluster(t, rc.Cluster)
	c.Spec.Exposure.Public = true
	c.Spec.Exposure.Allowlist = []string{"2001:db8::/32", "192.0.2.0/24"}
	if err := p.Validate(ctx, nil, c); err != nil {
		t.Fatal(err)
	}
}

func TestEngineOptionsAndManagedConfigValidation(t *testing.T) {
	p, rc, _, _ := setup(t)
	ctx := context.Background()
	for _, raw := range []string{`{"unknown":true}`, `null`, `[]`, `{"managementUI":{"unknown":true}}`, `{"plugins":["rabbitmq_management"]}`, `{"plugins":["rabbitmq_shovel","rabbitmq_shovel"]}`, `{"plugins":"rabbitmq_shovel"}`, `{} {}`} {
		c := cloneCluster(t, rc.Cluster)
		c.Spec.EngineOptions = json.RawMessage(raw)
		if err := p.Validate(ctx, nil, c); !errors.Is(err, engine.ErrInvalid) {
			t.Errorf("accepted options %s: %v", raw, err)
		}
	}
	for _, key := range []string{"listeners.tcp.default", "ssl_options.verify", "management.tcp.port", "management.ssl.port", "prometheus.tcp.port", "cluster_formation.peer_discovery_backend", "vm_memory_high_watermark.absolute", "total_memory_available_override_value", "disk_free_limit.absolute", "log.console", "default_user", "default_pass", "loopback_users", "distribution.listener.port_range.min", "default_queue_type", "quorum_queue.initial_cluster_size", "management.load_definitions", "load_definitions", "definitions.import_backend", "auth_backends.1", "ssl_cert_login_from"} {
		c := cloneCluster(t, rc.Cluster)
		c.Spec.EngineConfig = key + " = changed\n"
		if err := p.Validate(ctx, nil, c); !errors.Is(err, engine.ErrInvalid) {
			t.Errorf("accepted managed key %s: %v", key, err)
		}
	}
	for _, text := range []string{"malformed", "key = ", " = value", "invalid key = value"} {
		c := cloneCluster(t, rc.Cluster)
		c.Spec.EngineConfig = text
		if err := p.Validate(ctx, nil, c); !errors.Is(err, engine.ErrInvalid) {
			t.Errorf("accepted config %q: %v", text, err)
		}
	}
	c := cloneCluster(t, rc.Cluster)
	c.Spec.EngineConfig = "# customer settings\ncluster_name = customers\nconsumer_timeout = 600000\n"
	c.Spec.EngineOptions = json.RawMessage(`{"plugins":["rabbitmq_shovel","rabbitmq_shovel_management","rabbitmq_federation","rabbitmq_federation_management","rabbitmq_stream","rabbitmq_stream_management"],"managementUI":{"public":true}}`)
	if err := p.Validate(ctx, nil, c); err != nil {
		t.Fatal(err)
	}
}

func TestDataSourceValidation(t *testing.T) {
	p, rc, _, _ := setup(t)
	ctx := context.Background()
	st := rc.Store.(*store.Store)
	source := &domain.Cluster{Name: "source"}
	p.ApplyDefaults(&source.Spec)
	if err := st.CreateCluster(ctx, source); err != nil {
		t.Fatal(err)
	}
	b := &domain.Backup{Name: "definitions", Cluster: source.Name, Namespace: source.Ref().Namespace, Provider: "rabbitmq", Type: "definitions", Phase: domain.PhaseSucceeded, Storage: "s3", Location: "source/definitions.json.gz"}
	if err := st.CreateBackup(ctx, b); err != nil {
		t.Fatal(err)
	}
	c := cloneCluster(t, rc.Cluster)
	c.Spec.DataSource = &domain.DataSource{Cluster: source.Name, Backup: b.Name}
	if err := p.Validate(ctx, nil, c); err != nil {
		t.Fatal(err)
	}
	explicit := cloneCluster(t, c)
	explicit.Spec.DataSource.Namespace = c.Ref().Namespace
	if err := p.Validate(ctx, c, explicit); err != nil {
		t.Fatal("equivalent namespace rejected", err)
	}
	for _, test := range []struct {
		name   string
		change func(*domain.DataSource)
	}{{"self", func(d *domain.DataSource) { d.Cluster = rc.Cluster.Name }}, {"missing cluster", func(d *domain.DataSource) { d.Cluster = "" }}, {"missing backup", func(d *domain.DataSource) { d.Backup = "" }}, {"unknown backup", func(d *domain.DataSource) { d.Backup = "missing" }}, {"unknown namespace", func(d *domain.DataSource) { d.Namespace = "missing" }}, {"PITR", func(d *domain.DataSource) { at := time.Now(); d.PITRTime = &at }}} {
		t.Run(test.name, func(t *testing.T) {
			bad := cloneCluster(t, c)
			test.change(bad.Spec.DataSource)
			if err := p.Validate(ctx, nil, bad); !errors.Is(err, engine.ErrInvalid) {
				t.Fatal(err)
			}
		})
	}
	for _, variant := range []string{"wrong provider", "wrong type", "not succeeded"} {
		copy := *b
		switch variant {
		case "wrong provider":
			copy.Provider = "psmdb"
		case "wrong type":
			copy.Type = "logical"
		case "not succeeded":
			copy.Phase = domain.PhasePending
		}
		if err := st.UpdateBackup(ctx, &copy); err != nil {
			t.Fatal(err)
		}
		if err := p.Validate(ctx, nil, c); !errors.Is(err, engine.ErrInvalid) {
			t.Errorf("%s: %v", variant, err)
		}
	}
}

func TestZeropsYAMLAndMemberConfigTLSModes(t *testing.T) {
	for _, mode := range []string{"disabled", "preferTLS", "requireTLS"} {
		for _, publicUI := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/public=%v", mode, publicUI), func(t *testing.T) {
				p, rc, _, _ := setup(t)
				rc.Cluster.Spec.TLS.Mode = mode
				rc.Cluster.Spec.EngineOptions = json.RawMessage(fmt.Sprintf(`{"plugins":["rabbitmq_stream"],"managementUI":{"public":%v}}`, publicUI))
				rc.Cluster.Spec.EngineConfig = "consumer_timeout = 600000\n"
				s := state(t, rc)
				s.UID = "config"
				for range 3 {
					s.add(rc.Cluster.Name)
				}
				if err := p.ensureSecrets(context.Background(), rc, s); err != nil {
					t.Fatal(err)
				}
				if err := s.save(rc.Cluster); err != nil {
					t.Fatal(err)
				}
				m := s.Members[0]
				text, err := p.zeropsYAML(rc, m, "4.3.5")
				if err != nil {
					t.Fatal(err)
				}
				var doc map[string]any
				if err = yaml.Unmarshal([]byte(text), &doc); err != nil {
					t.Fatal(err)
				}
				for _, want := range []string{"temporaryShutdown: true", "/var/www/karakoram agent readiness --engine=rabbitmq --data-dir=/mnt/demom0v", "failureTimeout: 300s", "retryPeriod: 10s", "ubuntu@24.04", "hostname: demom0v", "rabbitmq-server=4.3.5-1", "1:27.3.4.17-1", "policy-rc.d", "deb1.rabbitmq.com", "deb2.rabbitmq.com", "KARAKORAM_URL", "KARAKORAM_CLUSTER", "KARAKORAM_MEMBER", "KARAKORAM_INTERNAL_CA", "KARAKORAM_ENGINE: rabbitmq", "KARAKORAM_DATA_DIR: /mnt/demom0v", "./karakoram agent"} {
					if !strings.Contains(text, want) {
						t.Errorf("missing %q in YAML:\n%s", want, text)
					}
				}
				for _, port := range []int{5672, 5671, 15672, 15671, 15692, 4369, 25672} {
					if !strings.Contains(text, fmt.Sprintf("port: %d", port)) {
						t.Errorf("missing port %d", port)
					}
				}
				run := doc["zerops"].([]any)[0].(map[string]any)["run"].(map[string]any)
				for _, entry := range run["ports"].([]any) {
					port := entry.(map[string]any)
					httpSupport, _ := port["httpSupport"].(bool)
					if httpSupport != (port["port"] == 15672 && publicUI) {
						t.Fatalf("httpSupport mismatch: %+v", port)
					}
				}
				prepare := run["prepareCommands"].([]any)
				install := ""
				policyIndex, installIndex := -1, -1
				for i, command := range prepare {
					text := command.(string)
					if strings.Contains(text, "policy-rc.d") {
						policyIndex = i
					}
					if strings.Contains(text, "rabbitmq-server=4.3.5-1") {
						installIndex = i
						install = text
					}
				}
				if policyIndex < 0 || installIndex < 0 || policyIndex >= installIndex {
					t.Fatal("broker package could start before policy-rc.d prevents system startup")
				}
				for _, pkg := range []string{"base", "asn1", "crypto", "eldap", "inets", "mnesia", "os-mon", "parsetools", "public-key", "runtime-tools", "ssl", "syntax-tools", "tools", "xmerl"} {
					if !strings.Contains(install, "erlang-"+pkg+"=1:27.3.4.17-1") {
						t.Errorf("unpinned Erlang package %s", pkg)
					}
				}
				newer, err := p.zeropsYAML(rc, m, "4.3.6")
				if err != nil {
					t.Fatal(err)
				}
				if text == newer || !strings.Contains(newer, "rabbitmq-server=4.3.6-1") {
					t.Fatal("version not embedded in prepare cache key")
				}
				cfg, err := p.MemberConfig(context.Background(), rc, m.Name)
				if err != nil {
					t.Fatal(err)
				}
				var spec rmqspec.MemberSpec
				if err = json.Unmarshal(cfg.EngineSpec, &spec); err != nil {
					t.Fatal(err)
				}
				if cfg.Engine != "rabbitmq" || cfg.Member != m.Name || cfg.TLS.Mode != mode || cfg.ConfigVersion != s.ConfigVersion || len(cfg.SystemUsers) < 2 || cfg.SystemUsers["admin"].Password == "" || cfg.SystemUsers["karakoram"].Password == "" {
					t.Fatalf("member config: %+v", cfg)
				}
				if spec.NodeName != "rabbit@demom0" || !slices.Equal(spec.Peers, []string{"rabbit@demom0", "rabbit@demom1", "rabbit@demom2"}) || spec.Cookie == "" || spec.Version != "4.3.6" || spec.DefaultQueueType != "quorum" || spec.PrometheusPort != 15692 || spec.ConfigOverrides != rc.Cluster.Spec.EngineConfig || spec.TotalMemoryBytes <= 0 || spec.MemoryHighWatermarkBytes <= 0 || spec.MemoryHighWatermarkBytes >= spec.TotalMemoryBytes || spec.DiskFreeLimitBytes <= 0 || !slices.Contains(spec.Plugins, "rabbitmq_management") || !slices.Contains(spec.Plugins, "rabbitmq_prometheus") || !slices.Contains(spec.Plugins, "rabbitmq_stream") {
					t.Fatalf("member spec: %+v", spec)
				}
				amqp, amqps, mgmt, mgmts := 5672, 0, 15672, 0
				if mode != "disabled" {
					amqps = 5671
					mgmts = 15671
				}
				if mode == "requireTLS" {
					amqp = 0
					if !publicUI {
						mgmt = 0
					}
				}
				if spec.AMQPPort != amqp || spec.AMQPSPort != amqps || spec.ManagementPort != mgmt || spec.ManagementTLSPort != mgmts || spec.InterNodeTLS != (mode == "requireTLS") {
					t.Fatalf("TLS listeners: %+v", spec)
				}
				if mode != "disabled" {
					block, _ := pem.Decode([]byte(cfg.TLS.CertPEM))
					if block == nil {
						t.Fatal("missing certificate")
					}
					cert, err := x509.ParseCertificate(block.Bytes)
					if err != nil {
						t.Fatal(err)
					}
					for _, host := range []string{m.Name, "127.0.0.1", "2001:db8::1"} {
						if err = cert.VerifyHostname(host); err != nil {
							t.Error(err)
						}
					}
				}
			})
		}
	}
}

func TestExposurePortAllocationRelayAndMetrics(t *testing.T) {
	p, rc, f, _ := setup(t)
	f.publicPorts = []infra.PublicPortRoute{{Hostname: "foreign", PublicPort: infra.PublicPort{Port: 5671, IPType: "IPV6"}}}
	rc.Cluster.Spec.Exposure.Public = true
	rc.Cluster.Spec.Exposure.Allowlist = []string{"2001:db8::/32"}
	rc.Cluster.Spec.EngineOptions = json.RawMessage(`{"managementUI":{"public":true}}`)
	ready(t, p, rc)
	s := state(t, rc)
	ports := map[int]bool{5671: true}
	for _, m := range s.Members {
		if m.PublicPort < 5672 || ports[m.PublicPort] {
			t.Fatalf("port collision: %+v", m)
		}
		ports[m.PublicPort] = true
		if !f.subdomains[m.Name] {
			t.Fatalf("management not exposed: %s", m.Name)
		}
	}
	for _, r := range f.publicPorts {
		if r.Hostname != "foreign" && (r.TargetPort != 5671 || !slices.Equal(r.Allowlist, rc.Cluster.Spec.Exposure.Allowlist)) {
			t.Fatal(r)
		}
	}
	targets := p.RelayTargets(rc.Cluster)
	for _, m := range s.Members {
		for _, port := range []int{9443, 5672, 5671, 15672, 15671} {
			target := net.JoinHostPort(m.Name, strconv.Itoa(port))
			if !slices.Contains(targets, target) {
				t.Errorf("missing relay target %s", target)
			}
		}
	}
	metrics := p.Metrics()
	seen := map[string]bool{}
	for _, metric := range metrics {
		if seen[metric.Exposition] || metric.Name == "" || metric.Scale <= 0 {
			t.Errorf("invalid metric %+v", metric)
		}
		seen[metric.Exposition] = true
	}
	if len(metrics) != len(rmqspec.MetricNames) {
		t.Fatal(metrics)
	}
	for _, name := range rmqspec.MetricNames {
		if !seen[name] {
			t.Errorf("missing metric %s", name)
		}
	}
	rc.Cluster.Spec.Exposure.Public = false
	rc.Cluster.Spec.EngineOptions = json.RawMessage(`{"managementUI":{"public":false}}`)
	ready(t, p, rc)
	for _, r := range f.publicPorts {
		if r.Hostname != "foreign" {
			t.Fatal("owned exposure survived", r)
		}
	}
	for _, m := range state(t, rc).Members {
		if f.subdomains[m.Name] || m.ManagementPublic {
			t.Fatal("public management survived", m.Name)
		}
	}
}

func TestProjectPortReservationsAreStableAcrossClusters(t *testing.T) {
	p, rc, f, _ := setup(t)
	ctx := context.Background()
	s := state(t, rc)
	m := s.add(rc.Cluster.Name)
	f.publicPorts = []infra.PublicPortRoute{{Hostname: "foreign", PublicPort: infra.PublicPort{Port: 5671, IPType: "IPV6"}}}
	first, err := p.allocatePublicPort(ctx, rc, s, m)
	if err != nil || first != 5672 {
		t.Fatal(first, err)
	}
	other := cloneCluster(t, rc.Cluster)
	other.Name = "other"
	other.Internal = nil
	otherRC := *rc
	otherRC.Cluster = other
	otherState, err := loadState(other)
	if err != nil {
		t.Fatal(err)
	}
	otherMember := otherState.add(other.Name)
	second, err := p.allocatePublicPort(ctx, &otherRC, otherState, otherMember)
	if err != nil || second != 5673 {
		t.Fatal(second, err)
	}
	again, err := p.allocatePublicPort(ctx, rc, s, m)
	if err != nil || again != first {
		t.Fatal("reservation unstable", again, err)
	}
}
