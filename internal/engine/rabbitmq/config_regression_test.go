// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package rabbitmq

import (
	"context"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"errors"
	"net"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/rabbitmq/rmqspec"
	"github.com/KarakoramDev/Karakoram/internal/pki"
	"gopkg.in/yaml.v3"
)

func TestProvidedCAMemberCertificates(t *testing.T) {
	p, rc, _, _ := setup(t)
	rc.Cluster.Spec.TLS.CA = "userProvided"
	ctx := context.Background()
	s := state(t, rc)
	s.add(rc.Cluster.Name)
	if err := p.ensureSecrets(ctx, rc, s); err == nil {
		t.Fatal("missing customer CA accepted")
	}
	ca, err := pki.NewCA("customer", 365*24*time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	for key, value := range map[string][]byte{"tls/ca.crt": ca.CertPEM, "tls/ca.key": ca.KeyPEM} {
		if err := rc.Secrets.SetClusterSecret(ctx, rc.Cluster.Ref(), key, value); err != nil {
			t.Fatal(err)
		}
	}
	if err := p.ensureSecrets(ctx, rc, s); err != nil {
		t.Fatal(err)
	}
	if err := s.save(rc.Cluster); err != nil {
		t.Fatal(err)
	}
	cfg, err := p.MemberConfig(ctx, rc, "demom0")
	if err != nil {
		t.Fatal(err)
	}
	if cfg.TLS.CAPEM != string(ca.CertPEM) || cfg.AgentTLS.CertPEM == "" || cfg.AgentTLS.KeyPEM == "" {
		t.Fatal("customer CA or separate agent certificate not propagated")
	}
	block, _ := pem.Decode([]byte(cfg.TLS.CertPEM))
	if block == nil {
		t.Fatal("missing member certificate")
	}
	leaf, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		t.Fatal(err)
	}
	roots := x509.NewCertPool()
	roots.AppendCertsFromPEM(ca.CertPEM)
	if _, err := leaf.Verify(x509.VerifyOptions{Roots: roots, DNSName: "demom0"}); err != nil {
		t.Fatal(err)
	}
}

func TestExposureTargetsFollowTLSModeAndAddressFamily(t *testing.T) {
	for _, mode := range []string{"disabled", "preferTLS", "requireTLS"} {
		for _, family := range []string{"IPV4", "IPV6"} {
			t.Run(mode+"/"+family, func(t *testing.T) {
				p, rc, f, _ := setup(t)
				rc.Cluster.Spec.Components.Engine.Replicas = 1
				rc.Cluster.Spec.TLS.Mode = mode
				rc.Cluster.Spec.Exposure.Public = true
				rc.Cluster.Spec.Exposure.IPType = family
				rc.Cluster.Spec.Exposure.Allowlist = []string{"192.0.2.0/24", "2001:db8::/32"}
				ready(t, p, rc)
				if len(f.publicPorts) != 1 {
					t.Fatal(f.publicPorts)
				}
				target := 5671
				if mode == "disabled" {
					target = 5672
				}
				route := f.publicPorts[0]
				if route.Hostname != "demom0" || route.TargetPort != target || route.Protocol != "tcp" || route.IPType != family || !slices.Equal(route.Allowlist, rc.Cluster.Spec.Exposure.Allowlist) {
					t.Fatal(route)
				}
			})
		}
	}
}

func TestRelayTargetsIncludePlannedObservedAndRemovingMembers(t *testing.T) {
	p, rc, _, _ := setup(t)
	if got := p.RelayTargets(nil); len(got) != 0 {
		t.Fatal(got)
	}
	if got := p.RelayTargets(rc.Cluster); len(got) != 15 {
		t.Fatal("fresh plan must be reachable before infrastructure provisioning", got)
	}
	s := state(t, rc)
	m := s.add(rc.Cluster.Name)
	m.Removing = true
	if err := s.save(rc.Cluster); err != nil {
		t.Fatal(err)
	}
	rc.Cluster.Status.Members = []domain.MemberStatus{{Name: m.Name}, {Name: "demom9"}}
	targets := p.RelayTargets(rc.Cluster)
	if len(targets) != 10 || !slices.IsSorted(targets) {
		t.Fatal("relay targets must be sorted, deduplicated and preserve observed identities", targets)
	}
	for _, host := range []string{m.Name, "demom9"} {
		for _, port := range []int{9443, 5672, 5671, 15672, 15671} {
			if !slices.Contains(targets, net.JoinHostPort(host, strconv.Itoa(port))) {
				t.Fatalf("missing relay target %s port %d", host, port)
			}
		}
	}
}

func TestTLSUpdatesPreserveCreationTimeDistributionTransport(t *testing.T) {
	for _, from := range []string{"disabled", "preferTLS", "requireTLS"} {
		t.Run(from, func(t *testing.T) {
			p, rc, _, _ := setup(t)
			rc.Cluster.Spec.TLS.Mode = from
			ready(t, p, rc)
			old := cloneCluster(t, rc.Cluster)
			for _, to := range []string{"disabled", "preferTLS", "requireTLS"} {
				next := cloneCluster(t, old)
				next.Spec.TLS.Mode = to
				err := p.Validate(context.Background(), old, next)
				crossesDistribution := (from == "requireTLS") != (to == "requireTLS")
				if crossesDistribution {
					if !errors.Is(err, engine.ErrInvalid) || !strings.Contains(err.Error(), "inter-node TLS is fixed at creation") {
						t.Fatalf("%s -> %s accepted incompatible distribution: %v", from, to, err)
					}
				} else if err != nil {
					t.Fatalf("%s -> %s rejected listener-only TLS update: %v", from, to, err)
				}
				// Even if a persisted desired spec or per-member listener snapshot
				// changes, configuration must never change the cluster transport.
				rc.Cluster = next
				s := state(t, rc)
				s.Members[0].TLSMode = to
				if err := s.save(rc.Cluster); err != nil {
					t.Fatal(err)
				}
				cfg, err := p.MemberConfig(context.Background(), rc, s.Members[0].Name)
				if err != nil {
					t.Fatal(err)
				}
				var spec rmqspec.MemberSpec
				if err := json.Unmarshal(cfg.EngineSpec, &spec); err != nil {
					t.Fatal(err)
				}
				if spec.InterNodeTLS != (from == "requireTLS") {
					t.Fatalf("%s -> %s changed creation-time distribution: %+v", from, to, spec)
				}
			}
		})
	}
}

func TestAutomaticLeafRenewalPublishesOnlyAfterSafeStop(t *testing.T) {
	p, rc, _, a := setup(t)
	ready(t, p, rc)
	ctx := context.Background()
	caCert, err := rc.Secrets.GetClusterSecret(ctx, rc.Cluster.Ref(), "tls/ca.crt")
	if err != nil {
		t.Fatal(err)
	}
	caKey, err := rc.Secrets.GetClusterSecret(ctx, rc.Cluster.Ref(), "tls/ca.key")
	if err != nil {
		t.Fatal(err)
	}
	ca, err := pki.LoadCA(caCert, caKey)
	if err != nil {
		t.Fatal(err)
	}
	for _, kind := range []string{"members", "agents"} {
		cert, key, err := ca.Issue(pki.CertRequest{
			CommonName: "demom0",
			DNSNames:   []string{"demom0", "localhost"},
			IPs:        []net.IP{net.ParseIP("127.0.0.1")},
			Validity:   time.Hour,
			Server:     true,
			Client:     kind == "members",
		})
		if err != nil {
			t.Fatal(err)
		}
		if err := rc.Secrets.SetClusterSecret(ctx, rc.Cluster.Ref(), "tls/"+kind+"/demom0.pem", append(cert, key...)); err != nil {
			t.Fatal(err)
		}
	}
	before, err := p.MemberConfig(ctx, rc, "demom0")
	if err != nil {
		t.Fatal(err)
	}
	baseline := state(t, rc)
	versions := map[string]int64{}
	for _, m := range baseline.Members {
		versions[m.Name] = m.ConfigVersion
	}
	if err := p.ensureSecrets(ctx, rc, baseline); err != nil {
		t.Fatal(err)
	}
	if err := baseline.save(rc.Cluster); err != nil {
		t.Fatal(err)
	}
	pending := map[string]string{}
	for _, kind := range []string{"members", "agents"} {
		bundle, err := rc.Secrets.GetClusterSecret(ctx, rc.Cluster.Ref(), "tls/"+kind+"/demom0.pending.pem")
		if err != nil {
			t.Fatalf("%s renewal not staged: %v", kind, err)
		}
		pending[kind] = string(bundle)
	}
	after, err := p.MemberConfig(ctx, rc, "demom0")
	if err != nil {
		t.Fatal(err)
	}
	if after.ConfigVersion != before.ConfigVersion || after.TLS.CertPEM != before.TLS.CertPEM || after.AgentTLS != before.AgentTLS {
		t.Fatal("pending renewal escaped the per-member publication gate")
	}
	a.log.clear()
	syncUntil(t, p, rc, func(s *internalState) bool {
		return s.Members[0].ChangeStep == "platform"
	})
	stopped, err := p.MemberConfig(ctx, rc, "demom0")
	if err != nil {
		t.Fatal(err)
	}
	if a.broker("demom0").running || stopped.ConfigVersion != before.ConfigVersion || stopped.TLS.CertPEM != before.TLS.CertPEM || stopped.AgentTLS != before.AgentTLS {
		t.Fatal("renewal published before the old broker was safely stopped")
	}
	if _, err := p.Sync(ctx, rc); err != nil {
		t.Fatal(err)
	}
	for _, kind := range []string{"members", "agents"} {
		bundle, err := rc.Secrets.GetClusterSecret(ctx, rc.Cluster.Ref(), "tls/"+kind+"/demom0.pem")
		if err != nil || string(bundle) != pending[kind] {
			t.Fatalf("%s staged renewal was not atomically promoted: %v", kind, err)
		}
		if _, err := rc.Secrets.GetClusterSecret(ctx, rc.Cluster.Ref(), "tls/"+kind+"/demom0.pending.pem"); !missing(err) {
			t.Fatalf("%s pending leaf survived promotion: %v", kind, err)
		}
	}
	syncUntil(t, p, rc, func(s *internalState) bool {
		return s.Phase == "ready" && s.Pending == nil && s.Members[0].ChangeStep == ""
	})
	calls := a.log.snapshot()
	assertBefore(t, calls, "agent:demom0:drain", "agent:demom0:stop")
	assertBefore(t, calls, "agent:demom0:stop", "infra:demom0:deploy")
	if countCall(calls, "infra:demom0:deploy") != 1 {
		t.Fatalf("automatic renewal deployed more than once: %v", calls)
	}
	final, err := p.MemberConfig(ctx, rc, "demom0")
	if err != nil {
		t.Fatal(err)
	}
	if final.ConfigVersion != before.ConfigVersion+1 || final.TLS.CertPEM == before.TLS.CertPEM || final.AgentTLS.CertPEM == before.AgentTLS.CertPEM {
		t.Fatal("automatic renewal did not publish both renewed leaves exactly once")
	}
	for _, m := range state(t, rc).Members {
		if m.Name != "demom0" && m.ConfigVersion != versions[m.Name] {
			t.Fatalf("automatic leaf renewal drifted untouched peer config version: %+v", m)
		}
	}
}

func TestMemberConfigDiscoveryExcludesUncreatedPeers(t *testing.T) {
	p, rc, _, _ := setup(t)
	s := state(t, rc)
	for range 3 {
		s.add(rc.Cluster.Name)
	}
	if err := p.ensureSecrets(t.Context(), rc, s); err != nil {
		t.Fatal(err)
	}
	assertPeers := func(name string, want []string) {
		t.Helper()
		if err := s.save(rc.Cluster); err != nil {
			t.Fatal(err)
		}
		config, err := p.MemberConfig(t.Context(), rc, name)
		if err != nil {
			t.Fatal(err)
		}
		var spec rmqspec.MemberSpec
		if err := json.Unmarshal(config.EngineSpec, &spec); err != nil {
			t.Fatal(err)
		}
		if !slices.Equal(spec.Peers, want) {
			t.Fatalf("%s peers=%v want=%v", name, spec.Peers, want)
		}
	}
	s.Members[0].Stage = "imported"
	assertPeers("demom0", []string{"rabbit@demom0"})
	s.Members[0].Stage = "ready"
	s.Bootstrapped = true
	s.Members[1].Stage = "imported"
	assertPeers("demom1", []string{"rabbit@demom0", "rabbit@demom1"})
	s.Members[1].Stage = "ready"
	s.Members[2].Stage = "imported"
	full := []string{"rabbit@demom0", "rabbit@demom1", "rabbit@demom2"}
	assertPeers("demom2", full)
	assertPeers("demom0", full)
	// Retained planned peers are not fresh identities: preserve discovery even
	// before their resumed runtimes exist.
	s.Members[1].Stage = "planned"
	s.Members[1].Resume = true
	s.Members[2].Stage = "planned"
	s.Members[2].ServiceID = "retained-runtime"
	assertPeers("demom0", full)
	// A wiped/new replacement must discover survivors, never bootstrap alone.
	replacement := s.add(rc.Cluster.Name)
	if err := p.ensureSecrets(t.Context(), rc, s); err != nil {
		t.Fatal(err)
	}
	assertPeers(replacement.Name, append(slices.Clone(full), "rabbit@demom3"))
	s.Members[1].Removing = true
	assertPeers("demom0", []string{"rabbit@demom0", "rabbit@demom2"})
}

func TestCatalogErlangPackageControlsAllBrokerVersions(t *testing.T) {
	p, rc, _, _ := setup(t)
	s := state(t, rc)
	m := s.add(rc.Cluster.Name)
	original := catalog.ErlangVersion
	catalog.ErlangVersion = "1:27.3.4.18-1"
	t.Cleanup(func() { catalog.ErlangVersion = original })
	for _, v := range catalog.Versions {
		text, err := p.zeropsYAML(rc, m, v.Version)
		if err != nil {
			t.Fatal(err)
		}
		for _, pkg := range []string{"base", "asn1", "crypto", "eldap", "inets", "mnesia", "os-mon", "parsetools", "public-key", "runtime-tools", "ssl", "syntax-tools", "tools", "xmerl"} {
			if !strings.Contains(text, "erlang-"+pkg+"="+catalog.ErlangVersion) {
				t.Fatalf("%s does not use catalog Erlang package for %s", v.Version, pkg)
			}
		}
		var doc map[string]any
		if err := yaml.Unmarshal([]byte(text), &doc); err != nil {
			t.Fatal(err)
		}
		run := doc["zerops"].([]any)[0].(map[string]any)["run"].(map[string]any)
		preference := "printf '%s\\n' 'Package: erlang*' 'Pin: version " + catalog.ErlangVersion + "' 'Pin-Priority: 1001' | sudo tee /etc/apt/preferences.d/erlang"
		repositoryIndex, preferenceIndex, updateIndex, installIndex := -1, -1, -1, -1
		for i, entry := range run["prepareCommands"].([]any) {
			command := entry.(string)
			switch {
			case strings.Contains(command, "/etc/apt/sources.list.d/rabbitmq.list"):
				repositoryIndex = i
			case command == preference:
				preferenceIndex = i
			case command == "sudo apt-get update":
				updateIndex = i
			case strings.Contains(command, "rabbitmq-server="+v.Version+"-1"):
				installIndex = i
			}
		}
		if repositoryIndex < 0 || preferenceIndex <= repositoryIndex || updateIndex <= preferenceIndex || installIndex <= updateIndex {
			t.Fatalf("%s must pin all Erlang dependencies before repository refresh and broker install: repository=%d preference=%d update=%d install=%d",
				v.Version, repositoryIndex, preferenceIndex, updateIndex, installIndex)
		}
		if strings.Contains(text, original) {
			t.Fatalf("%s retains the previous Erlang pin", v.Version)
		}
	}
	if _, err := p.zeropsYAML(rc, m, "4.4.0"); !errors.Is(err, engine.ErrInvalid) {
		t.Fatalf("deployment bypassed catalog: %v", err)
	}
}

func TestRequireTLSRejectsPublicManagementCreateAndUpdate(t *testing.T) {
	p, rc, _, _ := setup(t)
	ctx := context.Background()
	for _, mode := range []string{"disabled", "preferTLS", "requireTLS"} {
		old := cloneCluster(t, rc.Cluster)
		old.Spec.TLS.Mode = mode
		next := cloneCluster(t, old)
		next.Spec.EngineOptions = json.RawMessage(`{"managementUI":{"public":true}}`)
		for _, previous := range []*domain.Cluster{nil, old} {
			err := p.Validate(ctx, previous, next)
			if mode == "requireTLS" {
				if !errors.Is(err, engine.ErrInvalid) || !strings.Contains(err.Error(), "plaintext HTTP") {
					t.Fatalf("requireTLS public management accepted: %v", err)
				}
			} else if err != nil {
				t.Fatalf("%s public management rejected: %v", mode, err)
			}
		}
	}
}
