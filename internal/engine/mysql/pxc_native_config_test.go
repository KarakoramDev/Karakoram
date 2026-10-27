// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package mysql

import (
	"bytes"
	"encoding/json"
	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/mysql/myspec"
	"github.com/KarakoramDev/Karakoram/internal/infra"
	"os"
	"path/filepath"
	"testing"
)

// TestPXCExportNativeMemberConfigs bridges CP and opt-in native agent acceptance
// without importing test-only provisioning code into the production binary.
// Only infrastructure/transport is simulated: Core.Sync, the persistent store,
// sealed secrets, PKI issuance and MemberConfig are the production paths.
// The native consumer may remap addresses/ports/paths but must retain these
// actual certificates, credentials and Galera policy settings.
func TestPXCExportNativeMemberConfigs(t *testing.T) {
	directory := os.Getenv("KK_PXC_NATIVE_CONFIG_DIR")
	if directory == "" {
		t.Skip("set KK_PXC_NATIVE_CONFIG_DIR for the native CP-to-agent acceptance bridge")
	}
	p, rc, b := setupPXC(t)
	if version := os.Getenv("KARAKORAM_PXC_VERSION"); version != "" {
		rc.Cluster.Spec.Version = version
	}
	// Publicly supported parameter inputs bound native memory without altering
	// the generated security policy or lowering production resource validation.
	rc.Cluster.Spec.Components.Engine.Resources.MinRAM = 1
	rc.Cluster.Spec.Components.Engine.Resources.MaxRAM = 1
	rc.Cluster.Spec.EngineOptions = json.RawMessage(`{"parameters":{"innodb_buffer_pool_size":"134217728","max_connections":"30"},"binlog":{"archiveInterval":3600,"expireSeconds":604800}}`)
	if err := p.Validate(t.Context(), nil, rc.Cluster); err != nil {
		t.Fatal(err)
	}
	initial := map[string]agentapi.MemberConfig{}
	for range 120 {
		if _, err := p.Sync(t.Context(), rc); err != nil {
			t.Fatal(err)
		}
		current, err := load(rc.Cluster)
		if err != nil {
			t.Fatal(err)
		}
		if current.Formation.State == "bootstrapping" {
			for _, m := range current.data() {
				cfg, err := p.MemberConfig(t.Context(), rc, m.Name)
				if err != nil {
					t.Fatal(err)
				}
				initial[m.Name] = cfg
			}
			break
		}
		b.apply(t, p, rc)
	}
	if len(initial) != 3 {
		t.Fatal("did not capture real initial formation configuration")
	}
	converge(t, p, rc, b)
	s, err := load(rc.Cluster)
	if err != nil {
		t.Fatal(err)
	}
	configs := make([]agentapi.MemberConfig, 0, len(s.Members))
	initialConfigs := make([]agentapi.MemberConfig, 0, len(s.Members))
	shared := ""
	leaves := map[string]bool{}
	for _, m := range s.Members {
		config, err := p.MemberConfig(t.Context(), rc, m.Name)
		if err != nil {
			t.Fatal(err)
		}
		var spec myspec.Spec
		if err := json.Unmarshal(config.EngineSpec, &spec); err != nil {
			t.Fatal(err)
		}
		if err := spec.Validate(); err != nil {
			t.Fatal("production native spec", m.Name, err)
		}
		if m.Role == "data" {
			if spec.Galera.TrafficCert == nil || spec.Galera.TrafficCert.CertPEM == "" {
				t.Fatal("missing shared traffic certificate")
			}
			if shared != "" && shared != spec.Galera.TrafficCert.CertPEM {
				t.Fatal("traffic certificates differ")
			}
			shared = spec.Galera.TrafficCert.CertPEM
			if spec.ServerCert.CertPEM == shared || leaves[spec.ServerCert.CertPEM] {
				t.Fatal("SQL leaves are not member-specific")
			}
			leaves[spec.ServerCert.CertPEM] = true
		}
		configs = append(configs, config)
		first := config
		if m.Role == "data" {
			first = initial[m.Name]
		}
		initialConfigs = append(initialConfigs, first)
	}
	collect := func() []agentapi.MemberConfig {
		current, err := load(rc.Cluster)
		if err != nil {
			t.Fatal(err)
		}
		values := make([]agentapi.MemberConfig, 0, len(current.Members))
		// Keep data first even though monotonic additions follow the proxy in state.
		for _, role := range []string{"data", "proxy"} {
			for _, m := range current.Members {
				if m.Role != role {
					continue
				}
				cfg, err := p.MemberConfig(t.Context(), rc, m.Name)
				if err != nil {
					t.Fatal(err)
				}
				values = append(values, cfg)
			}
		}
		return values
	}
	if err := requestGaleraTrafficRotation(t.Context(), rc, s); err != nil {
		t.Fatal(err)
	}
	if err := save(rc.Cluster, s); err != nil {
		t.Fatal(err)
	}
	day2Ticks(t, rc, b, func(current *state) bool { return current.GaleraTLS == nil })
	rotated := collect()
	rc.Cluster.Generation++
	rc.Cluster.Spec.Components.Engine.Replicas = 5
	addedInitial := capturePXCAddedInitializers(t, p, rc, b)
	converge(t, p, rc, b)
	added := collect()
	for i, cfg := range added {
		if first, ok := addedInitial[cfg.Member]; ok {
			added[i] = first
		}
	}
	if err := os.MkdirAll(directory, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(directory, 0700); err != nil {
		t.Fatal(err)
	}
	for filename, values := range map[string][]agentapi.MemberConfig{"members.json": initialConfigs, "formed-members.json": configs, "rotated-members.json": rotated, "added-members.json": added} {
		raw, err := json.MarshalIndent(values, "", "  ")
		if err != nil {
			t.Fatal(err)
		}
		filename = filepath.Join(directory, filename)
		if err := os.WriteFile(filename, raw, 0600); err != nil {
			t.Fatal(err)
		}
		if err := os.Chmod(filename, 0600); err != nil {
			t.Fatal(err)
		}
	}
}

func capturePXCAddedInitializers(t *testing.T, p *Core, rc *engine.Context, b *fakeBackend) map[string]agentapi.MemberConfig {
	t.Helper()
	for range 120 {
		if _, err := p.Sync(t.Context(), rc); err != nil {
			t.Fatal(err)
		}
		current, err := load(rc.Cluster)
		if err != nil {
			t.Fatal(err)
		}
		ready := len(current.data()) == 5
		for _, m := range current.data() {
			if m.Index < 3 {
				continue
			}
			// Allocation is journaled before ensureSecrets. Every new identity is
			// imported before runtime deployment, so this is a real pre-initialize
			// checkpoint whose sealed credentials and certificates already exist.
			ready = ready && m.Stage == "imported" && !m.Initialized
		}
		if ready {
			initial := map[string]agentapi.MemberConfig{}
			for _, m := range current.data() {
				if m.Index < 3 {
					continue
				}
				cfg, err := p.MemberConfig(t.Context(), rc, m.Name)
				if err != nil {
					t.Fatal(err)
				}
				initial[m.Name] = cfg
			}
			if len(initial) != 2 {
				t.Fatal("did not capture two scale-out initializers")
			}
			return initial
		}
		b.apply(t, p, rc)
	}
	t.Fatal("did not reach credential-complete scale-out import checkpoint")
	return nil
}

func TestPXCAddedInitializerCaptureWaitsForSealedIdentity(t *testing.T) {
	p, rc, b := setupPXC(t)
	converge(t, p, rc, b)
	rc.Cluster.Generation++
	rc.Cluster.Spec.Components.Engine.Replicas = 5
	if _, err := p.Sync(t.Context(), rc); err != nil {
		t.Fatal(err)
	}
	current, err := load(rc.Cluster)
	if err != nil {
		t.Fatal(err)
	}
	if current.find("demom3") == nil || current.find("demom3").Stage != "planned" {
		t.Fatal("scale-out allocation checkpoint missing")
	}
	if _, err := secret(t.Context(), rc, "tls/servers/demom3.crt"); !missing(err) {
		t.Fatal("expected allocation before certificate issuance", err)
	}
	initial := capturePXCAddedInitializers(t, p, rc, b)
	for _, cfg := range initial {
		var spec myspec.Spec
		if err := json.Unmarshal(cfg.EngineSpec, &spec); err != nil {
			t.Fatal(err)
		}
		if !spec.AllowInitialize || spec.Galera == nil || spec.ServerCert == nil || spec.ServerCert.CertPEM == "" {
			t.Fatal("captured invalid pre-initialization member", cfg.Member)
		}
		if err := spec.Validate(); err != nil {
			t.Fatal(err)
		}
	}
}

type pxcBindBackend struct {
	infra.Backend
	family string
}

func (b pxcBindBackend) Layout() infra.Layout {
	layout := b.Backend.Layout()
	layout.PrivateAddressFamily = b.family
	return layout
}

func TestPXCGaleraBindFollowsBackendFamily(t *testing.T) {
	for _, tc := range []struct{ family, listen string }{
		{infra.AddressFamilyIPv4, "0.0.0.0"},
		{infra.AddressFamilyIPv6, "::"},
	} {
		t.Run(tc.family, func(t *testing.T) {
			p, rc, b := setupPXC(t)
			rc.Backend = pxcBindBackend{Backend: b, family: tc.family}
			rc.AgentConfigCapabilities = []string{agentapi.ConfigCapabilityGaleraListenHost}
			if _, err := p.Sync(t.Context(), rc); err != nil {
				t.Fatal(err)
			}
			s, err := load(rc.Cluster)
			if err != nil {
				t.Fatal(err)
			}
			cfg, err := p.MemberConfig(t.Context(), rc, s.data()[0].Name)
			if err != nil {
				t.Fatal(err)
			}
			var spec myspec.Spec
			if err := json.Unmarshal(cfg.EngineSpec, &spec); err != nil {
				t.Fatal(err)
			}
			if spec.Galera.ListenHost != tc.listen {
				t.Fatalf("family %s bind=%s, want %s", tc.family, spec.Galera.ListenHost, tc.listen)
			}
			if err := spec.Validate(); err != nil {
				t.Fatal(err)
			}
			for _, peer := range spec.Galera.Peers {
				if peer.Host != peer.Name {
					t.Fatalf("backend bind policy replaced advertised identity: %+v", peer)
				}
			}
			// Snapshot the previous strict nested schema, not today's Galera
			// struct: embedding today's type would hide a compatibility break.
			type previousGalera struct {
				ClusterName    string              `json:"clusterName"`
				FormationEpoch uint64              `json:"formationEpoch"`
				Peers          []myspec.Peer       `json:"peers"`
				GaleraPort     int                 `json:"galeraPort"`
				ISTPort        int                 `json:"istPort"`
				SSTPort        int                 `json:"sstPort"`
				Bootstrap      bool                `json:"bootstrap"`
				StartPosition  string              `json:"startPosition,omitempty"`
				GCacheSize     string              `json:"gcacheSize"`
				TrafficCert    *myspec.Certificate `json:"trafficCert"`
				TrafficCAPEM   string              `json:"trafficCAPEM"`
			}
			decodePrevious := func(raw []byte) error {
				var previous struct {
					myspec.Spec
					Galera *previousGalera `json:"galera,omitempty"`
				}
				d := json.NewDecoder(bytes.NewReader(raw))
				d.DisallowUnknownFields()
				return d.Decode(&previous)
			}
			if err := decodePrevious(cfg.EngineSpec); err == nil {
				t.Fatal("previous schema fixture accidentally accepts the new field")
			}
			withoutPolicy := bytes.Replace(cfg.EngineSpec, []byte(`"listenHost":"`+tc.listen+`",`), nil, 1)
			for _, capabilities := range [][]string{nil, {"unknown-capability"}, nil} {
				rc.AgentConfigCapabilities = capabilities
				legacy, err := p.MemberConfig(t.Context(), rc, s.data()[0].Name)
				if err != nil {
					t.Fatal(err)
				}
				if err := decodePrevious(legacy.EngineSpec); err != nil {
					t.Fatalf("legacy strict decoder rejected new CP output: %v", err)
				}
				if !bytes.Equal(legacy.EngineSpec, withoutPolicy) {
					t.Fatal("legacy request changed fields beyond omitting the unsupported bind policy")
				}
			}
		})
	}
}
