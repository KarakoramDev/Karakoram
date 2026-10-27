// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package openbao

import (
	"context"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"errors"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/nativeartifacts"
	"github.com/KarakoramDev/Karakoram/internal/engine/openbao/baospec"
	"github.com/KarakoramDev/Karakoram/internal/infra"
	"strings"
	"testing"
)

type edgeSecrets struct {
	engine.SecretStore
	values map[string][]byte
	tokens map[string]*domain.AgentToken
}

func (s *edgeSecrets) GetClusterSecret(_ context.Context, _ domain.ClusterRef, key string) ([]byte, error) {
	v, ok := s.values[key]
	if !ok {
		return nil, engine.ErrNotFound
	}
	return v, nil
}
func (s *edgeSecrets) SetClusterSecret(_ context.Context, _ domain.ClusterRef, key string, v []byte) error {
	s.values[key] = append([]byte(nil), v...)
	return nil
}
func (s *edgeSecrets) GetAgentToken(_ context.Context, _ domain.ClusterRef, name string) (*domain.AgentToken, error) {
	v := s.tokens[name]
	if v == nil {
		return nil, engine.ErrNotFound
	}
	return v, nil
}
func (s *edgeSecrets) SetAgentToken(_ context.Context, v *domain.AgentToken) error {
	s.tokens[v.Member] = v
	return nil
}

type edgeBackend struct {
	infra.Backend
	family string
}

func (b edgeBackend) Layout() infra.Layout {
	return infra.Layout{User: "runtime", AppDir: "/app", VolumeRoot: "/data", PrivateAddressFamily: b.family}
}
func (edgeBackend) ProjectInfo(context.Context) (infra.Project, error) {
	return infra.Project{PublicIPv4: "192.0.2.10", PublicIPv6: "2001:db8::10"}, nil
}
func edgeFixture(t *testing.T) (*Provider, *engine.Context, *internalState) {
	t.Helper()
	p := New(ProviderOptions{})
	c := &domain.Cluster{Name: "bao"}
	p.ApplyDefaults(&c.Spec)
	c.Spec.Version = "2.7.1"
	s := &internalState{UID: "uid", ConfigVersion: 1, Members: []*member{{Name: "baom0", Volume: "baom0v", VolumeID: "baom0v", Stage: "ready", ProcessGeneration: 7}, {Name: "baom1", Volume: "baom1v", VolumeID: "baom1v", Stage: "planned"}}}
	rc := &engine.Context{Cluster: c, Backend: edgeBackend{}, Secrets: &edgeSecrets{values: map[string][]byte{}, tokens: map[string]*domain.AgentToken{}}}
	if err := p.ensureSecrets(t.Context(), rc, s); err != nil {
		t.Fatal(err)
	}
	if err := s.save(c); err != nil {
		t.Fatal(err)
	}
	return p, rc, s
}
func TestEdgeMemberConfig(t *testing.T) {
	for _, tc := range []struct {
		name   string
		family string
		bind   string
		held   bool
	}{{"default", "", "::", false}, {"ipv4", infra.AddressFamilyIPv4, "0.0.0.0", false}, {"held", "", "::", true}} {
		t.Run(tc.name, func(t *testing.T) {
			p, rc, s := edgeFixture(t)
			rc.Backend = edgeBackend{family: tc.family}
			s.Members[0].Held = tc.held
			if err := s.save(rc.Cluster); err != nil {
				t.Fatal(err)
			}
			material := []byte(`{"shares":["NEVER-SHARE-BYTES"],"rootToken":"NEVER-ROOT-TOKEN","threshold":1,"sealType":"shamir"}`)
			if err := putSecret(t.Context(), rc, SecretUnseal, material); err != nil {
				t.Fatal(err)
			}
			if err := putSecret(t.Context(), rc, SecretManagementToken, []byte("NEVER-MANAGEMENT-TOKEN")); err != nil {
				t.Fatal(err)
			}
			v, err := p.MemberConfig(t.Context(), rc, "baom0")
			if err != nil {
				t.Fatal(err)
			}
			if v.Volume == nil || !v.Volume.Initialized {
				t.Fatalf("ready Raft member must retain local-data loss fencing: %+v", v.Volume)
			}
			var spec baospec.MemberSpec
			if err = json.Unmarshal(v.EngineSpec, &spec); err != nil {
				t.Fatal(err)
			}
			if spec.NodeID != "baom0" || spec.ListenAddress != tc.bind || spec.ProcessGeneration != 7 || spec.BinaryPath != "/opt/karakoram/openbao-2.7.1/bao" || spec.APIAddr != "https://baom0:8200" || spec.ClusterAddr != "https://baom0:8201" || spec.Storage.Type != "raft" || spec.Seal.Type != "shamir" || len(spec.Storage.RetryJoin) != 1 || spec.Storage.RetryJoin[0].LeaderAPIAddr != "https://baom1:8200" {
				t.Fatalf("spec: %+v", spec)
			}
			if (len(v.DesiredProcesses) == 0) != tc.held {
				t.Fatal(v.DesiredProcesses)
			}
			b, _ := json.Marshal(v)
			for _, secret := range []string{"NEVER-SHARE-BYTES", "NEVER-ROOT-TOKEN", "NEVER-MANAGEMENT-TOKEN"} {
				if strings.Contains(string(b), secret) {
					t.Fatal("member configuration leaked initialization material")
				}
			}
		})
	}
}
func TestEdgeCertificatesAndIdempotence(t *testing.T) {
	p, rc, s := edgeFixture(t)
	rc.Cluster.Spec.Exposure.Public = true
	if err := p.ensureSecrets(t.Context(), rc, s); err != nil {
		t.Fatal(err)
	}
	for _, kind := range []string{"members", "agents"} {
		key := "tls/" + kind + "/baom0.pem"
		bundle, _ := secret(t.Context(), rc, key)
		block, _ := pem.Decode(bundle)
		cert, err := x509.ParseCertificate(block.Bytes)
		if err != nil {
			t.Fatal(err)
		}
		for _, name := range []string{"baom0", "localhost", "127.0.0.1", "::1", "192.0.2.10", "2001:db8::10"} {
			if err = cert.VerifyHostname(name); err != nil {
				t.Fatal(err)
			}
		}
		if len(cert.ExtKeyUsage) != 2 {
			t.Fatal(cert.ExtKeyUsage)
		}
		if err = p.ensureSecrets(t.Context(), rc, s); err != nil {
			t.Fatal(err)
		}
		again, _ := secret(t.Context(), rc, key)
		if string(bundle) != string(again) {
			t.Fatal("non-idempotent certificate issuance")
		}
	}
}
func TestEdgeRuntimeVerifiedArtifact(t *testing.T) {
	p, rc, s := edgeFixture(t)
	v, err := p.runtimeSpec(rc, s.Members[0], "2.7.1")
	if err != nil {
		t.Fatal(err)
	}
	a, err := nativeartifacts.Get("openbao")
	if err != nil {
		t.Fatal(err)
	}
	if len(v.Prepare) < 3 {
		t.Fatalf("expected dependencies, OpenBao, and plugin preparation: %v", v.Prepare)
	}
	if v.Prepare[0] != "sudo apt-get update && sudo apt-get install -y ca-certificates curl python3" {
		t.Fatalf("archive validation dependencies must be installed first: %s", v.Prepare[0])
	}
	for _, command := range v.Prepare[1:] {
		if !strings.Contains(command, "python3 -c") {
			t.Fatal("expected verified archive preparation after dependency installation")
		}
	}
	prepare := strings.Join(v.Prepare, "\n")
	for _, want := range []string{a.SHA256, "sha256sum -c -", "/opt/karakoram/openbao-2.7.1"} {
		if !strings.Contains(prepare, want) {
			t.Fatalf("prepare missing %s", want)
		}
	}
	if v.Env["KARAKORAM_ENGINE"] != "openbao" || v.Volume != "baom0v" {
		t.Fatal(v)
	}
	env, _ := json.Marshal(v.Env)
	for _, bad := range []string{"rootToken", "unseal", "management-token", "BEGIN PRIVATE KEY"} {
		if strings.Contains(string(env), bad) {
			t.Fatalf("runtime environment contains %s", bad)
		}
	}
}
func TestEdgeConnectionAndReveal(t *testing.T) {
	p, rc, _ := edgeFixture(t)
	v, err := p.Connection(t.Context(), rc)
	if err != nil {
		t.Fatal(err)
	}
	if len(v.Credentials) != 0 || !strings.HasPrefix(v.Connection.URI, "https://") || v.CACertificate == "" {
		t.Fatal(v)
	}
	if _, err = p.RevealUnsealKeys(t.Context(), rc); !errors.Is(err, engine.ErrUnavailable) {
		t.Fatal(err)
	}
	if err = putSecret(t.Context(), rc, SecretUnseal, []byte(`{"shares":["share"],"threshold":1,"sealType":"shamir","rootToken":"root"}`)); err != nil {
		t.Fatal(err)
	}
	keys, err := p.RevealUnsealKeys(t.Context(), rc)
	if err != nil || keys.InitialRootToken != "root" || len(keys.Shares) != 1 {
		t.Fatal(keys, err)
	}
	connections, err := p.ListDatabaseConnections(t.Context(), rc)
	if err != nil || len(connections) != 0 {
		t.Fatal(connections, err)
	}
	targets, err := p.ConnectionTargets(t.Context(), rc)
	if err != nil || targets != nil {
		t.Fatal(targets, err)
	}
}
func TestEdgeMetricsCatalog(t *testing.T) {
	metrics := (&Provider{}).Metrics()
	if len(metrics) != len(baospec.MetricNames) {
		t.Fatal(len(metrics))
	}
	for i, m := range metrics {
		if m.Exposition != baospec.MetricNames[i] || m.Scale != 1 || m.Name == "" || m.Unit == "" {
			t.Fatal(m)
		}
		if strings.HasSuffix(m.Exposition, "_total") && m.Unit != "requests/second" {
			t.Fatal(m)
		}
	}
}
