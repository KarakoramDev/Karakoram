// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package server

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"github.com/KarakoramDev/Karakoram/internal/api"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine/psmdb/pki"
	"strings"
	"testing"
	"time"
)

func TestConfigFromZeropsEnvironment(t *testing.T) {
	for _, key := range []string{"KARAKORAM_DATA_DIR", "KARAKORAM_LISTEN", "KARAKORAM_INTERNAL_LISTEN", "KARAKORAM_PUBLIC_URL", "KARAKORAM_ADVERTISE_URL", "KARAKORAM_ZEROPS_PROJECT_ID", "KARAKORAM_ZEROPS_API_URL"} {
		t.Setenv(key, "")
	}
	t.Setenv("hostname", "karakoram")
	t.Setenv("projectId", "project-test")
	t.Setenv("zeropsSubdomain", "https://karakoram.example.test")
	t.Setenv("KARAKORAM_MASTER_KEY", strings.Repeat("m", 32))
	t.Setenv("KARAKORAM_ZEROPS_TOKEN", "test-token")
	c, err := ConfigFromEnv()
	if err != nil {
		t.Fatal(err)
	}
	if c.PublicURL != "https://karakoram.example.test" || c.AdvertiseURL != "https://karakoram:9443" || c.ZeropsProjectID != "project-test" || c.DatabasePath() != "/mnt/karakoramdata/karakoram.db" {
		t.Fatal(c)
	}
	t.Setenv("KARAKORAM_PUBLIC_URL", "https://name:password@example.test")
	if _, err = ConfigFromEnv(); err == nil {
		t.Fatal("credential-bearing public URL accepted")
	}
	t.Setenv("KARAKORAM_PUBLIC_URL", "https://example.test")
	t.Setenv("KARAKORAM_ADVERTISE_URL", "http://karakoram:9443")
	if _, err = ConfigFromEnv(); err == nil {
		t.Fatal("plaintext internal URL accepted")
	}
}
func TestInternalPKIPersistsSealedWithExpectedSANs(t *testing.T) {
	s, sealer, _ := testServer(t)
	ctx := context.Background()
	first, ca, err := internalTLS(ctx, s.Store, sealer, "cpnode")
	if err != nil {
		t.Fatal(err)
	}
	second, again, err := internalTLS(ctx, s.Store, sealer, "cpnode")
	if err != nil {
		t.Fatal(err)
	}
	firstLeaf, err := first.GetCertificate(&tls.ClientHelloInfo{})
	if err != nil {
		t.Fatal(err)
	}
	secondLeaf, err := second.GetCertificate(&tls.ClientHelloInfo{})
	if err != nil {
		t.Fatal(err)
	}
	if ca != again || string(firstLeaf.Certificate[0]) != string(secondLeaf.Certificate[0]) {
		t.Fatal("internal identity changed across restart")
	}
	cert, err := x509.ParseCertificate(firstLeaf.Certificate[0])
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"cpnode", "karakoram", "localhost", "127.0.0.1", "::1"} {
		if err = cert.VerifyHostname(name); err != nil {
			t.Fatal(name, err)
		}
	}
	raw, err := s.Store.GetSetting(ctx, "server/internal-pki")
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(raw), "PRIVATE KEY") || !strings.Contains(string(raw), "kk:v1:") {
		t.Fatal("PKI not sealed")
	}
	block, _ := pem.Decode([]byte(ca))
	if block == nil {
		t.Fatal("invalid CA")
	}
}
func TestAgentDialerCacheInvalidatesRotatedCredentials(t *testing.T) {
	s, _, _ := testServer(t)
	ctx := context.Background()
	ca, err := pki.NewCA("test", 24*time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if err = s.Store.SetClusterSecret(ctx, "prod", "tls/ca.crt", ca.CertPEM); err != nil {
		t.Fatal(err)
	}
	if err = s.Store.SetAgentToken(ctx, &domain.AgentToken{Cluster: "prod", Member: "prodm0", Token: "first-token"}); err != nil {
		t.Fatal(err)
	}
	d := NewAgentDialer(s.Store)
	defer d.Close()
	first, err := d.Dial(ctx, "prod", "prodm0")
	if err != nil {
		t.Fatal(err)
	}
	second, err := d.Dial(ctx, "prod", "prodm0")
	if err != nil || first != second {
		t.Fatal("cache miss", err)
	}
	if err = s.Store.SetAgentToken(ctx, &domain.AgentToken{Cluster: "prod", Member: "prodm0", Token: "second-token"}); err != nil {
		t.Fatal(err)
	}
	third, err := d.Dial(ctx, "prod", "prodm0")
	if err != nil || first == third {
		t.Fatal("rotated token retained", err)
	}
	if _, err = d.Dial(ctx, "prod", "other.example/path"); err == nil {
		t.Fatal("unsafe hostname accepted")
	}
}
func TestProviderTopologyDefaultsAndObservedMemberMapping(t *testing.T) {
	p := providerAPI(&fakeProvider{})
	if len(p.Topologies) != 2 || p.Topologies[1].Defaults.Components.ConfigServer == nil || p.Topologies[1].Defaults.Components.ConfigServer.Replicas != 1 {
		t.Fatal(p)
	}
	bounds := domain.Resources{CPUMode: "DEDICATED", MinCPU: 2, MaxCPU: 2, MinRAM: 1, MaxRAM: 2, MinDisk: 1, MaxDisk: 3}
	m := (&Server{}).memberAPI("prod", domain.MemberStatus{Name: "prodm0", PublicHost: "public.example:10000", Resources: &domain.ObservedResources{CPUCores: 1, MemoryGB: 0.5, DiskGB: 5}, Autoscaling: &bounds})
	if m.PublicHost == nil || m.Resources == nil || m.Resources.MemoryGB != 0.5 {
		t.Fatal(m)
	}
	if m.Autoscaling == nil || m.Autoscaling.CpuMode != api.ResourcesCpuMode("DEDICATED") || m.Autoscaling.MinCpu != 2 || m.Autoscaling.MaxRam != 2 || m.Autoscaling.MinDisk != 1 {
		t.Fatal("platform autoscaling was omitted or confused with allocation", m)
	}
}

func TestResourceDecimalPreservesAPIPrecision(t *testing.T) {
	for _, v := range []float64{0.1, 0.063, 0.125, 1.234, 0.0635} {
		if got := resourceDecimal(float32(v)); got != v {
			t.Fatalf("resource decimal %v widened to %v", v, got)
		}
	}
}

func TestTLSInputsStayWriteOnlyAndRejectCAReplacement(t *testing.T) {
	s, _, token := testServer(t)
	ca, err := pki.NewCA("user-provided", 24*time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	certificate, key := string(ca.CertPEM), string(ca.KeyPEM)
	spec := api.ClusterSpec{Provider: "fake", Version: "8.0", Topology: api.Topology{Type: "replicaSet"}, Tls: &api.TLS{Ca: "userProvided", Mode: "requireTLS", CaCertificate: &certificate, CaPrivateKey: &key}}
	body, err := json.Marshal(api.CreateClusterRequest{Name: "tlsdemo", Spec: spec})
	if err != nil {
		t.Fatal(err)
	}
	w := call(s, "POST", "/api/v1/clusters", string(body), token)
	if w.Code != 201 || strings.Contains(w.Body.String(), "PRIVATE KEY") || strings.Contains(w.Body.String(), "caPrivateKey") {
		t.Fatal(w.Code, w.Body.String())
	}
	stored, err := s.Store.GetClusterSecret(context.Background(), "tlsdemo", "tls/ca.key")
	if err != nil || string(stored) != key {
		t.Fatal("CA key not persisted", err)
	}
	replacement, err := pki.NewCA("replacement", 24*time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	certificate, key = string(replacement.CertPEM), string(replacement.KeyPEM)
	spec.Tls.CaCertificate = &certificate
	spec.Tls.CaPrivateKey = &key
	body, err = json.Marshal(api.ReplaceClusterRequest{Spec: spec})
	if err != nil {
		t.Fatal(err)
	}
	w = call(s, "PUT", "/api/v1/clusters/tlsdemo", string(body), token)
	if w.Code != 409 {
		t.Fatal(w.Code, w.Body.String())
	}
	cluster, err := s.Store.GetCluster(context.Background(), "tlsdemo")
	if err != nil || cluster.Generation != 1 {
		t.Fatal(cluster, err)
	}
	stored, err = s.Store.GetClusterSecret(t.Context(), "tlsdemo", "tls/ca.key")
	if err != nil || string(stored) != string(ca.KeyPEM) {
		t.Fatal("rejected replacement changed active CA", err)
	}
	patch, err := json.Marshal(map[string]any{"spec": map[string]any{"tls": spec.Tls}})
	if err != nil {
		t.Fatal(err)
	}
	if w = call(s, "PATCH", "/api/v1/clusters/tlsdemo", string(patch), token); w.Code != 409 {
		t.Fatal(w.Code, w.Body.String())
	}
	certificate, key = string(ca.CertPEM), string(ca.KeyPEM)
	body, err = json.Marshal(api.ReplaceClusterRequest{Spec: spec})
	if err != nil {
		t.Fatal(err)
	}
	if w = call(s, "PUT", "/api/v1/clusters/tlsdemo", string(body), token); w.Code != 200 {
		t.Fatal("identical CA rejected", w.Code, w.Body.String())
	}
}
