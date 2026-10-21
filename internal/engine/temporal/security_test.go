// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package temporal

import (
	"context"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"strings"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine/temporal/tspec"
)

func TestOIDCSecretProjectionAndSafePublicTransition(t *testing.T) {
	p, rc, _, _ := fixture(t, "combined")
	converge(t, p, rc)
	o, e := optionsFor(rc.Cluster)
	if e != nil {
		t.Fatal(e)
	}
	o.UI.PublicHTTPS = true
	o.UI.OIDC = &OIDCOptions{Issuer: "https://identity.example", ClientID: "temporal"}
	rc.Cluster.Spec.EngineOptions, _ = json.Marshal(o)
	if e = p.Validate(context.Background(), nil, rc.Cluster); e != nil {
		t.Fatal(e)
	}
	if _, e = p.Sync(context.Background(), rc); e == nil {
		t.Fatal("public UI admitted without sealed client secret")
	}
	state, _ := loadState(rc.Cluster)
	if state.Members[tspec.UI].Public {
		t.Fatal("route enabled without credentials")
	}
	secret := []byte("oidc-client-secret")
	if e = rc.Secrets.SetClusterSecret(context.Background(), rc.Cluster.Ref(), UIOIDCClientSecretKey, secret); e != nil {
		t.Fatal(e)
	}
	if _, e = p.Sync(context.Background(), rc); e != nil {
		t.Fatal(e)
	}
	state, _ = loadState(rc.Cluster)
	if state.Pending == nil || state.Pending.Action != "stop-public" {
		t.Fatal("anonymous UI was not stopped before enabling public access")
	}
	converge(t, p, rc)
	state, _ = loadState(rc.Cluster)
	for _, name := range []string{tspec.Server, tspec.UI} {
		config, e := p.MemberConfig(context.Background(), rc, state.Members[name].Name)
		if e != nil {
			t.Fatal(e)
		}
		var sp tspec.Spec
		json.Unmarshal(config.EngineSpec, &sp)
		if e = sp.Validate(); e != nil {
			t.Fatal(e)
		}
		if name == tspec.UI && (sp.UI == nil || sp.UI.OIDC == nil ||
			sp.UI.OIDC.ClientSecret != string(secret) || !strings.HasPrefix(sp.UI.PublicURL, "https://")) {
			t.Fatal("UI missing authenticated public configuration")
		}
		if name == tspec.Server && strings.Contains(string(config.EngineSpec), string(secret)) {
			t.Fatal("OIDC secret reached a server role")
		}
	}
	status, e := p.Status(context.Background(), rc)
	if e != nil {
		t.Fatal(e)
	}
	raw, _ := json.Marshal(status)
	if strings.Contains(string(raw), string(secret)) || strings.Contains(string(rc.Cluster.Internal), string(secret)) ||
		strings.Contains(string(rc.Cluster.Spec.EngineOptions), string(secret)) {
		t.Fatal("OIDC secret escaped sealed member configuration")
	}
	found := false
	for _, endpoint := range status.Connection.Endpoints {
		if endpoint.Name == "ui" && endpoint.Public {
			found = true
		}
	}
	if !found {
		t.Fatal("public UI endpoint not advertised")
	}
}

func TestCertificatesAndRenewalRepublish(t *testing.T) {
	p, rc, _, _ := fixture(t, "combined")
	converge(t, p, rc)
	state, _ := loadState(rc.Cluster)
	m := state.Members[tspec.Server]
	config, e := p.MemberConfig(context.Background(), rc, m.Name)
	if e != nil {
		t.Fatal(e)
	}
	var sp tspec.Spec
	json.Unmarshal(config.EngineSpec, &sp)
	for _, item := range []struct{ pem, name string }{
		{sp.TLS.InternodeCertPEM, InternodeServerName},
		{sp.TLS.FrontendCertPEM, FrontendServerName},
		{sp.TLS.FrontendCertPEM, m.Name},
		{config.AgentTLS.CertPEM, m.Name},
	} {
		block, _ := pem.Decode([]byte(item.pem))
		if block == nil {
			t.Fatal("invalid certificate PEM")
		}
		cert, e := x509.ParseCertificate(block.Bytes)
		if e != nil {
			t.Fatal(e)
		}
		if e = cert.VerifyHostname(item.name); e != nil {
			t.Fatal(e)
		}
	}
	// Advance to the renewal window using the same injected clock as upgrades.
	block, _ := pem.Decode([]byte(config.AgentTLS.CertPEM))
	leaf, _ := x509.ParseCertificate(block.Bytes)
	rc.Clock.(*testClock).now = leaf.NotAfter.Add(-29 * 24 * time.Hour)
	oldVersion := m.ConfigVersion
	oldToken := m.ConfigHash
	if _, e = p.Sync(context.Background(), rc); e != nil {
		t.Fatal(e)
	}
	state, _ = loadState(rc.Cluster)
	m = state.Members[tspec.Server]
	if m.ConfigVersion <= oldVersion || m.ConfigHash == oldToken {
		t.Fatal("renewal did not republish secret-bearing config")
	}
}

func TestOptionsRejectNonObjects(t *testing.T) {
	for _, raw := range []string{"null", "[]", "true", "{} {}", `{"ui":{"oidc":{"clientSecret":null}}}`} {
		if _, e := ParseOptions(json.RawMessage(raw)); e == nil {
			t.Fatalf("accepted %s", raw)
		}
	}
}

func TestTopologyComponentConsistency(t *testing.T) {
	p, rc, _, _ := fixture(t, "combined")
	c := rc.Cluster.Spec.Components.Extra[tspec.History]
	c.Replicas = 1
	rc.Cluster.Spec.Components.Extra[tspec.History] = c
	if e := p.Validate(context.Background(), nil, rc.Cluster); e == nil {
		t.Fatal("combined role admitted")
	}
	p, rc, _, _ = fixture(t, "split")
	c = rc.Cluster.Spec.Components.Extra[tspec.Frontend]
	c.Replicas = 0
	rc.Cluster.Spec.Components.Extra[tspec.Frontend] = c
	if e := p.Validate(context.Background(), nil, rc.Cluster); e == nil {
		t.Fatal("split without frontend admitted")
	}
	rc.Cluster.Spec.Components.Engine = domain.ComponentSpec{Replicas: 1}
	if e := p.Validate(context.Background(), nil, rc.Cluster); e == nil {
		t.Fatal("engine slot admitted")
	}
}
