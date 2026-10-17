// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package supabase

import (
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/nativeartifacts"
	"github.com/KarakoramDev/Karakoram/internal/engine/postgres"
	"github.com/KarakoramDev/Karakoram/internal/pki"
)

func TestGatewayCORSOriginsOptions(t *testing.T) {
	for _, origins := range [][]string{nil, {}, {"*"}, {"https://app.example.com", "http://localhost:3000"}} {
		raw, _ := json.Marshal(map[string]any{"gateway": map[string]any{"corsOrigins": origins}})
		o, err := ParseOptions(raw)
		if err != nil || !reflect.DeepEqual(o.Gateway.CORSOrigins, origins) {
			t.Fatalf("valid origins rejected: %v", err)
		}
	}
	for _, origins := range [][]string{{"null"}, {"*", "https://app.example.com"}, {"https://app.example.com/"}, {"https://app.example.com/path"}, {"https://user:pass@example.com"}, {"https://app.example.com?x=1"}, {"https://app.example.com#fragment"}, {"ftp://example.com"}} {
		raw, _ := json.Marshal(map[string]any{"gateway": map[string]any{"corsOrigins": origins}})
		if _, err := ParseOptions(raw); err == nil || !strings.Contains(err.Error(), "gateway.corsOrigins") {
			t.Fatalf("unsafe origins admitted: %v", origins)
		}
	}
	p, rc, _ := setupProvider(t)
	rc.Cluster.Spec.EngineOptions = json.RawMessage(`{"gateway":{"corsOrigins":["https://app.example.com"]}}`)
	convergeProvider(t, p, rc)
	config, err := p.MemberConfig(t.Context(), rc, "demogw")
	if err != nil {
		t.Fatal(err)
	}
	var sp struct {
		CORSOrigins []string `json:"corsOrigins"`
	}
	if err := json.Unmarshal(config.EngineSpec, &sp); err != nil || !reflect.DeepEqual(sp.CORSOrigins, []string{"https://app.example.com"}) {
		t.Fatal("CORS origins not delivered to gateway", err)
	}
}

func TestGraphQLExposesEffectivePostgRESTSchema(t *testing.T) {
	for _, tc := range []struct {
		raw     string
		schemas []string
	}{
		{`{}`, []string{"public", "graphql_public"}},
		{`{"extensions":["pg_graphql"]}`, []string{"public", "graphql_public"}},
		{`{"extensions":["pg_graphql"],"rest":{"schemas":["custom"]}}`, []string{"custom", "graphql_public"}},
		{`{"extensions":["pg_graphql"],"rest":{"schemas":["graphql_public","public"]}}`, []string{"graphql_public", "public"}},
		{`{"rest":{"schemas":["custom"]}}`, []string{"custom"}},
	} {
		o, err := ParseOptions(json.RawMessage(tc.raw))
		if err != nil || !reflect.DeepEqual(o.REST.Schemas, tc.schemas) {
			t.Fatalf("%s: schemas %v, error %v", tc.raw, o.REST.Schemas, err)
		}
	}
	p, rc, _ := setupProvider(t)
	rc.Cluster.Spec.EngineOptions = json.RawMessage(`{"extensions":["pg_graphql"],"rest":{"schemas":["public"]}}`)
	convergeProvider(t, p, rc)
	for _, member := range []string{"demogw", "demors"} {
		config, err := p.MemberConfig(t.Context(), rc, member)
		if err != nil {
			t.Fatal(err)
		}
		var sp struct {
			GraphQL bool `json:"graphql"`
			REST    struct {
				Schemas []string `json:"schemas"`
			} `json:"rest"`
		}
		if err := json.Unmarshal(config.EngineSpec, &sp); err != nil || !sp.GraphQL || !reflect.DeepEqual(sp.REST.Schemas, []string{"public", "graphql_public"}) {
			t.Fatal("GraphQL profile/schema not delivered", member, err)
		}
	}
}

func TestComponentCertificateRenewalRepublishesUnchangedConfig(t *testing.T) {
	for _, days := range []int{31, 29} {
		t.Run(time.Duration(days*24).String(), func(t *testing.T) {
			p, rc, _ := setupProvider(t)
			convergeProvider(t, p, rc)
			state, err := loadState(rc.Cluster)
			if err != nil {
				t.Fatal(err)
			}
			member := state.Members["gateway"]
			before, err := p.MemberConfig(t.Context(), rc, member.Name)
			if err != nil {
				t.Fatal(err)
			}
			caPEM, err := rc.Secrets.GetClusterSecret(t.Context(), rc.Cluster.Ref(), "tls/ca.crt")
			if err != nil {
				t.Fatal(err)
			}
			keyPEM, err := rc.Secrets.GetClusterSecret(t.Context(), rc.Cluster.Ref(), "tls/ca.key")
			if err != nil {
				t.Fatal(err)
			}
			ca, err := pki.LoadCA(caPEM, keyPEM)
			if err != nil {
				t.Fatal(err)
			}
			cert, key, err := ca.Issue(pki.CertRequest{CommonName: member.Name, DNSNames: []string{member.Name}, Server: true, Validity: time.Duration(days) * 24 * time.Hour})
			if err != nil {
				t.Fatal(err)
			}
			before.AgentTLS = agentapi.Certificate{CertPEM: string(cert), KeyPEM: string(key)}
			for path, value := range map[string]any{"supabase/agent-cert/" + member.Name: before.AgentTLS, "supabase/config/" + member.Name: before} {
				raw, _ := json.Marshal(value)
				if err := rc.Secrets.SetClusterSecret(t.Context(), rc.Cluster.Ref(), path, raw); err != nil {
					t.Fatal(err)
				}
			}
			busy, err := p.syncComponents(t.Context(), rc)
			if err != nil || busy != (days == 29) {
				t.Fatalf("renewal busy=%v, error=%v", busy, err)
			}
			after, err := p.MemberConfig(t.Context(), rc, member.Name)
			if err != nil {
				t.Fatal(err)
			}
			if days == 31 {
				if after.AgentTLS != before.AgentTLS || after.ConfigVersion != before.ConfigVersion {
					t.Fatal("renewed before the core thirty-day threshold")
				}
				return
			}
			if after.ConfigVersion != before.ConfigVersion+1 || after.AgentTLS.CertPEM == before.AgentTLS.CertPEM || string(after.EngineSpec) != string(before.EngineSpec) {
				t.Fatal("certificate renewal must increment config version without changing settings")
			}
			if renew, err := componentCertificateExpiry(rc, after.AgentTLS); err != nil || renew {
				t.Fatal("renewed certificate is not fresh", err)
			}
		})
	}
}

func TestValidateRequiresExtensionBundleMirror(t *testing.T) {
	t.Setenv("KARAKORAM_ARTIFACT_MIRROR", "")
	p := New(postgres.Options{})
	cluster := &domain.Cluster{Name: "demo", Namespace: "default"}
	p.ApplyDefaults(&cluster.Spec)
	bundle, err := nativeartifacts.Get("supabase-pg17-extensions")
	if err != nil {
		t.Fatal(err)
	}
	_, expected := nativeartifacts.ResolveURL(bundle)
	var missingMirror *nativeartifacts.MissingMirrorError
	if !errors.As(expected, &missingMirror) {
		t.Fatalf("expected development mirror requirement, got %v", expected)
	}
	if err := p.Validate(t.Context(), nil, cluster); !errors.Is(err, engine.ErrInvalid) || !strings.Contains(err.Error(), missingMirror.Error()) {
		t.Fatalf("extension bundle was not rejected during admission: %v", err)
	}
	t.Setenv("KARAKORAM_ARTIFACT_MIRROR", "https://artifacts.example.invalid")
	if err := p.Validate(t.Context(), nil, cluster); err != nil {
		t.Fatal(err)
	}
}

func TestStoragePrepareInstallsStructuredArchiveValidatorDependency(t *testing.T) {
	p, rc, _ := setupProvider(t)
	runtime, err := p.runtimeSpec(rc, &componentMember{Name: "demost", Component: "storage"})
	if err != nil {
		t.Fatal(err)
	}
	if len(runtime.Prepare) < 2 || !strings.Contains(runtime.Prepare[0], " python3") || !strings.Contains(runtime.Prepare[1], "python3 -c") {
		t.Fatal("Storage preparation must install Python before validating archive headers")
	}
}
