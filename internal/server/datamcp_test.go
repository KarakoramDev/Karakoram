// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package server

import (
	"context"
	"encoding/json"
	"errors"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/api"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/postgres"
)

func TestPostgresProfileDataMcpAdmission(t *testing.T) {
	p := postgres.New(postgres.Options{})
	for _, profile := range []string{"postgis", "pgvector", "timescaledb"} {
		t.Run(profile, func(t *testing.T) {
			c := &domain.Cluster{Spec: domain.ClusterSpec{Provider: "postgres", Topology: domain.Topology{Type: "ha"}, EngineOptions: json.RawMessage(`{"profiles":["` + profile + `"]}`), DataMcp: &domain.DataMcpSpec{Enabled: true}}}
			if err := validateDataMcp(p, c); err != nil {
				t.Fatalf("profile rejected: %v", err)
			}
			c.Spec.Topology.Type = "distributed"
			err := validateDataMcp(p, c)
			var field *engine.ValidationError
			if !errors.As(err, &field) || field.Field != "spec.dataMcp.enabled" || !strings.Contains(field.Message, "distributed") {
				t.Fatalf("distributed topology needs clear field error: %v", err)
			}
			c.Spec.DataMcp.Enabled = false
			if err := validateDataMcp(p, c); err != nil {
				t.Fatalf("disabled data MCP rejected: %v", err)
			}
		})
	}
}

type dataMcpProvider struct {
	fakeProvider
	called bool
	err    error
}

func (p *dataMcpProvider) DataAccessCredentials(context.Context, *engine.Context, engine.DataAccessLevel) (engine.DataAccessCredentials, error) {
	return engine.DataAccessCredentials{}, nil
}
func (p *dataMcpProvider) DataMcp(*domain.Cluster) (engine.DataMcpBackend, error) {
	p.called = true
	return engine.DataMcpBackend{Kind: engine.DataMcpNative}, p.err
}

func TestDataMcpAdmission(t *testing.T) {
	for _, tc := range []struct {
		name    string
		spec    *domain.DataMcpSpec
		invalid bool
	}{
		{"absent", nil, false},
		{"disabled", &domain.DataMcpSpec{Scope: domain.DataMcpScope{Databases: []string{""}}, Access: "invalid"}, false},
		{"default", &domain.DataMcpSpec{Enabled: true}, false},
		{"write", &domain.DataMcpSpec{Enabled: true, Access: domain.DataMcpAccessReadWrite}, false},
		{"bad access", &domain.DataMcpSpec{Enabled: true, Access: "write"}, true},
		{"empty tool", &domain.DataMcpSpec{Enabled: true, Tools: domain.DataMcpTools{Deny: []string{" "}}}, true},
		{"long tool", &domain.DataMcpSpec{Enabled: true, Tools: domain.DataMcpTools{Deny: []string{strings.Repeat("x", 65)}}}, true},
		{"duplicate tool", &domain.DataMcpSpec{Enabled: true, Tools: domain.DataMcpTools{Deny: []string{"a", "a"}}}, true},
		{"many tools", &domain.DataMcpSpec{Enabled: true, Tools: domain.DataMcpTools{Deny: make([]string, 65)}}, true},
		{"many databases", &domain.DataMcpSpec{Enabled: true, Scope: domain.DataMcpScope{Databases: make([]string, 65)}}, true},
		{"many schemas", &domain.DataMcpSpec{Enabled: true, Scope: domain.DataMcpScope{Schemas: make([]string, 65)}}, true},
		{"many indices", &domain.DataMcpSpec{Enabled: true, Scope: domain.DataMcpScope{IndexPatterns: make([]string, 65)}}, true},
		{"many vhosts", &domain.DataMcpSpec{Enabled: true, Scope: domain.DataMcpScope{Vhosts: make([]string, 65)}}, true},
		{"empty scope", &domain.DataMcpSpec{Enabled: true, Scope: domain.DataMcpScope{Schemas: []string{""}}}, true},
		{"long scope", &domain.DataMcpSpec{Enabled: true, Scope: domain.DataMcpScope{Vhosts: []string{strings.Repeat("x", 257)}}}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := &dataMcpProvider{}
			c := &domain.Cluster{Spec: domain.ClusterSpec{Provider: "fake", DataMcp: tc.spec}}
			err := validateDataMcp(p, c)
			if (err != nil) != tc.invalid {
				t.Fatalf("error = %v", err)
			}
			enabled := tc.spec.DataMcpEnabled()
			if p.called != (enabled && !tc.invalid) {
				t.Fatalf("provider called = %v", p.called)
			}
			if enabled && !tc.invalid && c.Spec.DataMcp.Access == "" {
				t.Fatal("missing default access")
			}
		})
	}
	c := &domain.Cluster{Spec: domain.ClusterSpec{Provider: "unsupported", DataMcp: &domain.DataMcpSpec{Enabled: true}}}
	if err := validateDataMcp(&fakeProvider{}, c); err == nil || !strings.Contains(err.Error(), "unsupported") {
		t.Fatalf("unsupported engine: %v", err)
	}
	want := errors.New("engine scope rejected")
	if err := validateDataMcp(&dataMcpProvider{err: want}, c); !errors.Is(err, want) {
		t.Fatalf("scope error = %v", err)
	}
}

func TestDataMcpModelRoundTripAndPatch(t *testing.T) {
	original := &domain.DataMcpSpec{Enabled: true, Access: domain.DataMcpAccessReadWrite, AuditQueryText: true, Scope: domain.DataMcpScope{Databases: []string{"app"}, Schemas: []string{"public"}, IndexPatterns: []string{"logs-*"}, Vhosts: []string{"/"}}, Tools: domain.DataMcpTools{Deny: []string{"drop"}, ConfirmWrites: true}, Resources: &domain.Resources{CPUMode: "SHARED", MinCPU: 1, MaxCPU: 2, MinRAM: 1, MaxRAM: 2, MinDisk: 5, MaxDisk: 10, StartCPU: ptr(1.0)}}
	if got := dataMcpDomain(dataMcpAPI(original)); !reflect.DeepEqual(got, original) {
		t.Fatalf("round trip: %#v", got)
	}
	if dataMcpAPI(nil) != nil || dataMcpDomain(nil) != nil {
		t.Fatal("nil spec became present")
	}
	base := specAPI(domain.ClusterSpec{DataMcp: original})
	b, err := json.Marshal(base)
	if err != nil {
		t.Fatal(err)
	}
	var target any
	if err = json.Unmarshal(b, &target); err != nil {
		t.Fatal(err)
	}
	patched := mergePatch(target, map[string]any{"dataMcp": map[string]any{"enabled": false, "scope": map[string]any{"schemas": nil}}})
	b, err = json.Marshal(patched)
	if err != nil {
		t.Fatal(err)
	}
	var input api.ClusterInputSpec
	if err = json.Unmarshal(b, &input); err != nil {
		t.Fatal(err)
	}
	got := specDomain(input).DataMcp
	if got.Enabled || got.Scope.Schemas != nil || !reflect.DeepEqual(got.Scope.Databases, original.Scope.Databases) || !got.Tools.ConfirmWrites || got.Access != original.Access {
		t.Fatalf("patch lost fields: %#v", got)
	}
	status := (&Server{}).statusAPI(domain.ClusterRef{}, domain.ClusterStatus{DataMcp: &domain.DataMcpStatus{Phase: "Ready", Kind: "native", Upstream: "native-elasticsearch", UpstreamVersion: "1", Access: "read", Message: "available"}}).DataMcp
	if status == nil || status.Phase != "Ready" || value(status.Kind) != "native" || value(status.Upstream) != "native-elasticsearch" || value(status.UpstreamVersion) != "1" || value(status.Access) != "read" || value(status.Message) != "available" {
		t.Fatalf("status: %#v", status)
	}
}

func TestDataMcpAdmissionMutationRoutes(t *testing.T) {
	for _, method := range []string{"POST", "PUT", "PATCH"} {
		t.Run(method, func(t *testing.T) {
			s, _, token := testServer(t)
			path := "/api/v1/clusters/prod"
			body := `{"spec":{"provider":"fake","dataMcp":{"enabled":true}}}`
			if method == "POST" {
				path = "/api/v1/clusters"
				body = `{"name":"sample","spec":{"provider":"fake","dataMcp":{"enabled":true}}}`
			}
			request := httptest.NewRequest(method, path, strings.NewReader(body))
			request.Header.Set("Authorization", "Bearer "+token)
			request.Header.Set("Content-Type", "application/json")
			result := httptest.NewRecorder()
			testHandler(s).ServeHTTP(result, request)
			if result.Code != 400 || !strings.Contains(result.Body.String(), "fake") {
				t.Fatalf("unsupported engine: status %d: %s", result.Code, result.Body.String())
			}
		})
	}
}

func TestDataMcpAcceptedMutationRoutes(t *testing.T) {
	for _, method := range []string{"POST", "PUT", "PATCH"} {
		t.Run(method, func(t *testing.T) {
			s, _, token := testServer(t)
			provider := &dataMcpProvider{}
			registry, err := engine.NewRegistry(provider)
			if err != nil {
				t.Fatal(err)
			}
			s.Registry = registry
			path := "/api/v1/clusters/prod"
			body := `{"spec":{"provider":"fake","dataMcp":{"enabled":true}}}`
			wantStatus := 200
			if method == "POST" {
				path = "/api/v1/clusters"
				body = `{"name":"sample","spec":{"provider":"fake","dataMcp":{"enabled":true}}}`
				wantStatus = 201
			}
			request := httptest.NewRequest(method, path, strings.NewReader(body))
			request.Header.Set("Authorization", "Bearer "+token)
			request.Header.Set("Content-Type", "application/json")
			result := httptest.NewRecorder()
			testHandler(s).ServeHTTP(result, request)
			if result.Code != wantStatus {
				t.Fatalf("status %d: %s", result.Code, result.Body.String())
			}
			var cluster api.Cluster
			if err := json.Unmarshal(result.Body.Bytes(), &cluster); err != nil {
				t.Fatal(err)
			}
			if !provider.called || cluster.Spec.DataMcp == nil || value(cluster.Spec.DataMcp.Access) != "read" {
				t.Fatalf("data MCP default missing: %s", result.Body.String())
			}
		})
	}
}
