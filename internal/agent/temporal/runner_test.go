// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package temporal

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/agent"
	"github.com/KarakoramDev/Karakoram/internal/engine/temporal/tspec"
	"gopkg.in/yaml.v3"
)

func testSpec() tspec.Spec {
	return tspec.Spec{Component: tspec.Server, Version: "1.32.1", InstallDir: "/opt/karakoram/temporal-server-1.32-1.32.1", RestartToken: "one", ClusterName: "test", HistoryShards: 8, Persistence: &tspec.Database{Host: "pg.example.test", Port: 5432, Database: "temporal", VisibilityDatabase: "temporal_visibility", Username: "owner", Password: "private-password", CAPEM: "postgres-ca", MaxConns: 10, VisibilityMaxConns: 5}, TLS: tspec.TLS{CAPEM: "ca", InternodeCertPEM: "cert", InternodeKeyPEM: "key", InternodeServerName: "internode.test", FrontendCertPEM: "frontend-cert", FrontendKeyPEM: "frontend-key", FrontendServerName: "frontend.test", RequireClientAuth: true}}
}

// TestRenderRoles checks role selection and private TLS configuration.
func TestRenderRoles(t *testing.T) {
	for _, component := range []string{tspec.Server, tspec.History, tspec.Matching, tspec.Frontend, tspec.Worker, tspec.UI} {
		t.Run(component, func(t *testing.T) {
			s := testSpec()
			s.Component = component
			if component == tspec.UI {
				s.Persistence = nil
				s.FrontendAddress = "frontend:7233"
				s.UI = &tspec.UISettings{PublicURL: "https://ui.example.test/workflows", OIDC: &tspec.OIDC{Issuer: "https://issuer.example.test", ClientID: "id", ClientSecret: "private-oidc", CallbackURL: "https://ui.example.test/workflows/auth/sso/callback"}}
			}
			if err := s.Validate(); err != nil {
				t.Fatal(err)
			}
			p, files, err := render(s, "/data", "10.0.0.1", "0.0.0.0", s.InstallDir)
			if err != nil {
				t.Fatal(err)
			}
			if strings.Contains(strings.Join(append(p.Args, p.Env...), " "), "private-") {
				t.Fatal("secret in process arguments or environment")
			}
			var config map[string]any
			if err := yaml.Unmarshal(files["temporal/development.yaml"], &config); err != nil {
				t.Fatal(err)
			}
			if component == tspec.UI {
				if config["publicPath"] != "/workflows" || config["port"] != 8080 {
					t.Fatal(config)
				}
				return
			}
			text := string(files["temporal/development.yaml"])
			for _, key := range []string{"postgres12_pgx", "numHistoryShards: 8", "enableHostVerification: true", "serverName: pg.example.test", "maxConns: 10", "maxIdleConns: 10", "broadcastAddress: 10.0.0.1", "requireClientAuth: true", "127.0.0.1:9090", "/data/karakoram/temporal/postgres-ca.pem", "systemWorker:", "httpPort: 7243"} {
				if !strings.Contains(text, key) {
					t.Errorf("config missing %s", key)
				}
			}
			for _, role := range tspec.Roles(component) {
				if !strings.Contains(strings.Join(p.Args, " "), "--service="+role) {
					t.Error("missing role", role)
				}
			}
		})
	}
}

// TestNamespaceValidationRoutes checks malformed and protected namespace actions.
func TestNamespaceValidationRoutes(t *testing.T) {
	r := &Runner{spec: testSpec(), applied: true}
	mux := http.NewServeMux()
	r.Routes(mux, nil)
	for _, tt := range []struct {
		action, body string
		code         int
	}{
		{tspec.ActionEnsureNamespaces, `null`, 400},
		{tspec.ActionEnsureNamespaces, `[{"name":"orders","retention":"1h"}]`, 400},
		{tspec.ActionEnsureNamespaces, `[{"name":"orders","retention":"24h","password":"x"}]`, 400},
		{tspec.ActionEnsureNamespaces, `[{"name":"orders","retention":"24h"},{"name":"orders","retention":"24h"}]`, 400},
		{tspec.ActionEnsureNamespaces, `[] {}`, 400},
		{tspec.ActionDeleteNamespace, `{"name":"default"}`, 409},
		{tspec.ActionDeleteNamespace, `{"name":"temporal-system"}`, 409},
		{tspec.ActionDeleteNamespace, `{"name":"../unsafe"}`, 400},
		{tspec.ActionEnsureNamespaces, strings.Repeat(" ", 65537) + "[]", 400},
	} {
		request := httptest.NewRequest(http.MethodPost, "/v1/temporal/"+tt.action, strings.NewReader(tt.body))
		response := httptest.NewRecorder()
		mux.ServeHTTP(response, request)
		if response.Code != tt.code {
			t.Errorf("%s: got %d want %d", tt.action, response.Code, tt.code)
		}
	}
	r.spec.Component = tspec.History
	for _, action := range []string{tspec.ActionEnsureNamespaces, tspec.ActionDeleteNamespace, tspec.ActionNamespaces} {
		response := httptest.NewRecorder()
		mux.ServeHTTP(response, httptest.NewRequest(http.MethodPost, "/v1/temporal/"+action, strings.NewReader("{}")))
		if response.Code != 409 {
			t.Error("non-frontend action was accepted", action, response.Code)
		}
	}
}

// TestReadinessArguments checks the post-flag component argument contract.
func TestReadinessArguments(t *testing.T) {
	r := &Runner{}
	options := agent.Options{DataDir: t.TempDir()}
	for _, args := range [][]string{nil, {"--engine=temporal", "server"}, {"unknown"}, {"server", "history"}} {
		if r.Readiness(context.Background(), options, args) == nil {
			t.Errorf("accepted %q", args)
		}
	}
	dir := filepath.Join(options.DataDir, "karakoram", "temporal")
	if err := os.MkdirAll(dir, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "readiness.json"), []byte(`{"Component":"history","Address":"127.0.0.1"}`), 0600); err != nil {
		t.Fatal(err)
	}
	if err := r.Readiness(context.Background(), options, []string{"server"}); err == nil || !strings.Contains(err.Error(), "snapshot") {
		t.Fatal(err)
	}
}

// TestSchemaToolSanitization checks that migration diagnostics omit secrets.
func TestSchemaToolSanitization(t *testing.T) {
	for _, err := range []error{errors.New("postgres://owner:private-password@host/db"), &exec.ExitError{Stderr: []byte("SQL_PASSWORD=private-password")}} {
		result := toolError(err)
		if result == nil || strings.Contains(result.Error(), "private-password") || strings.Contains(result.Error(), "postgres://") {
			t.Fatal("schema diagnostic leaked input")
		}
	}
	if toolError(nil) != nil {
		t.Fatal("success treated as failure")
	}
	dir := t.TempDir()
	script := "#!/bin/sh\nprintf 'postgres://owner:private-password@host/db\\n' >&2\nexit 42\n"
	if err := os.WriteFile(filepath.Join(dir, "temporal-sql-tool"), []byte(script), 0700); err != nil {
		t.Fatal(err)
	}
	err := runSchemaTool(context.Background(), testSpec(), dir, dir, "temporal", "setup-schema", "-v", "0.0")
	if err == nil || !strings.Contains(err.Error(), "exit 42") || strings.Contains(err.Error(), "private-password") {
		t.Fatal("schema output was not sanitized")
	}
}
