// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

// Package keycloak supervises the native Keycloak distribution.
package keycloak

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/agent"
	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/engine/keycloak/kcspec"
)

var _ agent.Engine = (*Runner)(nil)

type Runner struct {
	mu      sync.RWMutex
	digest  [32]byte
	applied bool
}

func (*Runner) Name() string                   { return "keycloak" }
func (*Runner) Processes() []string            { return []string{"keycloak"} }
func (*Runner) Version(context.Context) string { return kcspec.Version }

// property escapes Java properties and SmallRye expression syntax. Secrets must
// not become additional options or expression lookups when parsed by Keycloak.
func property(s string) string {
	r := strings.NewReplacer("\\", "\\\\", "\n", "\\n", "\r", "\\r", "\t", "\\t", "$", "\\\\$", "=", "\\=", ":", "\\:", " ", "\\ ")
	return r.Replace(s)
}

// Render is shared with native qualification: all secret-bearing values live in
// one owner-only configuration file, never process arguments or environment.
func Render(s kcspec.Spec, dataDir string) (agent.ProcessSpec, map[string][]byte, error) {
	if err := s.Validate(); err != nil {
		return agent.ProcessSpec{}, nil, err
	}
	dir := filepath.Join(dataDir, "karakoram", "keycloak")
	pairs := [][2]string{
		{"db", "postgres"}, {"health-enabled", "true"}, {"http-enabled", "true"},
		{"http-host", "0.0.0.0"}, {"http-port", "8080"},
		{"http-management-host", "127.0.0.1"}, {"http-management-port", "9000"},
		{"http-management-scheme", "http"}, {"db-url", s.JDBCURL(filepath.Join(dir, "ca.pem"))},
		{"db-username", s.Username}, {"db-password", s.Password},
		{"bootstrap-admin-username", "admin"}, {"bootstrap-admin-password", s.AdminPassword},
		{"db-pool-max-size", "20"},
	}
	if s.Hostname == "" {
		pairs = append(pairs, [2]string{"hostname-strict", "false"})
	} else {
		pairs = append(pairs, [2]string{"hostname", s.Hostname})
	}
	if s.ProxyHeaders != "" {
		pairs = append(pairs, [2]string{"proxy-headers", s.ProxyHeaders})
	}
	var b strings.Builder
	for _, p := range pairs {
		fmt.Fprintf(&b, "%s=%s\n", p[0], property(p[1]))
	}
	files := map[string][]byte{"keycloak/keycloak.conf": []byte(b.String()), "keycloak/ca.pem": []byte(s.CAPEM)}
	return agent.ProcessSpec{Binary: kcspec.InstallDir + "/bin/kc.sh", Args: []string{"--config-file=" + filepath.Join(dir, "keycloak.conf"), "start", "--optimized"}, Env: []string{"JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64", "JAVA_OPTS_KC_HEAP=-Xms128m -Xmx768m"}, Dir: kcspec.InstallDir}, files, nil
}

type runnerNode interface {
	DataDir() string
	WriteFile(string, []byte, os.FileMode) error
	Configure(string, agent.ProcessSpec) error
	Action(context.Context, string, string) error
	Stopped(string) bool
}

func (r *Runner) Apply(ctx context.Context, n *agent.Node, c agentapi.MemberConfig) ([]string, error) {
	return r.apply(ctx, n, c, buildOptimized)
}

func buildOptimized(ctx context.Context, process agent.ProcessSpec) error {
	// Build has no runtime credentials in its environment, arguments or config.
	cmd := exec.CommandContext(ctx, process.Binary, "build", "--db=postgres", "--health-enabled=true")
	cmd.Dir = process.Dir
	cmd.Env = []string{"PATH=/usr/bin:/bin", "LANG=C.UTF-8", "JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64", "JAVA_OPTS_KC_HEAP=-Xms128m -Xmx768m"}
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("keycloak optimized build: %w", err)
	}
	return nil
}

func (r *Runner) apply(ctx context.Context, n runnerNode, c agentapi.MemberConfig, build func(context.Context, agent.ProcessSpec) error) ([]string, error) {
	if c.Engine != "keycloak" {
		return nil, errors.New("configuration engine does not match Keycloak")
	}
	var s kcspec.Spec
	if err := json.Unmarshal(c.EngineSpec, &s); err != nil {
		return nil, errors.New("invalid Keycloak configuration")
	}
	process, files, err := Render(s, n.DataDir())
	if err != nil {
		return nil, err
	}
	digest := sha256.Sum256(c.EngineSpec)
	r.mu.RLock()
	changed := !r.applied || r.digest != digest
	applied := r.applied
	r.mu.RUnlock()
	if changed && applied {
		if err = n.Action(ctx, "keycloak", "stop"); err != nil {
			return nil, err
		}
	}
	for name, data := range files {
		if err = n.WriteFile(name, data, 0600); err != nil {
			return nil, err
		}
	}
	if !applied {
		if err = build(ctx, process); err != nil {
			return nil, err
		}
	}
	if err = n.Configure("keycloak", process); err != nil {
		return nil, err
	}
	if !c.Maintenance && !n.Stopped("keycloak") && (c.DesiredProcesses == nil || slices.Contains(c.DesiredProcesses, "keycloak")) {
		if err = n.Action(ctx, "keycloak", "start"); err != nil {
			return nil, err
		}
	}
	r.mu.Lock()
	r.digest = digest
	r.applied = true
	r.mu.Unlock()
	if changed {
		return []string{"keycloak"}, nil
	}
	return nil, nil
}

func (*Runner) Readiness(ctx context.Context, _ agent.Options, _ []string) error { return ready(ctx) }
func ready(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://127.0.0.1:9000/health/ready", nil)
	if err != nil {
		return err
	}
	client := http.Client{Transport: &http.Transport{Proxy: nil}, Timeout: 3 * time.Second}
	defer client.CloseIdleConnections()
	res, err := client.Do(req)
	if err != nil {
		return errors.New("keycloak management health unavailable")
	}
	defer res.Body.Close()
	var result struct {
		Status string `json:"status"`
	}
	if res.StatusCode != http.StatusOK || json.NewDecoder(res.Body).Decode(&result) != nil || result.Status != "UP" {
		return errors.New("keycloak is not ready")
	}
	return nil
}
func (*Runner) Metrics(ctx context.Context, _ *agent.Node) []agent.Metric {
	value := 0.
	if ready(ctx) == nil {
		value = 1
	}
	return []agent.Metric{{Name: "karakoram_keycloak_ready", Help: "Keycloak management readiness", Value: value}}
}
func (*Runner) Routes(mux *http.ServeMux, _ *agent.Node) {
	handler := func(w http.ResponseWriter, req *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(struct {
			Ready bool `json:"ready"`
		}{ready(req.Context()) == nil})
	}
	mux.HandleFunc("GET /v1/keycloak/status", handler)
	mux.HandleFunc("POST /v1/keycloak/status", handler)
}
