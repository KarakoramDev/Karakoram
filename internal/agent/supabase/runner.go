// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

// Package supabase supervises one native Supabase component per member.
package supabase

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/agent"
	"github.com/KarakoramDev/Karakoram/internal/agent/supabase/gateway"
	"github.com/KarakoramDev/Karakoram/internal/agent/supabase/logship"
	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/engine/supabase/sbkeys"
	"github.com/KarakoramDev/Karakoram/internal/engine/supabase/sbspec"
)

var _ agent.Engine = (*Runner)(nil)

// Runner is registered for engine supabase; its zero value is ready for use.
type Runner struct {
	mu             sync.RWMutex
	spec           sbspec.Spec
	applied        bool
	digest         [32]byte
	preparedDigest [32]byte
	prepared       bool
	forward        *forwarder
	metaBridge     *http.Server
	studioIngress  *http.Server
	shipCancel     context.CancelFunc
	storage        storageProbe
}

func (*Runner) Name() string        { return sbspec.Engine }
func (*Runner) Processes() []string { return append([]string(nil), sbspec.Components...) }
func (r *Runner) Version(context.Context) string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.spec.Version
}

func (r *Runner) Apply(ctx context.Context, n *agent.Node, c agentapi.MemberConfig) ([]string, error) {
	if c.Engine != sbspec.Engine {
		return nil, errors.New("configuration engine does not match Supabase")
	}
	var s sbspec.Spec
	if len(c.EngineSpec) == 0 || json.Unmarshal(c.EngineSpec, &s) != nil {
		return nil, errors.New("invalid Supabase component payload")
	}
	if err := s.Validate(); err != nil {
		return nil, err
	}
	if err := validateService(s); err != nil {
		return nil, err
	}
	process, files, err := Render(s, n.DataDir())
	if err != nil {
		return nil, err
	}
	digest := sha256.Sum256(c.EngineSpec)
	r.mu.RLock()
	old, applied, changed := r.spec, r.applied, r.digest != digest
	r.mu.RUnlock()
	if !applied {
		changed = true
	}
	if changed && applied {
		if err := n.Action(ctx, old.Component, "stop"); err != nil {
			return nil, err
		}
	}
	if changed && (s.Component == "functions" || old.Component == "functions") {
		protected, err := os.OpenRoot(filepath.Join(n.DataDir(), "karakoram"))
		if err == nil {
			err = protected.RemoveAll("functions")
			protected.Close()
		}
		if err != nil && !os.IsNotExist(err) {
			return nil, err
		}
	}
	// Render through rooted atomic writes; no service can read another member's
	// secrets via group/world permissions or a symlink outside the protected root.
	for name, data := range files {
		if err := n.WriteFile(name, data, 0600); err != nil {
			return nil, err
		}
	}
	if s.CAPEM != "" {
		if err := n.WriteFile("supabase/ca.pem", []byte(s.CAPEM), 0600); err != nil {
			return nil, err
		}
	}
	readiness, _ := json.Marshal(sbspec.Spec{Component: s.Component, AnonKey: s.AnonKey, ServiceRoleKey: s.ServiceRoleKey, RealtimeAdminSecret: s.RealtimeAdminSecret})
	if err := n.WriteFile("supabase/readiness.json", readiness, 0600); err != nil {
		return nil, err
	}
	protected, err := os.OpenRoot(filepath.Join(n.DataDir(), "karakoram"))
	if err != nil {
		return nil, err
	}
	for _, directory := range []string{"functions", "snippets", "deno-cache", "vector-data"} {
		if err = protected.MkdirAll(directory, 0700); err != nil {
			protected.Close()
			return nil, err
		}
	}
	protected.Close()
	if changed {
		r.mu.Lock()
		if r.forward != nil {
			r.forward.stop()
			r.forward = nil
		}
		if r.shipCancel != nil {
			r.shipCancel()
			r.shipCancel = nil
		}
		if r.metaBridge != nil {
			_ = r.metaBridge.Close()
			r.metaBridge = nil
		}
		if r.studioIngress != nil {
			_ = r.studioIngress.Close()
			r.studioIngress = nil
		}
		if s.Component == "meta" || s.Component == "studio" {
			server, e := startMetaBridge(s)
			if e != nil {
				r.mu.Unlock()
				return nil, e
			}
			r.metaBridge = server
		}
		if s.Component == "studio" {
			server, e := startStudioIngress(s)
			if e != nil {
				r.mu.Unlock()
				return nil, e
			}
			r.studioIngress = server
		}
		if sbspec.NeedsDatabase(s.Component) {
			address := "127.0.0.1:5432"
			if s.Component == "pooler" {
				address = "127.0.0.1:15432"
			}
			f, e := newForwarder(s, address)
			if e != nil {
				r.mu.Unlock()
				return nil, e
			}
			f.start(context.Background())
			r.forward = f
		}
		r.mu.Unlock()
	}
	if err := n.Configure(s.Component, process); err != nil {
		return nil, err
	}
	states, _ := n.Snapshot()
	for name := range states {
		if name != s.Component {
			if err := n.Action(ctx, name, "stop"); err != nil {
				return nil, err
			}
		}
	}
	if !c.Maintenance && !n.Stopped(s.Component) && (c.DesiredProcesses == nil || contains(c.DesiredProcesses, s.Component)) {
		if err := r.prepare(ctx, s, process, n.DataDir(), digest); err != nil {
			return nil, err
		}
		action := "start"
		if changed && applied && old.Component == s.Component {
			action = "restart"
		}
		if err := n.Action(ctx, s.Component, action); err != nil {
			return nil, err
		}
	} else {
		if err := n.Action(ctx, s.Component, "stop"); err != nil {
			return nil, err
		}
	}
	r.mu.Lock()
	r.spec = s
	r.applied = true
	r.digest = digest
	if changed && s.VectorSinkURL != "" {
		secrets := []string{s.JWTSecret, s.JWTKeys, s.ServiceRoleKey, s.SecretKey, s.ServiceAsymmetricJWT, s.Studio.DashboardPassword, s.Auth.SMTP.Password, s.Storage.SecretKey, s.Storage.ProtocolSecretKey, s.SecretKeyBase, s.VaultEncKey, s.RealtimeDBEncKey, s.LogflarePrivateToken, s.LogflarePublicToken, s.VectorSinkToken, s.PatroniPassword}
		for _, password := range s.DBRoles {
			secrets = append(secrets, password)
		}
		r.shipCancel = logship.Start(context.Background(), n, logship.Options{URL: s.VectorSinkURL, Token: s.VectorSinkToken, Member: c.Member, Processes: []string{s.Component}, Secrets: secrets})
	}
	r.mu.Unlock()
	if changed {
		return []string{s.Component}, nil
	}
	return nil, nil
}
func contains(names []string, name string) bool {
	for _, candidate := range names {
		if candidate == name {
			return true
		}
	}
	return false
}

func validateService(s sbspec.Spec) error {
	role := ""
	switch s.Component {
	case "auth":
		role = "supabase_auth_admin"
	case "rest":
		role = "authenticator"
	case "storage":
		role = "supabase_storage_admin"
	case "realtime", "analytics", "pooler":
		role = "supabase_admin"
	case "meta", "studio", "functions":
		role = "postgres"
	}
	if role != "" && s.DBRoles[role] == "" {
		return fmt.Errorf("missing database credential for %s", role)
	}
	if s.Component == "auth" && (s.SiteURL == "" || s.APIExternalURL == "") {
		return errors.New("site and external API URLs are required for Auth")
	}
	if (s.Component == "meta" || s.Component == "studio") && s.Studio.MetaCryptoKey == "" {
		return errors.New("metadata crypto key is required")
	}
	if s.Component == "analytics" && (s.LogflarePublicToken == "" || s.LogflarePrivateToken == "") {
		return errors.New("analytics ingestion and private tokens are required")
	}
	if s.REST.MaxRows < 0 || s.Storage.FileSizeLimit < 0 {
		return errors.New("component limits cannot be negative")
	}
	if s.Component == "realtime" && (len(s.RealtimeDBEncKey) != 16 || len(s.SecretKeyBase) < 64) {
		return errors.New("invalid Realtime encryption secrets")
	}
	if s.Component == "realtime" && (len(s.RealtimeAdminSecret) < 32 || s.RealtimeAdminSecret == s.JWTSecret) {
		return errors.New("an independent administration secret is required for Realtime")
	}
	if s.Component == "pooler" && (s.Pooler.TenantID == "" || s.DBRoles["pgbouncer"] == "" || len(s.VaultEncKey) != 32 || len(s.SecretKeyBase) < 64) {
		return errors.New("invalid pooler tenant or secrets")
	}
	if s.Component == "storage" {
		u, e := url.Parse(s.Storage.Endpoint)
		if e != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") || s.Storage.TLS != (u.Scheme == "https") || s.Storage.Bucket == "" || s.Storage.TenantID == "" || s.Storage.AccessKey == "" || s.Storage.SecretKey == "" {
			return errors.New("storage requires a registered S3 backend")
		}
	}
	return nil
}

// BeforeStart prepares the current payload even when Apply accepted it while
// stopped. Generic explicit start/restart controls also invoke this hook.
func (r *Runner) BeforeStart(ctx context.Context, n *agent.Node, name string) error {
	c := n.Config()
	digest := sha256.Sum256(c.EngineSpec)
	r.mu.RLock()
	prepared := r.prepared && r.preparedDigest == digest
	r.mu.RUnlock()
	if prepared {
		return nil
	}
	var s sbspec.Spec
	if err := json.Unmarshal(c.EngineSpec, &s); err != nil {
		return err
	}
	if name != s.Component {
		return errors.New("process does not match the configured Supabase component")
	}
	process, _, err := Render(s, n.DataDir())
	if err != nil {
		return err
	}
	return r.prepare(ctx, s, process, n.DataDir(), digest)
}

func (r *Runner) prepare(ctx context.Context, s sbspec.Spec, process agent.ProcessSpec, dataDir string, digest [32]byte) error {
	r.mu.RLock()
	prepared := r.prepared && r.preparedDigest == digest
	r.mu.RUnlock()
	if prepared {
		return nil
	}
	if err := Prepare(ctx, s, process, dataDir); err != nil {
		return err
	}
	r.mu.Lock()
	r.preparedDigest, r.prepared = digest, true
	r.mu.Unlock()
	return nil
}

// Seed startup also opens HTTP. Start with the independent admin secret in
// application configuration before changing only the environment read by seeds.
const realtimeSeed = `{:ok, _} = Application.ensure_all_started(:realtime)
System.put_env("API_JWT_SECRET", System.fetch_env!("KARAKORAM_REALTIME_TENANT_JWT_SECRET"))
Realtime.Release.seeds(Realtime.Repo)`

// Prepare runs upstream release migrations and tenant seed before foreground
// startup. Output is intentionally discarded: migration tools may echo URLs.
func Prepare(ctx context.Context, s sbspec.Spec, p agent.ProcessSpec, dataDir string) error {
	var commands [][]string
	l := artifactLayout(s)
	switch s.Component {
	case "realtime":
		commands = [][]string{{l.migrate}, {l.release, "eval", realtimeSeed}}
	case "pooler":
		data, err := os.ReadFile(filepath.Join(dataDir, "karakoram/supabase/pooler.exs"))
		if err != nil {
			return err
		}
		commands = [][]string{{l.migrate}, {l.release, "eval", string(data)}}
	case "analytics":
		commands = [][]string{{l.release, "eval", "Logflare.Release.migrate"}}
	}
	for _, args := range commands {
		commandCtx, cancel := context.WithTimeout(ctx, 2*time.Minute)
		cmd := exec.CommandContext(commandCtx, args[0], args[1:]...)
		cmd.Dir = p.Dir
		cmd.Env = append([]string{"PATH=" + filepath.Join(s.InstallDir, "bin") + ":/usr/local/bin:/usr/bin:/bin"}, p.Env...)
		if s.Component == "realtime" && len(args) > 1 && args[1] == "eval" {
			cmd.Env = append(cmd.Env, "KARAKORAM_REALTIME_TENANT_JWT_SECRET="+s.JWTSecret)
		}
		err := cmd.Run()
		cancel()
		if err != nil {
			return fmt.Errorf("%s database preparation failed: %w", s.Component, err)
		}
	}
	return nil
}

func (r *Runner) Readiness(ctx context.Context, o agent.Options, args []string) error {
	var s sbspec.Spec
	data, err := os.ReadFile(filepath.Join(o.DataDir, "karakoram/supabase/readiness.json"))
	if err == nil {
		_ = json.Unmarshal(data, &s)
	}
	if len(args) > 0 {
		s.Component = args[0]
	}
	return healthy(ctx, s)
}
func healthy(ctx context.Context, s sbspec.Spec) error {
	address := ""
	path := ""
	method := "GET"
	switch s.Component {
	case "gateway":
		address = "127.0.0.1:8000"
	case "functions":
		address = "127.0.0.1:9000"
	case "auth":
		address = "127.0.0.1:9999"
		// A primary-key lookup is bounded even on large tenants. Listing one
		// user still executes COUNT(*) in GoTrue's default offset pagination.
		// A valid absent UUID returns user_not_found only after a DB lookup.
		path = "/admin/users/00000000-0000-0000-0000-000000000000"
	case "rest":
		address = "127.0.0.1:3001"
		path = "/ready"
	case "realtime":
		address = "127.0.0.1:4000"
		path = "/api/tenants/realtime-dev/health"
		// HTTP 200 can carry an unhealthy result; inspect the GET payload.
	case "storage":
		address = "127.0.0.1:5000"
		// /status is unconditional liveness. Listing buckets exercises the
		// configured tenant database without creating application objects.
		path = "/bucket"
	case "imgproxy":
		address = "127.0.0.1:5001"
		path = "/health"
	case "meta":
		address = "127.0.0.1:8081"
		// /health returns a timestamp without consulting PostgreSQL.
		path = "/schemas"
	case "studio":
		address = "127.0.0.1:3002"
		path = "/api/platform/profile"
	case "pooler":
		address = "127.0.0.1:4000"
		path = "/api/health"
	case "analytics":
		address = "127.0.0.1:4000"
		path = "/health"
	case "vector":
		address = "127.0.0.1:9001"
		path = "/health"
	default:
		return errors.New("unconfigured Supabase component")
	}
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	if path == "" {
		conn, err := (&net.Dialer{}).DialContext(ctx, "tcp", address)
		if err != nil {
			return errors.New("component listener is not ready")
		}
		conn.Close()
		return nil
	}
	req, _ := http.NewRequestWithContext(ctx, method, "http://"+address+path, nil)
	if s.Component == "realtime" {
		token, err := sbkeys.LegacyJWT(s.RealtimeAdminSecret, "service_role", time.Now())
		if err != nil {
			return errors.New("credential for Realtime administration is unavailable")
		}
		req.Header.Set("Authorization", "Bearer "+token)
	}
	if s.Component == "studio" {
		req.Header.Set("apikey", s.ServiceRoleKey)
	}
	if s.Component == "auth" || s.Component == "storage" {
		req.Header.Set("Authorization", "Bearer "+s.ServiceRoleKey)
	}
	response, err := http.DefaultClient.Do(req)
	if err != nil {
		if s.Component == "auth" || s.Component == "storage" || s.Component == "meta" || s.Component == "rest" {
			return fmt.Errorf("%s database readiness probe is unavailable; inspect the component and its PostgreSQL primary connectivity/schema readiness", s.Component)
		}
		return errors.New("component health endpoint is unavailable")
	}
	defer response.Body.Close()
	if s.Component == "auth" && response.StatusCode == http.StatusNotFound {
		var result struct {
			ErrorCode string `json:"error_code"`
		}
		if err := json.NewDecoder(io.LimitReader(response.Body, 4096)).Decode(&result); err == nil && result.ErrorCode == "user_not_found" {
			return nil
		}
	}
	if response.StatusCode != 200 && response.StatusCode != 204 {
		if s.Component == "auth" || s.Component == "storage" || s.Component == "meta" || s.Component == "rest" {
			return fmt.Errorf("%s database readiness probe returned HTTP %d; inspect the component and its PostgreSQL primary connectivity/schema readiness", s.Component, response.StatusCode)
		}
		return errors.New("component health endpoint is not ready")
	}
	if s.Component == "realtime" {
		var health struct {
			Data struct {
				Healthy bool `json:"healthy"`
			} `json:"data"`
		}
		if err := json.NewDecoder(io.LimitReader(response.Body, 64<<10)).Decode(&health); err != nil || !health.Data.Healthy {
			return errors.New("tenant/database health of Realtime is not ready; inspect the tenant and its PostgreSQL primary connectivity")
		}
	}
	if s.Component == "rest" {
		// /ready can retain a live LISTEN connection and a loaded cache while
		// serving requests fail to acquire/query a database connection. The
		// read-only OpenAPI root exercises PostgREST's actual database path.
		req, _ := http.NewRequestWithContext(ctx, http.MethodGet, "http://127.0.0.1:3000/", nil)
		req.Header.Set("Authorization", "Bearer "+s.ServiceRoleKey)
		response, err := http.DefaultClient.Do(req)
		if err != nil {
			return errors.New("REST database/schema cache serving probe is unavailable; inspect PostgREST and its PostgreSQL primary connectivity")
		}
		response.Body.Close()
		if response.StatusCode != http.StatusOK {
			return fmt.Errorf("REST database/schema cache serving probe returned HTTP %d; inspect PostgREST and its PostgreSQL primary connectivity", response.StatusCode)
		}
	}
	return nil
}
func (r *Runner) status(ctx context.Context, n *agent.Node) sbspec.Status {
	r.mu.RLock()
	s, applied := r.spec, r.applied
	r.mu.RUnlock()
	status := sbspec.Status{Component: s.Component, Version: s.Version}
	states, maintenance := n.Snapshot()
	if !applied {
		status.Message = "component configuration has not been applied"
	} else if maintenance || states[s.Component].State != "running" {
		status.Message = "component process is not running"
	} else if err := healthy(ctx, s); err != nil {
		status.Message = err.Error()
	} else {
		status.Ready = true
		if s.Component == "storage" {
			at, message := r.storage.check(ctx, s.Storage)
			status.StorageS3CheckedAt = &at
			if message != "" {
				status.Ready = false
				status.Reason = "StorageS3Unavailable"
				status.Message = message
			}
		}
		// This observation gates final stack readiness only. Ready remains the
		// listener/bootstrap signal so gateway-first provisioning can advance.
		if s.Component == "gateway" {
			hosts, err := gateway.HostCounts(ctx)
			if err != nil {
				status.Message = "gateway upstream discovery is unavailable"
			} else {
				status.GatewayUpstreamHosts = hosts
			}
		}
	}
	return status
}
func (r *Runner) Routes(mux *http.ServeMux, n *agent.Node) {
	handler := func(w http.ResponseWriter, req *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(r.status(req.Context(), n))
	}
	mux.HandleFunc("GET /v1/supabase/status", handler)
	mux.HandleFunc("POST /v1/supabase/status", handler)
}
func (r *Runner) Metrics(ctx context.Context, n *agent.Node) []agent.Metric {
	status := r.status(ctx, n)
	if status.Component == "" {
		return nil
	}
	up := float64(0)
	if status.Ready {
		up = 1
	}
	states, _ := n.Snapshot()
	labels := map[string]string{"component": status.Component}
	return []agent.Metric{{Name: "karakoram_supabase_up", Help: "Whether the Supabase component is ready", Labels: labels, Value: up}, {Name: "karakoram_supabase_restarts_total", Help: "Supervised Supabase component restarts", Labels: labels, Value: float64(states[status.Component].Restarts), Counter: true}}
}
