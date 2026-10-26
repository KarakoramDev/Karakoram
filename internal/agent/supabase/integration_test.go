//go:build integration

// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package supabase

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/agent"
	"github.com/KarakoramDev/Karakoram/internal/agent/supabase/initsql"
	"github.com/KarakoramDev/Karakoram/internal/engine/supabase/sbkeys"
	"github.com/KarakoramDev/Karakoram/internal/engine/supabase/sbspec"
	"github.com/jackc/pgx/v5"
)

// TestNativeAPI exercises curated bootstrap plus the production renderer,
// forwarder and health paths with actual upstream foreground executables. Run
// unprivileged in an isolated network namespace: it owns loopback service ports.
func TestNativeAPI(t *testing.T) {
	pg := os.Getenv("KARAKORAM_PG_BIN_DIR")
	auth := os.Getenv("KARAKORAM_SUPABASE_AUTH_DIR")
	rest := os.Getenv("KARAKORAM_SUPABASE_REST_DIR")
	envoy := os.Getenv("KARAKORAM_SUPABASE_GATEWAY_DIR")
	if pg == "" || auth == "" || rest == "" || envoy == "" {
		t.Skip("native PostgreSQL, Auth, PostgREST and Envoy artifact directories are required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	root := t.TempDir()
	pgdata := filepath.Join(root, "pgdata")
	init := exec.CommandContext(ctx, filepath.Join(pg, "initdb"), "-D", pgdata, "-U", "postgres", "--auth-local=trust", "--auth-host=trust", "--no-locale", "--encoding=UTF8")
	if output, err := init.CombinedOutput(); err != nil {
		t.Fatalf("initdb: %v %s", err, output)
	}
	logDir := os.Getenv("KARAKORAM_SUPABASE_LOG_DIR")
	if logDir == "" {
		logDir = root
	}
	if err := os.MkdirAll(logDir, 0700); err != nil {
		t.Fatal(err)
	}
	start := func(name string, p agent.ProcessSpec) {
		t.Helper()
		f, err := os.OpenFile(filepath.Join(logDir, name+".log"), os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0600)
		if err != nil {
			t.Fatal(err)
		}
		cmd := exec.CommandContext(ctx, p.Binary, p.Args...)
		cmd.Dir = p.Dir
		cmd.Env = append([]string{"PATH=/usr/bin:/bin", "LANG=C.UTF-8"}, p.Env...)
		cmd.Stdout = f
		cmd.Stderr = f
		cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
		if err := cmd.Start(); err != nil {
			f.Close()
			t.Fatalf("start %s: %v", name, err)
		}
		t.Cleanup(func() {
			syscall.Kill(-cmd.Process.Pid, syscall.SIGTERM)
			done := make(chan struct{})
			go func() { cmd.Wait(); close(done) }()
			select {
			case <-done:
			case <-time.After(5 * time.Second):
				syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
				<-done
			}
			f.Close()
		})
	}
	start("postgres", agent.ProcessSpec{Binary: filepath.Join(pg, "postgres"), Args: []string{"-D", pgdata, "-h", "127.0.0.2", "-p", "5432", "-k", root, "-c", "wal_level=logical", "-c", "max_connections=50", "-c", "shared_buffers=32MB"}})
	var conn *pgx.Conn
	var err error
	for deadline := time.Now().Add(20 * time.Second); time.Now().Before(deadline); {
		conn, err = pgx.Connect(ctx, "postgres://postgres@127.0.0.2:5432/postgres?sslmode=disable")
		if err == nil {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if err != nil {
		t.Fatal("PostgreSQL did not start")
	}
	defer conn.Close(context.Background())
	keys, err := sbkeys.Generate(rand.Reader, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	passwords := map[string]string{}
	for _, role := range []string{"supabase_admin", "supabase_auth_admin", "authenticator", "supabase_storage_admin", "pgbouncer"} {
		passwords[role] = "native-" + role + "-" + rand.Text()
	}
	components := []string{"gateway", "auth", "rest"}
	plan := initsql.Plan(components, nil)
	if err := initsql.Apply(ctx, conn, plan, passwords); err != nil {
		t.Fatal(err)
	}
	if err := initsql.Apply(ctx, conn, plan, passwords); err != nil {
		t.Fatalf("bootstrap replay: %v", err)
	}
	if _, err := conn.Exec(ctx, "CREATE TABLE public.native_probe(id integer PRIMARY KEY, value text); INSERT INTO public.native_probe VALUES (1,'native-ok'); GRANT SELECT ON public.native_probe TO anon;"); err != nil {
		t.Fatal(err)
	}
	var superuser, escalated bool
	if err := conn.QueryRow(ctx, "SELECT rolsuper FROM pg_roles WHERE rolname='postgres'").Scan(&superuser); err != nil || !superuser {
		t.Fatal("postgres was demoted")
	}
	if err := conn.QueryRow(ctx, "SELECT pg_has_role('authenticator','supabase_admin','MEMBER')").Scan(&escalated); err != nil || escalated {
		t.Fatal("authenticator has admin membership")
	}
	base := sbspec.Spec{RestartToken: "native-test", Topology: "single", DataPeers: []string{"127.0.0.2"}, Database: "postgres", DBRoles: passwords, SiteURL: "http://localhost:3000", APIExternalURL: "http://127.0.0.1:8000", GatewayURL: "http://127.0.0.1:8000", JWTSecret: keys.JWTSecret, AnonKey: keys.AnonKey, ServiceRoleKey: keys.ServiceRoleKey, Auth: sbspec.Auth{Autoconfirm: true, JWTExpiry: 3600}, REST: sbspec.REST{Schemas: []string{"public"}}, Upstreams: map[string]string{"auth": "127.0.0.1:9999", "rest": "127.0.0.1:3000"}}
	// An enabled metadata route must reject anonymous credentials before any
	// upstream connection; the core fixture deliberately starts no meta process.
	base.Upstreams["meta"] = "127.0.0.1:8080"
	f, err := newForwarder(base, "127.0.0.1:5432")
	if err != nil {
		t.Fatal(err)
	}
	f.start(ctx)
	defer f.stop()
	launch := func(component, version, install string) {
		s := base
		s.Component = component
		s.Version = version
		s.InstallDir = install
		if err := s.Validate(); err != nil {
			t.Fatal(err)
		}
		p, files, err := Render(s, root)
		if err != nil {
			t.Fatal(err)
		}
		for name, data := range files {
			path := filepath.Join(root, "karakoram", name)
			if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(path, data, 0600); err != nil {
				t.Fatal(err)
			}
		}
		if err := Prepare(ctx, s, p, root); err != nil {
			t.Fatal(err)
		}
		start(component, p)
		for deadline := time.Now().Add(35 * time.Second); ; {
			if healthy(ctx, s) == nil {
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("%s health endpoint did not become ready; see protected process log", component)
			}
			time.Sleep(100 * time.Millisecond)
		}
	}
	launch("auth", "2.197.0", auth)
	t.Run("AuthPinnedEmptyDatabaseLookup", func(t *testing.T) {
		// Exercise the actual pinned 2.197.0 handler, not a liveness fixture:
		// no users exist yet, and loadUser must complete its indexed DB query.
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://127.0.0.1:9999/admin/users/00000000-0000-0000-0000-000000000000", nil)
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Authorization", "Bearer "+base.ServiceRoleKey)
		response, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer response.Body.Close()
		var result struct {
			ErrorCode string `json:"error_code"`
		}
		if err := json.NewDecoder(io.LimitReader(response.Body, 4096)).Decode(&result); err != nil || response.StatusCode != http.StatusNotFound || result.ErrorCode != "user_not_found" {
			t.Fatalf("pinned Auth absent-user lookup: HTTP %d, error code %q, decode=%v", response.StatusCode, result.ErrorCode, err)
		}
	})
	launch("rest", "14.18", rest)
	launch("gateway", "1.39.1", envoy)
	request := func(method, path, key string, body []byte) (int, []byte) {
		t.Helper()
		req, err := http.NewRequestWithContext(ctx, method, "http://127.0.0.1:8000"+path, bytes.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("apikey", key)
		req.Header.Set("Content-Type", "application/json")
		response, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer response.Body.Close()
		data, _ := io.ReadAll(io.LimitReader(response.Body, 1<<20))
		return response.StatusCode, data
	}
	signup, _ := json.Marshal(map[string]string{"email": "native-" + strings.ToLower(rand.Text()) + "@example.com", "password": "N4tive-test-" + rand.Text()})
	status, data := request("POST", "/auth/v1/signup", keys.AnonKey, signup)
	if status != 200 {
		t.Fatalf("signup status %d (body withheld)", status)
	}
	var user struct {
		ID          string `json:"id"`
		AccessToken string `json:"access_token"`
		User        struct {
			ID string `json:"id"`
		} `json:"user"`
	}
	if json.Unmarshal(data, &user) != nil || (user.ID == "" && user.User.ID == "") {
		t.Fatal("signup did not return a user")
	}
	t.Log("gateway signup: HTTP 200, autoconfirm user created")
	status, data = request("GET", "/rest/v1/native_probe", keys.AnonKey, nil)
	if status != 200 || !bytes.Contains(data, []byte("native-ok")) {
		t.Fatalf("anon REST query status %d", status)
	}
	t.Log("gateway anon PostgREST query: HTTP 200, expected row returned")
	status, _ = request("GET", "/pg/tables", keys.AnonKey, nil)
	if status != 403 {
		t.Fatalf("anonymous metadata route status %d, expected 403", status)
	}
	t.Log("gateway service-only metadata route: anonymous HTTP 403")
	t.Log("bootstrap replay: passed; postgres superuser retained; authenticator has no supabase_admin membership")
}
