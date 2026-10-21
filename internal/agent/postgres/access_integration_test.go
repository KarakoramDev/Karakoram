// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

//go:build integration

package postgres

import (
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/engine/postgres/pgspec"
	"github.com/jackc/pgx/v5"
	"golang.org/x/crypto/pbkdf2"
)

func nativeVerifier(t *testing.T, password string) string {
	t.Helper()
	salt := make([]byte, 16)
	if _, err := rand.Read(salt); err != nil {
		t.Fatal(err)
	}
	salted := pbkdf2.Key([]byte(password), salt, 4096, 32, sha256.New)
	h := hmac.New(sha256.New, salted)
	h.Write([]byte("Client Key"))
	stored := sha256.Sum256(h.Sum(nil))
	h = hmac.New(sha256.New, salted)
	h.Write([]byte("Server Key"))
	enc := base64.StdEncoding.EncodeToString
	return "SCRAM-SHA-256$4096:" + enc(salt) + "$" + enc(stored[:]) + ":" + enc(h.Sum(nil))
}
func nativeRequest(t *testing.T, h *nativeAgent, action string, request, result any) {
	t.Helper()
	response, err := h.request(http.MethodPost, "/v1/postgres/"+action, request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(response.Body, 1024))
		t.Fatalf("%s action HTTP%d: %s", action, response.StatusCode, body)
	}
	if result != nil {
		if err = json.NewDecoder(response.Body).Decode(result); err != nil {
			t.Fatal(err)
		}
	}
}
func userConnect(ctx context.Context, host, database, user, password string) (*pgx.Conn, error) {
	config, err := pgx.ParseConfig("")
	if err != nil {
		return nil, err
	}
	config.Host = host
	config.Port = 5432
	config.Database = database
	config.User = user
	config.Password = password
	config.TLSConfig = nil
	config.Fallbacks = nil
	config.ConnectTimeout = 2 * time.Second
	config.RuntimeParams = map[string]string{}
	return pgx.ConnectConfig(ctx, config)
}
func nativeProxy(t *testing.T, p *pgspec.Proxy, host, caPEM string) paths {
	t.Helper()
	paths := dataPaths(t.TempDir())
	if err := privateDir(paths.proxy); err != nil {
		t.Fatal(err)
	}
	spec := testSpec()
	spec.Role = pgspec.RoleProxy
	spec.Proxy = p
	spec.ListenHost = host
	spec.CAPEM = caPEM
	files, err := render(spec, paths, nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, file := range files {
		path := filepath.Join(paths.root, file.name)
		if err = os.MkdirAll(filepath.Dir(path), 0700); err != nil {
			t.Fatal(err)
		}
		if err = os.WriteFile(path, []byte(file.content), 0600); err != nil {
			t.Fatal(err)
		}
	}
	process := exec.Command("/usr/bin/proxysql", "-f", "-c", filepath.Join(paths.root, "proxysql.cnf"), "-D", paths.proxy)
	process.Dir = paths.proxy
	process.Env = []string{"PATH=/usr/bin:/bin", "LANG=C.UTF-8", "LC_ALL=C.UTF-8"}
	process.Stdout = io.Discard
	process.Stderr = io.Discard
	process.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err = process.Start(); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- process.Wait() }()
	t.Cleanup(func() {
		_ = syscall.Kill(-process.Process.Pid, syscall.SIGTERM)
		select {
		case <-done:
		case <-time.After(15 * time.Second):
			_ = syscall.Kill(-process.Process.Pid, syscall.SIGKILL)
			<-done
			t.Error("native proxy shutdown timed out")
		}
	})
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if restart, err := reconcileProxyWait(ctx, p, host, filepath.Join(paths.root, "ca.pem"), p.Admin); err != nil || restart {
		t.Fatalf("native proxy reconciliation failed: restart=%t err=%v", restart, err)
	}
	return paths
}

func TestNativeAccessAdminMembership(t *testing.T) {
	bin := os.Getenv("KARAKORAM_PG_BIN_DIR")
	if bin == "" {
		t.Skip("set KARAKORAM_PG_BIN_DIR for native admin membership acceptance")
	}
	for _, name := range []string{"postgres", "initdb", "pg_ctl"} {
		if info, err := os.Stat(filepath.Join(bin, name)); err != nil || info.Mode()&0111 == 0 {
			t.Skipf("native PostgreSQL binary %s is unavailable", name)
		}
	}
	if os.Geteuid() == 0 {
		t.Fatal("native PostgreSQL requires an unprivileged runtime user")
	}
	s := testSpec()
	s.BinDir = bin
	s.Postgres.Parameters = map[string]string{"shared_buffers": "32MB", "max_connections": "30"}
	h := startNativeAgent(t, s)
	parents := []string{"pg_read_all_data", "pg_write_all_data"}
	role := pgspec.RoleWrite{Role: pgspec.Role{Name: "bao_admin", Login: true, Createrole: true, ConnectionLimit: -1, MemberOf: parents, MemberOfAdmin: parents}}
	nativeRequest(t, h, pgspec.ActionRoles, pgspec.AccessRequest{Operation: "create", Role: &role}, nil)
	check := func(wantAdmin int) {
		t.Helper()
		var got pgspec.Role
		nativeRequest(t, h, pgspec.ActionRoles, pgspec.AccessRequest{Operation: "get", Name: role.Name}, &got)
		if len(got.MemberOf) != 2 || len(got.MemberOfAdmin) != wantAdmin || !got.Login || !got.Createrole {
			t.Fatalf("membership/admin options did not round trip: %+v", got)
		}
	}
	check(2)
	// Removing delegation must not revoke ordinary membership.
	role.MemberOfAdmin = nil
	nativeRequest(t, h, pgspec.ActionRoles, pgspec.AccessRequest{Operation: "update", Name: role.Name, Role: &role}, nil)
	check(0)
	role.MemberOfAdmin = parents
	nativeRequest(t, h, pgspec.ActionRoles, pgspec.AccessRequest{Operation: "update", Name: role.Name, Role: &role}, nil)
	check(2)
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	c, err := localConnect(ctx, s, dataPaths(h.dir))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close(context.Background())
	if _, err = c.Exec(ctx, `SET ROLE bao_admin; CREATE ROLE bao_dynamic; GRANT pg_read_all_data,pg_write_all_data TO bao_dynamic; DROP ROLE bao_dynamic; RESET ROLE`); err != nil {
		t.Fatal("OpenBao account cannot create, grant and drop dynamic roles", err)
	}
	nativeRequest(t, h, pgspec.ActionRoles, pgspec.AccessRequest{Operation: "delete", Name: role.Name}, nil)
}

func TestNativeAccessLifecycle(t *testing.T) {
	if os.Getenv("KARAKORAM_PG_BIN_DIR") == "" {
		t.Skip("set KARAKORAM_PG_BIN_DIR for native access acceptance")
	}
	if os.Geteuid() == 0 {
		t.Fatal("native PostgreSQL requires an unprivileged runtime user")
	}
	s := testSpec()
	s.BinDir = os.Getenv("KARAKORAM_PG_BIN_DIR")
	s.Postgres.Parameters = map[string]string{"shared_buffers": "32MB", "max_connections": "30"}
	h := startNativeAgent(t, s)
	first, second := "native-access-first-secret", "native-access-second-secret"
	hash := nativeVerifier(t, first)
	role := pgspec.RoleWrite{Role: pgspec.Role{Name: "access_app", Login: true, ConnectionLimit: -1, MemberOf: []string{"pg_monitor"}}, Password: &hash}
	nativeRequest(t, h, pgspec.ActionRoles, pgspec.AccessRequest{Operation: "create", Role: &role}, nil)
	reader := pgspec.RoleWrite{Role: pgspec.Role{Name: "access_reader", Login: true, ConnectionLimit: -1}, Password: &hash}
	nativeRequest(t, h, pgspec.ActionRoles, pgspec.AccessRequest{Operation: "create", Role: &reader}, nil)
	db := pgspec.Database{Name: "access_database", Owner: role.Name, Encoding: "UTF8", Locale: "C", AllowConnections: true}
	nativeRequest(t, h, pgspec.ActionDatabases, pgspec.AccessRequest{Operation: "create", Database: &db}, nil)
	var gotRole pgspec.Role
	nativeRequest(t, h, pgspec.ActionRoles, pgspec.AccessRequest{Operation: "get", Name: role.Name}, &gotRole)
	if !gotRole.Login || len(gotRole.MemberOf) != 1 || gotRole.MemberOf[0] != "pg_monitor" {
		t.Fatal("created role attributes/membership did not round trip")
	}
	var gotDB pgspec.Database
	nativeRequest(t, h, pgspec.ActionDatabases, pgspec.AccessRequest{Operation: "get", Name: db.Name}, &gotDB)
	if gotDB.Owner != role.Name || gotDB.Encoding != "UTF8" || gotDB.Locale != "C" || !gotDB.AllowConnections {
		t.Fatal("created database attributes did not round trip")
	}
	db.Owner = reader.Name
	db.AllowConnections = false
	nativeRequest(t, h, pgspec.ActionDatabases, pgspec.AccessRequest{Operation: "update", Name: db.Name, Database: &db}, nil)
	nativeRequest(t, h, pgspec.ActionDatabases, pgspec.AccessRequest{Operation: "get", Name: db.Name}, &gotDB)
	if gotDB.Owner != reader.Name || gotDB.AllowConnections {
		t.Fatal("database owner/allowConnections update did not round trip")
	}
	db.Owner = role.Name
	db.AllowConnections = true
	nativeRequest(t, h, pgspec.ActionDatabases, pgspec.AccessRequest{Operation: "update", Name: db.Name, Database: &db}, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	app, err := userConnect(ctx, "127.0.0.1", db.Name, role.Name, first)
	if err != nil {
		t.Fatal("native SCRAM role login failed")
	}
	if _, err = app.Exec(ctx, "CREATE SCHEMA access_schema"); err != nil {
		t.Fatal(err)
	}
	if _, err = app.Exec(ctx, "CREATE TABLE public.access_probe(value integer)"); err != nil {
		t.Fatal(err)
	}
	app.Close(ctx)
	grant := pgspec.Grant{Role: reader.Name, Database: db.Name, Privileges: []string{"CONNECT", "TEMP"}}
	nativeRequest(t, h, pgspec.ActionGrants, pgspec.AccessRequest{Operation: "grant", Grant: &grant}, nil)
	grant.Schema = "access_schema"
	grant.Privileges = []string{"USAGE"}
	nativeRequest(t, h, pgspec.ActionGrants, pgspec.AccessRequest{Operation: "grant", Grant: &grant}, nil)
	var grants []pgspec.Grant
	nativeRequest(t, h, pgspec.ActionGrants, pgspec.AccessRequest{Operation: "list", Name: db.Name}, &grants)
	found := false
	for _, v := range grants {
		found = found || v.Role == reader.Name && v.Schema == grant.Schema
	}
	if !found {
		t.Fatal("schema grant was not listed")
	}
	nativeRequest(t, h, pgspec.ActionGrants, pgspec.AccessRequest{Operation: "revoke", Grant: &grant}, nil)
	admin, err := localConnect(ctx, s, dataPaths(h.dir))
	if err != nil {
		t.Fatal(err)
	}
	if _, err = admin.Exec(ctx, "ALTER SYSTEM SET listen_addresses='127.0.0.2'"); err != nil {
		t.Fatal(err)
	}
	admin.Close(ctx)
	response, err := h.request(http.MethodPost, "/v1/processes/postgres/restart", nil)
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatal("could not isolate native backend listener")
	}
	// Separate member processes normally have separate network namespaces. Native
	// acceptance binds two loopback addresses to exercise both legs in one guest.
	proxy := &pgspec.Proxy{Servers: []string{"127.0.0.2"}, Monitor: s.Monitor, Admin: pgspec.Credentials{Username: "access_admin", Password: "native-proxy-admin"}, Users: []pgspec.ProxyUser{{Username: role.Name, Password: first}}, Threads: 1, MaxConnections: 16}
	proxyPaths := nativeProxy(t, proxy, "127.0.0.1", "")
	app, err = userConnect(ctx, "127.0.0.1", db.Name, role.Name, first)
	if err != nil {
		t.Fatal("ProxySQL SCRAM frontend/backend login failed")
	}
	if _, err = app.Exec(ctx, "INSERT INTO public.access_probe VALUES(1)"); err != nil {
		t.Fatal(err)
	}
	app.Close(ctx)
	hash = nativeVerifier(t, second)
	role.Password = &hash
	role.MemberOf = []string{"pg_read_all_data"}
	nativeRequest(t, h, pgspec.ActionRoles, pgspec.AccessRequest{Operation: "update", Name: role.Name, Role: &role}, nil)
	nativeRequest(t, h, pgspec.ActionRoles, pgspec.AccessRequest{Operation: "get", Name: role.Name}, &gotRole)
	if len(gotRole.MemberOf) != 1 || gotRole.MemberOf[0] != "pg_read_all_data" {
		t.Fatal("membership replacement did not revoke the previous parent")
	}
	proxy.Users[0].Password = second
	control, err := proxyConnect(ctx, proxy)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = reconcileProxy(ctx, control, proxy, "127.0.0.1", filepath.Join(proxyPaths.root, "ca.pem")); err != nil {
		t.Fatal(err)
	}
	control.Close(ctx)
	if old, err := userConnect(ctx, "127.0.0.1", db.Name, role.Name, first); err == nil {
		old.Close(ctx)
		t.Fatal("old ProxySQL password still authenticates")
	}
	if old, err := userConnect(ctx, "127.0.0.2", db.Name, role.Name, first); err == nil {
		old.Close(ctx)
		t.Fatal("old PostgreSQL password still authenticates")
	}
	app, err = userConnect(ctx, "127.0.0.1", db.Name, role.Name, second)
	if err != nil {
		t.Fatal("rotated ProxySQL password rejected")
	}
	if _, err = app.Exec(ctx, "INSERT INTO public.access_probe VALUES(2)"); err != nil {
		t.Fatal(err)
	}
	var count int
	if err = app.QueryRow(ctx, "SELECT count(*) FROM public.access_probe").Scan(&count); err != nil || count != 2 {
		t.Fatal("ProxySQL writes were not retained across rotation")
	}
	app.Close(ctx)
	nativeRequest(t, h, pgspec.ActionDatabases, pgspec.AccessRequest{Operation: "delete", Name: db.Name, Force: true}, nil)
	nativeRequest(t, h, pgspec.ActionRoles, pgspec.AccessRequest{Operation: "delete", Name: role.Name}, nil)
	nativeRequest(t, h, pgspec.ActionRoles, pgspec.AccessRequest{Operation: "delete", Name: reader.Name}, nil)
	proxy.Users = nil
	control, err = proxyConnect(ctx, proxy)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = reconcileProxy(ctx, control, proxy, "127.0.0.1", filepath.Join(proxyPaths.root, "ca.pem")); err != nil {
		t.Fatal(err)
	}
	control.Close(ctx)
	if deleted, err := userConnect(ctx, "127.0.0.1", "postgres", role.Name, second); err == nil {
		deleted.Close(ctx)
		t.Fatal("deleted role still authenticates through ProxySQL")
	}
	t.Log("native roles/databases/grants: CRUD, membership replacement, SCRAM on both ProxySQL legs, rotation rejects old credentials, deletion rejects fresh login")
}
func TestNativeMajorUpgrade(t *testing.T) {
	if os.Getenv("KARAKORAM_PG_BIN_DIR") == "" || os.Getenv("KARAKORAM_PG_UPGRADE") != "1" {
		t.Skip("set KARAKORAM_PG_BIN_DIR and KARAKORAM_PG_UPGRADE=1 with both major packages installed")
	}
	for _, major := range []int{16, 17} {
		if _, err := os.Stat(fmt.Sprintf("/usr/lib/postgresql/%d/bin/pg_upgrade", major)); err != nil {
			t.Fatal("native upgrade requires both Percona PostgreSQL major packages")
		}
	}
	s := testSpec()
	s.Major = 16
	s.BinDir = "/usr/lib/postgresql/16/bin"
	s.Version = "16.15.1"
	s.Postgres.Parameters = map[string]string{"shared_buffers": "32MB", "max_connections": "30"}
	h := startNativeAgent(t, s)
	defer h.stop(t)
	h.client.Timeout = 2 * time.Minute
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	c, err := localConnect(ctx, s, dataPaths(h.dir))
	if err != nil {
		t.Fatal(err)
	}
	if _, err = c.Exec(ctx, "CREATE TABLE public.upgrade_probe(value integer)"); err != nil {
		t.Fatal(err)
	}
	if _, err = c.Exec(ctx, "INSERT INTO public.upgrade_probe VALUES(42)"); err != nil {
		t.Fatal(err)
	}
	c.Close(ctx)
	status := h.status(t)
	if status.Locale == "" {
		t.Fatal("upgrade locale was not observed")
	}
	h.mu.Lock()
	h.config.DesiredProcesses = []string{}
	h.config.ConfigVersion++
	h.mu.Unlock()
	response, err := h.request(http.MethodPost, "/v1/reload", map[string]any{})
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatal("could not stop PostgreSQL for upgrade")
	}
	request := pgspec.UpgradeRequest{Receipt: "native-major-upgrade", OldMajor: 16, NewMajor: 17, Locale: status.Locale}
	var progress pgspec.UpgradeResponse
	nativeRequest(t, h, pgspec.ActionUpgradeCheck, request, &progress)
	if !progress.Checked || progress.Done {
		t.Fatal("offline upgrade check checkpoint incorrect")
	}
	nativeRequest(t, h, pgspec.ActionUpgradeRun, request, &progress)
	if !progress.Checked || !progress.Done {
		t.Fatal("offline pg_upgrade link did not complete")
	}
	s.Major = 17
	s.BinDir = "/usr/lib/postgresql/17/bin"
	s.Version = "17.11.1"
	raw, _ := json.Marshal(s)
	h.mu.Lock()
	h.config.EngineSpec = raw
	h.config.DesiredProcesses = nil
	h.config.ConfigVersion++
	h.mu.Unlock()
	response, err = h.request(http.MethodPost, "/v1/reload", map[string]any{})
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatal("upgraded PostgreSQL did not restart")
	}
	c, err = localConnect(ctx, s, dataPaths(h.dir))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close(ctx)
	var value int
	var version string
	if err = c.QueryRow(ctx, "SELECT value FROM public.upgrade_probe").Scan(&value); err != nil || value != 42 {
		t.Fatal("upgrade did not retain data")
	}
	if err = c.QueryRow(ctx, "SHOW server_version").Scan(&version); err != nil || !strings.HasPrefix(version, "17.") {
		t.Fatal("upgrade did not activate PostgreSQL17")
	}
	nativeRequest(t, h, pgspec.ActionUpgradeRun, request, &progress)
	if !progress.Done {
		t.Fatal("completed upgrade retry was not idempotent")
	}
	t.Log("native Percona16→17: maintenance exclusion, explicit locale, pg_upgrade check/link, durable activation, retained row and idempotent completion passed")
}
