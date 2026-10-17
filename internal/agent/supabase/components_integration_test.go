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
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/agent"
	"github.com/KarakoramDev/Karakoram/internal/agent/supabase/initsql"
	"github.com/KarakoramDev/Karakoram/internal/engine/supabase/sbkeys"
	"github.com/KarakoramDev/Karakoram/internal/engine/supabase/sbspec"
	"github.com/coder/websocket"
	"github.com/jackc/pgx/v5"
)

// TestNativeAdditionalComponents runs only source-built components explicitly
// provided by an operator. It uses the same renderer, release preparation and
// primary forwarder as production; no application service is replaced by a fake.
func TestNativeAdditionalComponents(t *testing.T) {
	pg := os.Getenv("KARAKORAM_PG_BIN_DIR")
	gatewayDir := os.Getenv("KARAKORAM_SUPABASE_GATEWAY_DIR")
	storageDir := os.Getenv("KARAKORAM_SUPABASE_STORAGE_DIR")
	realtimeDir := os.Getenv("KARAKORAM_SUPABASE_REALTIME_DIR")
	metaDir := os.Getenv("KARAKORAM_SUPABASE_META_DIR")
	studioDir := os.Getenv("KARAKORAM_SUPABASE_STUDIO_DIR")
	if pg == "" || gatewayDir == "" || (storageDir == "" && realtimeDir == "" && metaDir == "" && studioDir == "") {
		t.Skip("native PostgreSQL, Envoy and at least one source-built Supabase artifact are required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	root := t.TempDir()
	logDir := os.Getenv("KARAKORAM_SUPABASE_LOG_DIR")
	if logDir == "" {
		logDir = root
	}
	if err := os.MkdirAll(logDir, 0700); err != nil {
		t.Fatal(err)
	}
	init := exec.CommandContext(ctx, filepath.Join(pg, "initdb"), "-D", filepath.Join(root, "pgdata"), "-U", "postgres", "--auth-local=trust", "--auth-host=trust", "--no-locale", "--encoding=UTF8")
	if output, err := init.CombinedOutput(); err != nil {
		t.Fatalf("initdb: %v %s", err, output)
	}
	start := func(t *testing.T, name string, p agent.ProcessSpec) func() {
		t.Helper()
		f, err := os.OpenFile(filepath.Join(logDir, "additional-"+name+".log"), os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0600)
		if err != nil {
			t.Fatal(err)
		}
		cmd := exec.CommandContext(ctx, p.Binary, p.Args...)
		cmd.Dir = p.Dir
		cmd.Env = append([]string{"PATH=/usr/local/bin:/usr/bin:/bin", "LANG=C.UTF-8"}, p.Env...)
		cmd.Stdout = f
		cmd.Stderr = f
		cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
		if err := cmd.Start(); err != nil {
			f.Close()
			t.Fatalf("start %s: %v", name, err)
		}
		var once sync.Once
		stop := func() {
			once.Do(func() {
				syscall.Kill(-cmd.Process.Pid, syscall.SIGTERM)
				done := make(chan struct{})
				go func() { cmd.Wait(); close(done) }()
				select {
				case <-done:
				case <-time.After(10 * time.Second):
					syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
					<-done
				}
				f.Close()
			})
		}
		t.Cleanup(stop)
		return stop
	}
	stopPG := start(t, "postgres", agent.ProcessSpec{Binary: filepath.Join(pg, "postgres"), Args: []string{"-D", filepath.Join(root, "pgdata"), "-h", "127.0.0.2", "-p", "5432", "-k", root, "-c", "wal_level=logical", "-c", "output_plugin_libraries=pgoutput,test_decoding,wal2json", "-c", "max_replication_slots=20", "-c", "max_wal_senders=20", "-c", "max_connections=80", "-c", "shared_buffers=32MB"}})
	defer stopPG()
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
	for _, role := range []string{"supabase_admin", "supabase_auth_admin", "supabase_storage_admin", "authenticator"} {
		passwords[role] = "integration-" + rand.Text()
	}
	components := []string{}
	if storageDir != "" {
		components = append(components, "storage")
	}
	if realtimeDir != "" {
		components = append(components, "realtime")
	}
	if err := initsql.Apply(ctx, conn, initsql.Plan(components, nil), passwords); err != nil {
		t.Fatal(err)
	}
	passwords["postgres"] = "integration-" + rand.Text()
	base := sbspec.Spec{RestartToken: "native-components", Topology: "single", DataPeers: []string{"127.0.0.2"}, Database: "postgres", DBRoles: passwords, SiteURL: "http://localhost:3000", APIExternalURL: "http://127.0.0.1:8000", GatewayURL: "http://127.0.0.1:8000", JWTSecret: keys.JWTSecret, AnonKey: keys.AnonKey, ServiceRoleKey: keys.ServiceRoleKey, SecretKeyBase: keys.SecretKeyBase, RealtimeDBEncKey: keys.RealtimeDBEncKey, VaultEncKey: keys.VaultEncKey, Upstreams: map[string]string{}, Studio: sbspec.Studio{Organization: "Native", Project: "Native integration", DashboardUsername: "dashboard", DashboardPassword: keys.DashboardPassword, MetaCryptoKey: keys.PGMetaCryptoKey}, Storage: sbspec.Storage{Endpoint: os.Getenv("KARAKORAM_SUPABASE_S3_ENDPOINT"), Bucket: os.Getenv("KARAKORAM_SUPABASE_S3_BUCKET"), Region: os.Getenv("KARAKORAM_SUPABASE_S3_REGION"), PathStyle: true, AccessKey: os.Getenv("KARAKORAM_SUPABASE_S3_ACCESS_KEY"), SecretKey: os.Getenv("KARAKORAM_SUPABASE_S3_SECRET_KEY"), TenantID: os.Getenv("KARAKORAM_SUPABASE_S3_TENANT"), ProtocolAccessKey: keys.StorageS3AccessKeyID, ProtocolSecretKey: keys.StorageS3SecretAccessKey}}
	base.Storage.TLS = strings.HasPrefix(base.Storage.Endpoint, "https://")
	base.RealtimeAdminSecret = rand.Text() + rand.Text()
	if base.Storage.Region == "" {
		base.Storage.Region = "us-east-1"
	}
	if base.Storage.TenantID == "" {
		base.Storage.TenantID = "native-integration"
	}
	f, err := newForwarder(base, "127.0.0.1:5432")
	if err != nil {
		t.Fatal(err)
	}
	f.start(ctx)
	defer f.stop()
	launch := func(t *testing.T, component, version, install string) func() {
		t.Helper()
		s := base
		s.Component = component
		s.Version = version
		s.InstallDir = install
		s = s.ProjectSecrets()
		if err := s.Validate(); err != nil {
			t.Fatal(err)
		}
		if err := validateService(s); err != nil {
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
		for _, directory := range []string{"functions", "snippets"} {
			if err := os.MkdirAll(filepath.Join(root, "karakoram", directory), 0700); err != nil {
				t.Fatal(err)
			}
		}
		if err := Prepare(ctx, s, p, root); err != nil {
			t.Fatal(err)
		}
		if component == "meta" || component == "studio" {
			bridge, err := startMetaBridge(s)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = bridge.Close() })
		}
		if component == "studio" {
			ingress, err := startStudioIngress(s)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = ingress.Close() })
		}
		stop := start(t, component, p)
		for deadline := time.Now().Add(90 * time.Second); ; {
			if healthy(ctx, s) == nil {
				break
			}
			if time.Now().After(deadline) {
				stop()
				t.Fatalf("%s did not become ready; see protected native process log", component)
			}
			time.Sleep(200 * time.Millisecond)
		}
		return stop
	}
	request := func(t *testing.T, method, path, key string, body []byte) (int, []byte) {
		t.Helper()
		req, _ := http.NewRequestWithContext(ctx, method, "http://127.0.0.1:8000"+path, bytes.NewReader(body))
		if key != "" {
			req.Header.Set("apikey", key)
		}
		req.Header.Set("Content-Type", "application/json")
		r, e := http.DefaultClient.Do(req)
		if e != nil {
			t.Fatal(e)
		}
		defer r.Body.Close()
		data, _ := io.ReadAll(io.LimitReader(r.Body, 1<<20))
		return r.StatusCode, data
	}
	t.Run("storage", func(t *testing.T) {
		if storageDir == "" {
			t.Skip("Storage artifact is not provided")
		}
		if base.Storage.Endpoint == "" || base.Storage.AccessKey == "" || base.Storage.SecretKey == "" || base.Storage.Bucket == "" {
			t.Skip("registered S3 backend environment is required")
		}
		base.Upstreams = map[string]string{"storage": "127.0.0.1:5000"}
		defer launch(t, "storage", "1.81.0", storageDir)()
		defer launch(t, "gateway", "1.39.1", gatewayDir)()
		bucket := "native-" + strings.ToLower(rand.Text())
		body, _ := json.Marshal(map[string]any{"id": bucket, "name": bucket, "public": false})
		status, _ := request(t, "POST", "/storage/v1/bucket", keys.ServiceRoleKey, body)
		if status != 200 && status != 201 {
			t.Fatalf("Storage bucket create status %d", status)
		}
		defer func() {
			request(t, "POST", "/storage/v1/bucket/"+bucket+"/empty", keys.ServiceRoleKey, []byte(`{}`))
			request(t, "DELETE", "/storage/v1/bucket/"+bucket, keys.ServiceRoleKey, nil)
		}()
		req, _ := http.NewRequestWithContext(ctx, "POST", "http://127.0.0.1:8000/storage/v1/object/"+bucket+"/native.txt", strings.NewReader("native-s3-ok"))
		req.Header.Set("Content-Type", "text/plain")
		req.Header.Set("Authorization", "Bearer "+keys.ServiceRoleKey)
		r, e := http.DefaultClient.Do(req)
		if e != nil {
			t.Fatal(e)
		}
		r.Body.Close()
		if r.StatusCode != 200 {
			t.Fatalf("Storage upload status %d", r.StatusCode)
		}
		status, data := request(t, "GET", "/storage/v1/object/authenticated/"+bucket+"/native.txt", keys.ServiceRoleKey, nil)
		if status != 200 || string(data) != "native-s3-ok" {
			t.Fatalf("Storage S3 download status %d", status)
		}
		t.Log("native Storage S3 upload/download through gateway passed")
	})
	t.Run("realtime", func(t *testing.T) {
		if realtimeDir == "" {
			t.Skip("Realtime artifact is not provided")
		}
		base.Upstreams = map[string]string{"realtime": "127.0.0.1:4000"}
		if _, err := conn.Exec(ctx, "CREATE TABLE public.native_realtime(id integer PRIMARY KEY, body text); ALTER PUBLICATION supabase_realtime ADD TABLE public.native_realtime; GRANT SELECT ON public.native_realtime TO anon, authenticated, service_role;"); err != nil {
			t.Fatal(err)
		}
		defer launch(t, "realtime", "2.143.3", realtimeDir)()
		defer launch(t, "gateway", "1.39.1", gatewayDir)()
		for _, method := range []string{"GET", "DELETE"} {
			for _, key := range []string{keys.AnonKey, keys.ServiceRoleKey, keys.AnonJWT, keys.ServiceRoleJWT} {
				req, _ := http.NewRequestWithContext(ctx, method, "http://127.0.0.1:4000/api//tenants/realtime-dev", nil)
				req.Header.Set("Authorization", "Bearer "+key)
				resp, err := http.DefaultClient.Do(req)
				if err != nil {
					t.Fatal(err)
				}
				resp.Body.Close()
				if resp.StatusCode != http.StatusForbidden {
					t.Fatalf("private Realtime %s accepted a tenant credential: %d", method, resp.StatusCode)
				}
			}
		}
		wsctx, cancel := context.WithTimeout(ctx, 30*time.Second)
		defer cancel()
		address := "ws://127.0.0.1:8000/realtime/v1/websocket?vsn=1.0.0&apikey=" + url.QueryEscape(keys.ServiceRoleKey)
		ws, _, err := websocket.Dial(wsctx, address, nil)
		if err != nil {
			t.Fatal("Realtime websocket upgrade failed")
		}
		defer ws.CloseNow()
		join := map[string]any{"topic": "realtime:public:native_realtime", "event": "phx_join", "ref": "1", "payload": map[string]any{"access_token": keys.ServiceRoleKey, "config": map[string]any{"broadcast": map[string]any{"ack": true, "self": false}, "presence": map[string]string{"key": ""}, "postgres_changes": []map[string]string{{"event": "INSERT", "schema": "public", "table": "native_realtime"}}}}}
		data, _ := json.Marshal(join)
		if err := ws.Write(wsctx, websocket.MessageText, data); err != nil {
			t.Fatal(err)
		}
		joined := false
		subscribed := false
		for !joined || !subscribed {
			_, data, err := ws.Read(wsctx)
			if err != nil {
				t.Fatal("Realtime join acknowledgement unavailable")
			}
			var event map[string]any
			if json.Unmarshal(data, &event) == nil && event["event"] == "phx_reply" {
				payload, _ := event["payload"].(map[string]any)
				joined = payload["status"] == "ok"
				if !joined {
					t.Fatal("Realtime join rejected")
				}
			}
			if event["event"] == "system" {
				payload, _ := event["payload"].(map[string]any)
				if payload["extension"] == "postgres_changes" {
					if payload["status"] != "ok" {
						t.Fatal("Realtime replication subscription rejected")
					}
					subscribed = true
				}
			}
		}
		if _, err := conn.Exec(ctx, "INSERT INTO public.native_realtime VALUES(1,'native-insert')"); err != nil {
			t.Fatal(err)
		}
		for {
			_, data, err := ws.Read(wsctx)
			if err != nil {
				t.Fatal("Realtime INSERT event unavailable")
			}
			var event map[string]any
			if json.Unmarshal(data, &event) == nil && event["event"] == "postgres_changes" && bytes.Contains(data, []byte("native-insert")) {
				break
			}
		}
		t.Log("native Realtime websocket received a PostgreSQL INSERT through gateway")
	})
	t.Run("meta-studio", func(t *testing.T) {
		if metaDir == "" {
			t.Skip("postgres-meta artifact is not provided")
		}
		base.Upstreams = map[string]string{"meta": "127.0.0.1:8080"}
		defer launch(t, "meta", "0.100.0", metaDir)()
		resp, err := http.Get("http://127.0.0.1:8080/tables")
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusUnauthorized {
			t.Fatalf("unauthenticated private metadata status %d", resp.StatusCode)
		}
		if studioDir != "" {
			base.Upstreams["studio"] = "127.0.0.1:3002"
			defer launch(t, "studio", "2026.10.12-bc27d2661edd", studioDir)()
			req, _ := http.NewRequestWithContext(ctx, "POST", "http://127.0.0.1:3002/api/platform/pg-meta/default/query", strings.NewReader(`{"query":"SELECT current_user"}`))
			req.Header.Set("Content-Type", "application/json")
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Fatal(err)
			}
			resp.Body.Close()
			if resp.StatusCode != http.StatusUnauthorized {
				t.Fatalf("unauthenticated private Studio SQL status %d", resp.StatusCode)
			}
		}
		defer launch(t, "gateway", "1.39.1", gatewayDir)()
		status, _ := request(t, "GET", "/pg/tables", keys.ServiceRoleKey, nil)
		if status != 200 {
			t.Fatalf("native postgres-meta tables status %d", status)
		}
		status, _ = request(t, "GET", "/pg/tables", keys.AnonKey, nil)
		if status != 403 {
			t.Fatalf("anonymous postgres-meta status %d", status)
		}
		t.Log("native postgres-meta database query and service-only gateway protection passed")
		if studioDir == "" {
			t.Log("Studio not run: standalone artifact is not provided")
			return
		}
		status, _ = request(t, "GET", "/", "", nil)
		if status != 401 {
			t.Fatalf("unauthenticated Studio status %d", status)
		}
		req, _ := http.NewRequestWithContext(ctx, "GET", "http://127.0.0.1:8000/", nil)
		req.SetBasicAuth(base.Studio.DashboardUsername, base.Studio.DashboardPassword)
		r, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		r.Body.Close()
		if r.StatusCode != 200 {
			t.Fatalf("authenticated Studio status %d", r.StatusCode)
		}
		t.Logf("native Studio behind gateway basic-auth: HTTP %d", r.StatusCode)
	})
}
