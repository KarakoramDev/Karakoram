//go:build integration

// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package supabase

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"syscall"
	"testing"
	"time"
)

func TestNativeVector(t *testing.T) {
	install := os.Getenv("KARAKORAM_SUPABASE_VECTOR_DIR")
	if install == "" {
		t.Skip("native Vector artifact directory is required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
	defer cancel()
	root := t.TempDir()
	var mu sync.Mutex
	received := map[string][]map[string]any{}
	analytics := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("x-api-key") != "test-ingestion-token" {
			t.Error("missing analytics ingestion key")
		}
		var events []map[string]any
		if err := json.NewDecoder(r.Body).Decode(&events); err != nil {
			t.Errorf("invalid Logflare JSON batch: %v", err)
		}
		mu.Lock()
		name := r.URL.Query().Get("source_name")
		received[name] = append(received[name], events...)
		mu.Unlock()
		w.WriteHeader(200)
	}))
	defer analytics.Close()
	u, _ := url.Parse(analytics.URL)
	s := testSpec("vector")
	s.DatabaseIPv6 = true
	s.InstallDir = install
	s.LogflarePublicToken = "test-ingestion-token"
	s.VectorSinkToken = "test-vector-token"
	s.Upstreams["analytics"] = u.Host
	p, files, err := Render(s, root)
	if err != nil {
		t.Fatal(err)
	}
	for name, data := range files {
		path := filepath.Join(root, "karakoram", name)
		os.MkdirAll(filepath.Dir(path), 0700)
		if err := os.WriteFile(path, data, 0600); err != nil {
			t.Fatal(err)
		}
	}
	os.MkdirAll(filepath.Join(root, "karakoram/vector-data"), 0700)
	validate := exec.CommandContext(ctx, p.Binary, "validate", "--skip-healthchecks", filepath.Join(root, "karakoram/supabase/vector.json"))
	if output, err := validate.CombinedOutput(); err != nil {
		t.Fatalf("Vector configuration invalid: %v\n%s", err, output)
	}
	logs := os.Getenv("KARAKORAM_SUPABASE_LOG_DIR")
	if logs == "" {
		logs = root
	}
	os.MkdirAll(logs, 0700)
	f, err := os.OpenFile(filepath.Join(logs, "vector.log"), os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0600)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	cmd := exec.CommandContext(ctx, p.Binary, p.Args...)
	cmd.Dir = p.Dir
	cmd.Env = append([]string{"PATH=/usr/bin:/bin"}, p.Env...)
	cmd.Stdout = f
	cmd.Stderr = f
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() { syscall.Kill(-cmd.Process.Pid, syscall.SIGTERM); cmd.Wait() }()
	for deadline := time.Now().Add(10 * time.Second); ; {
		if healthy(ctx, s) == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("Vector health did not become ready")
		}
		time.Sleep(100 * time.Millisecond)
	}
	now := time.Now().UTC()
	batch := []map[string]any{
		{"member": "member", "process": "gateway", "timestamp": now, "message": "gateway diagnostic"},
		{"member": "member", "process": "auth", "timestamp": now, "message": `{"time":"2026-11-05T12:00:00Z","level":"info","msg":"auth message"}`},
		{"member": "member", "process": "rest", "timestamp": now, "message": "05/Nov/2026:12:00:00 +0000: rest message"},
		{"member": "member", "process": "realtime", "timestamp": now, "message": "12:00:00.000 [info] realtime message"},
		{"member": "member", "process": "realtime", "timestamp": now, "message": "12:00:00.000 [info] GET /health"},
		{"member": "member", "process": "storage", "timestamp": now, "message": `{"msg":"storage message","level":"info","time":"2026-11-05T12:00:00Z","hostname":"storage-host","pid":42}`},
		{"member": "member", "process": "functions", "timestamp": now, "message": "functions message ${UNSET_TEST_VALUE}"},
		{"member": "member", "process": "postgres", "timestamp": now, "message": "WARNING: native vector ingestion"},
		{"member": "member", "process": "patroni", "timestamp": now, "message": "patroni message"},
	}
	data, _ := json.Marshal(batch)
	request := func(token string) int {
		req, _ := http.NewRequestWithContext(ctx, "POST", "http://[::1]:9002", bytes.NewReader(data))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", "Bearer "+token)
		r, e := http.DefaultClient.Do(req)
		if e != nil {
			t.Fatal(e)
		}
		r.Body.Close()
		return r.StatusCode
	}
	if status := request("invalid"); status < 400 {
		t.Fatal("Vector accepted invalid bearer token")
	}
	if status := request(s.VectorSinkToken); status < 200 || status >= 300 {
		t.Fatalf("Vector ingestion status %d", status)
	}
	for deadline := time.Now().Add(10 * time.Second); ; {
		mu.Lock()
		complete := len(received) == 7 &&
			len(received["cloudflare.logs.prod"]) == 1 &&
			len(received["gotrue.logs.prod"]) == 1 &&
			len(received["postgREST.logs.prod"]) == 1 &&
			len(received["realtime.logs.prod"]) == 1 &&
			len(received["storage.logs.prod.2"]) == 1 &&
			len(received["deno-relay-logs"]) == 1 &&
			len(received["postgres.logs"]) == 2
		mu.Unlock()
		if complete {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("Vector did not route all service identities")
		}
		time.Sleep(100 * time.Millisecond)
	}
	mu.Lock()
	defer mu.Unlock()
	for source, events := range received {
		for _, event := range events {
			metadata, ok := event["metadata"].(map[string]any)
			if !ok || metadata["member"] != "member" || event["event_message"] == nil {
				t.Errorf("%s lost native member or message metadata", source)
			}
		}
	}
	authMetadata := received["gotrue.logs.prod"][0]["metadata"].(map[string]any)
	if authMetadata["timestamp"] != "2026-11-05T12:00:00Z" || authMetadata["level"] != "info" {
		t.Error("Auth structured metadata differs from upstream Vector policy")
	}
	if received["postgREST.logs.prod"][0]["event_message"] != "rest message" ||
		received["realtime.logs.prod"][0]["event_message"] != "realtime message" ||
		received["storage.logs.prod.2"][0]["event_message"] != "storage message" ||
		received["deno-relay-logs"][0]["event_message"] != "functions message ${UNSET_TEST_VALUE}" {
		t.Error("service parsing or literal message preservation changed")
	}
	t.Log("native Vector config validated; bearer authentication enforced; all seven Logflare identities, structured parsing and Realtime health filtering exercised")
}
