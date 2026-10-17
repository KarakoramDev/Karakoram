//go:build integration

// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package supabase

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
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
	received := map[string]bool{}
	analytics := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("x-api-key") != "test-ingestion-token" {
			t.Error("missing analytics ingestion key")
		}
		io.Copy(io.Discard, r.Body)
		mu.Lock()
		received[r.URL.Query().Get("source_name")] = true
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
	batch := []map[string]any{{"member": "member", "process": "auth", "timestamp": time.Now().UTC(), "message": "auth message"}, {"member": "member", "process": "rest", "timestamp": time.Now().UTC(), "message": "rest message"}, {"member": "member", "process": "postgres", "timestamp": time.Now().UTC(), "message": "LOG: native vector ingestion"}}
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
		complete := received["gotrue.logs.prod"] && received["postgREST.logs.prod"] && received["postgres.logs"]
		mu.Unlock()
		if complete {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("Vector did not route all service identities")
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Log("native Vector config validated; bearer authentication rejects invalid credentials; HTTP batches routed to Auth/PostgREST/Postgres Logflare source identities")
}
